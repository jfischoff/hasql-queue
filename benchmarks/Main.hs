module Main where
import System.Environment
import Database.PostgreSQL.Simple.Queue
import Database.PostgreSQL.Simple.Queue.Migrate
import Control.Concurrent.Async
import Data.Aeson
import Data.IORef
import Control.Exception
import           Crypto.Hash.SHA1 (hash)
import qualified Data.ByteString.Base64.URL as Base64
import qualified Data.ByteString.Char8 as BSC
import           Data.Pool
import           Database.Postgres.Temp
import           Database.PostgreSQL.Simple
import           Control.Concurrent
import           Control.Monad (replicateM, forever, void)


-- TODO need to make sure the number of producers and consumers does not go over the number of connections


withConn :: DB -> (Connection -> IO a) -> IO a
withConn db f = do
  let connStr = toConnectionString db
  bracket (connectPostgreSQL connStr) close f

withSetup :: (Pool Connection -> IO ()) -> IO ()
withSetup f = do
  -- Helper to throw exceptions
  let throwE x = either throwIO pure =<< x

  throwE $ withDbCache $ \dbCache -> do
    --let combinedConfig = autoExplainConfig 15 <> cacheConfig dbCache
    let combinedConfig = defaultConfig <> cacheConfig dbCache
    migratedConfig <- throwE $ cacheAction (("~/.tmp-postgres/" <>) . BSC.unpack . Base64.encode . hash
        $ BSC.pack migrationQueryString) (flip withConn migrate) combinedConfig
    withConfig migratedConfig $ \db -> do
      print $ toConnectionString db

      f =<< createPool
              (do
                c <- connectPostgreSQL $ toConnectionString db
                setup c
                pure c
              ) close 2 60 49

payload :: Value
payload = toJSON 'a'

main :: IO ()
main = do
  [producerCount, consumerCount, time, initialDequeueCount, initialEnqueueCount] <- map read <$> getArgs
  -- create a temporary database
  enqueueCounter <- newIORef (0 :: Int)
  dequeueCounter <- newIORef (0 :: Int)

  let printCounters = do
        finalEnqueueCount <- readIORef enqueueCounter
        finalDequeueCount <- readIORef dequeueCounter
        putStrLn $ "Time " <> show time <> " secs"
        putStrLn $ "Enqueue Count: " <> show finalEnqueueCount
        putStrLn $ "Dequeue Count: " <> show finalDequeueCount

  flip finally printCounters $ withSetup $ \pool -> do
    -- enqueue the enqueueCount + dequeueCount
    let totalEnqueueCount = initialDequeueCount + initialEnqueueCount
        enqueueAction = void $ withResource pool $ \conn -> enqueue conn payload
        dequeueAction = void $ withResource pool $ dequeue

    replicateConcurrently_ totalEnqueueCount enqueueAction
    replicateConcurrently_ initialDequeueCount dequeueAction

    withResource pool $ \conn -> void $ execute_ conn "VACUUM FULL ANALYZE"

    -- forever $ threadDelay 1000000000

    let enqueueLoop = forever $ do
          enqueueAction
          atomicModifyIORef' enqueueCounter $ \x -> (x+1, ())

        dequeueLoop = forever $ do
           dequeueAction
           atomicModifyIORef' dequeueCounter $ \x -> (x+1, ())

    -- Need better exception behavior ... idk ... I'll deal with this later
    _enqueueThreads <- replicateM producerCount $ async enqueueLoop
    _dequeueThreads <- replicateM consumerCount $ async dequeueLoop

    threadDelay $ time * 1000000
    throwIO $ userError "Finished"
