module Main where
import           System.Environment
import           Hasql.Queue.Migrate
import           Data.IORef
import           Control.Exception
import           Crypto.Hash.SHA1 (hash)
import qualified Data.ByteString.Base64.URL as Base64
import qualified Data.ByteString.Char8 as BSC
import           Data.Pool
import           Database.Postgres.Temp
import           Control.Concurrent
import           Control.Monad (replicateM, forever, void)
import           Hasql.Session
import           Hasql.Connection
import           Data.Function
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Statement
import qualified Hasql.Queue.Internal as I
import qualified Hasql.Queue.IO as IO
import qualified Hasql.Queue.Session as S
import           Data.Int

-- TODO need to make sure the number of producers and consumers does not go over the number of connections

withConn :: DB -> (Connection -> IO a) -> IO a
withConn db f = do
  let connStr = toConnectionString db
  bracket (either (throwIO . userError . show) pure =<< acquire connStr) release f

withSetup :: (Pool Connection -> IO ()) -> IO ()
withSetup f = do
  -- Helper to throw exceptions
  let throwE x = either throwIO pure =<< x

  throwE $ withDbCache $ \dbCache -> do
    --let combinedConfig = autoExplainConfig 15 <> cacheConfig dbCache
    let combinedConfig = defaultConfig <> cacheConfig dbCache
    migratedConfig <- throwE $ cacheAction (("~/.tmp-postgres/" <>) . BSC.unpack . Base64.encode . hash
        $ BSC.pack $ migrationQueryString "int4")
        (flip withConn $ flip migrate "int4")
        combinedConfig
    withConfig migratedConfig $ \db -> do
      print $ toConnectionString db

      f =<< createPool
              (either (throwIO . userError . show) pure =<< acquire (toConnectionString db)
              ) release 2 60 49

payload :: Int32
payload = 1

main :: IO ()
main = do
  [producerCount, consumerCount, time, initialDequeueCount, initialEnqueueCount, batchCount, notify]
    <- map read <$> getArgs
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
    let enqueueAction = if notify > 0
          then void $ withResource pool $ \conn -> IO.enqueue "channel" conn E.int4 [payload]
          else void $ withResource pool $ \conn -> I.runThrow (S.enqueue E.int4 [payload]) conn
        dequeueAction = if notify > 0
          then void $ withResource pool $ \conn ->
            IO.withDequeue "channel" conn D.int4 1 batchCount (const $ pure ())
          else void $ withResource pool $ \conn -> fix $ \next ->
            I.runThrow (S.dequeue D.int4 batchCount) conn >>= \case
              [] -> next
              _ -> pure ()


    let enqueueInsertSql = "INSERT INTO payloads (attempts, value) SELECT 0, g.value FROM generate_series(1, $1) AS g (value)"
        enqueueInsertStatement =
          statement (fromIntegral initialEnqueueCount) $ Statement enqueueInsertSql (E.param $ E.nonNullable E.int4) D.noResult False

    _ <- withResource pool $ run enqueueInsertStatement

    let dequeueInsertSql = "INSERT INTO payloads (attempts, state, value) SELECT 0, 'dequeued', g.value FROM generate_series(1, $1) AS g (value)"
        dequeueInsertStatement =
          statement (fromIntegral initialDequeueCount) $ Statement dequeueInsertSql (E.param $ E.nonNullable E.int4) D.noResult False

    _ <- withResource pool $ run dequeueInsertStatement

    withResource pool $ \conn -> void $ run (sql "VACUUM FULL ANALYZE") conn
    putStrLn "Finished VACUUM FULL ANALYZE"

    let enqueueLoop = forever $ do
          enqueueAction
          atomicModifyIORef' enqueueCounter $ \x -> (x+1, ())

        dequeueLoop = forever $ do
           dequeueAction
           atomicModifyIORef' dequeueCounter $ \x -> (x+1, ())

    -- Need better exception behavior ... idk ... I'll deal with this later
    _enqueueThreads <- replicateM producerCount $ forkIO enqueueLoop
    _dequeueThreads <- replicateM consumerCount $ forkIO dequeueLoop

    threadDelay $ time * 1000000
    throwIO $ userError "Finished"
