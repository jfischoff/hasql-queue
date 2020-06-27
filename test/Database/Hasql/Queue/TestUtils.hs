module Database.Hasql.Queue.TestUtils where
import           Test.Hspec
import           Database.Postgres.Temp as Temp
import           Hasql.Connection
import           Control.Exception as E
import           Data.Pool
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Base64.URL as Base64
import           Control.Concurrent
import           Data.IORef
import           Control.Concurrent.Async
import           Data.Foldable (traverse_)
import           Control.Monad((<=<))
import           Crypto.Hash.SHA1 (hash)
import           Hasql.Queue.Migrate
import qualified Database.PostgreSQL.Simple.Options as O

aroundAll :: forall a. ((a -> IO ()) -> IO ()) -> SpecWith a -> Spec
aroundAll withFunc specWith = do
  (var, stopper, asyncer) <- runIO $
    (,,) <$> newEmptyMVar <*> newEmptyMVar <*> newIORef Nothing
  let theStart :: IO a
      theStart = do

        thread <- async $ do
          withFunc $ \x -> do
            putMVar var x
            takeMVar stopper
          pure $ error "Don't evaluate this"

        writeIORef asyncer $ Just thread

        either pure pure =<< (wait thread `race` takeMVar var)

      theStop :: a -> IO ()
      theStop _ = do
        putMVar stopper ()
        traverse_ cancel =<< readIORef asyncer

  beforeAll theStart $ afterAll theStop $ specWith

withConn :: Temp.DB -> (Connection -> IO a) -> IO a
withConn db f = do
  let connStr = toConnectionString db
  E.bracket (either (throwIO . userError . show) pure =<< acquire connStr) release f

withSetup :: (Pool Connection -> IO ()) -> IO ()
withSetup f = either throwIO pure <=< withDbCache $ \dbCache -> do
  migratedConfig <- either throwIO pure =<<
      cacheAction
        (("~/.tmp-postgres/" <>) . BSC.unpack . Base64.encode . hash
          $ BSC.pack $ migrationQueryString "int4")
        (flip withConn $ flip migrate "int4")
        (verboseConfig <> cacheConfig dbCache)
  withConfig migratedConfig $ \db -> do
    f =<< createPool
      (either (throwIO . userError . show) pure =<< acquire
        (O.toConnectionString ((toConnectionOptions db) {O.host = pure "127.0.0.1"})))
      release
      2
      60
      50

withConnection :: (Connection -> IO ()) -> Pool Connection -> IO ()
withConnection = flip withResource
