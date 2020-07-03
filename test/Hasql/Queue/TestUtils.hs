module Hasql.Queue.TestUtils where
import qualified Data.ByteString.Char8 as BSC
import Control.Concurrent.Async
import           Database.Postgres.Temp as Temp
import           Test.Hspec
import           Control.Exception as E
import           Data.Pool
import           Hasql.Connection
import           Hasql.Session
import qualified Data.ByteString.Base64.URL as Base64
import           Control.Concurrent
import           Data.IORef
import           Data.Foldable
import           Control.Monad ((<=<))
import           Crypto.Hash.SHA1 (hash)
import           Hasql.Queue.Migrate

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
      (either (throwIO . userError . show) pure =<< acquire (toConnectionString db))
      release
      2
      60
      50

withConnection :: (Connection -> IO ()) -> Pool Connection -> IO ()
withConnection = flip withResource

runImplicitTransaction :: Pool Connection -> Session a -> IO a
runImplicitTransaction pool action = do
  let wrappedAction = do
        r <- action
        pure r
  withResource pool $ \conn ->
    either (throwIO . userError . show) pure =<< run wrappedAction conn

runReadCommitted :: Pool Connection -> Session a -> IO a
runReadCommitted = flip withReadCommitted

withReadCommitted :: Session a -> Pool Connection -> IO a
withReadCommitted action pool = do
  let wrappedAction = do
        sql "BEGIN"
        r <- action
        sql "ROLLBACK"
        pure r
  withResource pool $ \conn ->
    either (throwIO . userError . show) pure =<< run wrappedAction conn
