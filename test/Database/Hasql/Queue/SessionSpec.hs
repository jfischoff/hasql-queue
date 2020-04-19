{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Database.Hasql.Queue.SessionSpec where
import           Hasql.Queue.Internal
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Exception as E
import           Control.Monad
import           Data.IORef
import           Hasql.Queue.Session
import           Hasql.Queue.Migrate
import           Test.Hspec                     (SpecWith, Spec, describe, parallel, it, afterAll, beforeAll, runIO)
import           Test.Hspec.Expectations.Lifted
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Database.Postgres.Temp as Temp
import           Data.Pool
import           Data.Foldable
import           Test.Hspec.Core.Spec (sequential)
import           Crypto.Hash.SHA1 (hash)
import qualified Data.ByteString.Base64.URL as Base64
import qualified Data.ByteString.Char8 as BSC
import           Hasql.Connection
import           Hasql.Session
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Typeable
import           Data.Int

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
          $ BSC.pack $ migrationQueryString <> intPayloadMigration)
        (flip withConn $ flip migrate intPayloadMigration)
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
runImplicitTransaction pool action = withResource pool $ \conn ->
    either (throwIO . userError . show) pure =<< run action conn

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

newtype TooManyRetries = TooManyRetries Int64
  deriving (Show, Eq, Typeable)

instance Exception TooManyRetries

spec :: Spec
spec = describe "Hasql.Queue.Session" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "basic" $ do
    it "is okay to migrate multiple times" $ withConnection $ \conn ->
      liftIO $ migrate conn intPayloadMigration

    it "empty locks nothing" $ \pool -> do
      runReadCommitted pool (withDequeue D.int4 8 return) >>= \x ->
        x `shouldBe` Nothing
    it "empty gives count 0" $ \pool ->
      runReadCommitted pool getCount `shouldReturn` 0

    it "enqueue/withDequeue" $ \pool -> do
      (withDequeueResult, firstCount, secondCount) <- runReadCommitted pool $ do
        enqueueNotify E.int4 [1]
        firstCount <- getCount
        withDequeueResult <- withDequeue D.int4 8 (`shouldBe` 1)

        secondCount <- getCount
        pure (withDequeueResult, firstCount, secondCount)

      firstCount `shouldBe` 1
      secondCount `shouldBe` 0
      withDequeueResult `shouldBe` Just ()

    it "enqueue/withDequeue/retries" $ \pool -> do
      e <- E.try $ runImplicitTransaction pool $ do
        void $ enqueue E.int4 [1]
        theCount <- getCount

        void $ withDequeue D.int4 8 $ const $
            throwM $ TooManyRetries theCount

      (e :: Either TooManyRetries ()) `shouldBe` Left (TooManyRetries 1)

      runImplicitTransaction pool (dequeuePayload D.int4 1) >>= \[(Payload {..})] -> do
        pAttempts `shouldBe` 1
        pValue `shouldBe` 1

      e1 <- E.try $ runImplicitTransaction pool $ do
        void $ enqueue E.int4 [1]
        theCount <- getCount

        void $ withDequeue D.int4 8 $ const $
            throwM $ TooManyRetries theCount

      (e1 :: Either TooManyRetries ()) `shouldBe` Left (TooManyRetries 1)

      replicateM_ 6 $ E.handle (\(_ :: TooManyRetries) -> pure ()) $ runImplicitTransaction pool $ do
          void $ withDequeue D.int4 8 $ const $
            throwM $ TooManyRetries 1

      runImplicitTransaction pool (dequeuePayload D.int4 1) >>= \[(Payload {..})] -> do
        pAttempts `shouldBe` 7
        pValue `shouldBe` 1

    it "enqueue/withDequeue/timesout" $ \pool -> do
      e <- E.try $ runReadCommitted pool $ do
        void $ enqueue E.int4 [1]
        firstCount <- getCount

        void $ withDequeue D.int4 1 $ const $
            throwM $ TooManyRetries firstCount

      (e :: Either TooManyRetries ())`shouldBe` Left (TooManyRetries 1)

      runReadCommitted pool getCount `shouldReturn` 0

    it "selects the oldest first" $ \pool -> do
      (firstCount, firstwithDequeueResult, secondwithDequeueResult, secondCount) <- runReadCommitted pool $ do
        enqueue E.int4 [1]
        liftIO $ threadDelay 100

        enqueue E.int4 [2]

        firstCount <- getCount

        firstwithDequeueResult   <- withDequeue D.int4 8 (`shouldBe` 1)
        secondwithDequeueResult <- withDequeue D.int4 8 (`shouldBe` 2)

        secondCount <- getCount
        pure (firstCount, firstwithDequeueResult, secondwithDequeueResult, secondCount)

      firstCount `shouldBe` 2
      firstwithDequeueResult `shouldBe` Just ()

      secondCount `shouldBe` 0
      secondwithDequeueResult `shouldBe` Just ()
