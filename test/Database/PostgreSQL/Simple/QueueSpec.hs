{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Database.PostgreSQL.Simple.QueueSpec where
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Control.Exception as E
import           Control.Monad
import           Data.Aeson
import           Data.Function
import           Data.IORef
import           Data.List
import           Database.PostgreSQL.Simple.Queue
import           Database.PostgreSQL.Simple.Queue.Migrate
import           Test.Hspec                     (SpecWith, Spec, describe, parallel, it, afterAll, beforeAll, runIO)
import           Test.Hspec.Expectations.Lifted
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Data.List.Split
import           Data.Either
import           Database.Postgres.Temp as Temp
import           Data.Pool
import           Data.Foldable
import           Test.Hspec.Core.Spec (sequential)
import           Crypto.Hash.SHA1 (hash)
import qualified Data.ByteString.Base64.URL as Base64
import qualified Data.ByteString.Char8 as BSC
import           Hasql.Connection
import           Hasql.Session
import           Data.Typeable

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
          $ BSC.pack migrationQueryString)
        (flip withConn migrate)
        (verboseConfig <> cacheConfig dbCache)
  withConfig migratedConfig $ \db -> do
    f =<< createPool
      (either (throwIO . userError . show) pure =<< acquire (toConnectionString db))
      release
      2
      60
      50

data Abort = Abort
  deriving (Show, Eq, Typeable)

instance Exception Abort

withConnection :: (Connection -> IO ()) -> Pool Connection -> IO ()
withConnection = flip withResource

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

spec :: Spec
spec = describe "Database.Queue" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "basic" $ do
    it "is okay to migrate multiple times" $ withConnection $
      liftIO . migrate

    it "empty locks nothing" $ \pool -> do
      runReadCommitted pool (withPayloadDB 8 return) >>= \case
        Left err -> fail $ show err
        Right x -> x `shouldBe` Nothing
    it "empty gives count 0" $ \pool ->
      runReadCommitted pool getCountDB `shouldReturn` 0

    it "enqueuesDB/withPayloadDB" $ \pool -> do
      (payloadId, theCount) <- runReadCommitted pool $ do
        payloadId <- enqueueDB $ String "Hello"
        (payloadId,) <$> getCountDB
      theCount `shouldBe` 1

      theCount' <- runReadCommitted pool $ do
        -- TODO assert that this returns a right
        withPayloadDB 8 (\(Payload {..}) -> do
          pId `shouldBe` payloadId
          pValue `shouldBe` String "Hello"
          )

        getCountDB

      theCount' `shouldBe` 0

{-
    it "enqueuesDB/withPayloadDB/retries" $ withReadCommitted $ do
      void $ enqueueDB $ String "Hello"
      getCountDB `shouldReturn` 1

      xs <- replicateM 7 $ withPayloadDB 8 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      either throwM (const $ pure ()) =<< withPayloadDB 8 (\(Payload {..}) -> do
        pAttempts `shouldBe` 7
        pValue `shouldBe` String "Hello"
        )
    it "enqueuesDB/withPayloadDB/timesout" $ withReadCommitted $  do
      void $ enqueueDB $ String "Hello"
      getCountDB `shouldReturn` 1

      xs <- replicateM 2 $ withPayloadDB 1 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      getCountDB `shouldReturn` 0

    it "selects the oldest first" $ withReadCommitted $ do
      payloadId0 <- enqueueDB $ String "Hello"
      liftIO $ threadDelay 100

      payloadId1 <- enqueueDB $ String "Hi"

      getCountDB `shouldReturn` 2

      either throwM return =<< withPayloadDB 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId0
        pValue `shouldBe` String "Hello"
        )

      either throwM return =<< withPayloadDB 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId1
        pValue `shouldBe` String "Hi"
        )

      getCountDB `shouldReturn` 0

    it "enqueues and dequeues concurrently withPayload" $ \testDB -> do
      let withPool' = flip withConnection testDB
          elementCount = 1000 :: Int
          expected = [0 .. elementCount - 1]

      ref <- newTVarIO []

      loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
        lastCount <- either throwM return <=< withPayload c 0 $ \(Payload {..}) -> do
          atomically $ do
            xs <- readTVar ref
            writeTVar ref $ pValue : xs
            return $ length xs + 1

        when (lastCount < elementCount) next

      forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
         forM_ xs $ \i -> enqueue c $ toJSON i

      waitAnyCancel loopThreads
      xs <- atomically $ readTVar ref
      let Just decoded = mapM (decode . encode) xs
      sort decoded `shouldBe` sort expected

  aroundAll withSetup $ describe "basic" $ do
    it "enqueues and dequeues concurrently dequeue" $ \testDB -> do
      let withPool' = flip withConnection testDB
          elementCount = 1000 :: Int
          expected = [0 .. elementCount - 1]

      ref <- newTVarIO []

      loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
        Payload {..} <- dequeue c
        lastCount <- atomically $ do
          xs <- readTVar ref
          writeTVar ref $ pValue : xs
          return $ length xs + 1

        when (lastCount < elementCount) next

      forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
         forM_ xs $ \i -> enqueue c $ toJSON i

      waitAnyCancel loopThreads
      xs <- atomically $ readTVar ref
      let Just decoded = mapM (decode . encode) xs
      sort decoded `shouldBe` sort expected
-}
