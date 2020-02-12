{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
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
import           Test.Hspec                     (SpecWith, Spec, hspec, describe, parallel, it, afterAll, beforeAll, runIO)
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

main :: IO ()
main = hspec spec

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
      (withPayloadDBResult, firstCount, secondCount) <- runReadCommitted pool $ do
        payloadId <- enqueueDB $ String "Hello"
        firstCount <- getCountDB
        withPayloadDBResult <- withPayloadDB 8 (\(Payload {..}) -> do
            pId `shouldBe` payloadId
            pValue `shouldBe` String "Hello"
          )

        secondCount <- getCountDB
        pure (withPayloadDBResult, firstCount, secondCount)

      case withPayloadDBResult of
        Left err -> fail $ "withPayloadDB failed with: " <> show err
        Right _ -> pure ()

      firstCount `shouldBe` 1
      secondCount `shouldBe` 0


    it "enqueuesDB/withPayloadDB/retries" $ \pool -> do
      (theCount, xs) <- runReadCommitted pool $ do
        void $ enqueueDB $ String "Hello"
        theCount <- getCountDB

        fmap (theCount,) $ replicateM 7 $ withPayloadDB 8 (\(Payload {..}) ->
            throwM $ userError "not enough tries"
          )

      theCount `shouldBe` 1
      all isLeft xs `shouldBe` True

      either throwM (const $ pure ()) <=< runReadCommitted pool $ withPayloadDB 8 (\(Payload {..}) -> do
        pAttempts `shouldBe` 7
        pValue `shouldBe` String "Hello"
        )

    it "enqueuesDB/withPayloadDB/timesout" $ \pool -> do
      (firstCount, xs, secondCount) <- runReadCommitted pool $ do
        void $ enqueueDB $ String "Hello"
        firstCount <- getCountDB

        xs <- replicateM 2 $ withPayloadDB 1 (\(Payload {..}) ->
            throwM $ userError "not enough tries"
          )

        secondCount <- getCountDB

        pure (firstCount, xs, secondCount)

      firstCount `shouldBe` 1
      all isLeft xs `shouldBe` True

      secondCount `shouldBe` 0

    it "selects the oldest first" $ \pool -> do
      (firstCount, firstWithPayloadResult, secondWithPayloadResult, secondCount) <- runReadCommitted pool $ do
        payloadId0 <- enqueueDB $ String "Hello"
        liftIO $ threadDelay 100

        payloadId1 <- enqueueDB $ String "Hi"

        firstCount <- getCountDB

        firstWithPayloadResult <- withPayloadDB 8 (\(Payload {..}) -> do
            pId `shouldBe` payloadId0
            pValue `shouldBe` String "Hello"
          )

        secondWithPayloadResult <- withPayloadDB 8 (\(Payload {..}) -> do
            pId `shouldBe` payloadId1
            pValue `shouldBe` String "Hi"
          )

        secondCount <- getCountDB
        pure (firstCount, firstWithPayloadResult, secondWithPayloadResult, secondCount)

      firstCount `shouldBe` 2

      case firstWithPayloadResult of
        Left err -> fail $ "first withPayloadDB failed with: " <> show err
        Right _ -> pure ()

      case secondWithPayloadResult of
        Left err -> fail $ "second withPayloadDB failed with: " <> show err
        Right _ -> pure ()

      secondCount `shouldBe` 0

    it "enqueues and dequeues concurrently withPayload" $ \testDB -> do
      let withPool' = flip withConnection testDB
          elementCount = 1000 :: Int
          expected = [0 .. elementCount - 1]

      ref <- newTVarIO []

      loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
        lastCount <- either throwM return <=< withPayload c 1 $ \(Payload {..}) -> do
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
{-
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
