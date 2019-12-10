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
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.Transaction
import           Database.PostgreSQL.Simple.Queue
import           Database.PostgreSQL.Simple.Queue.Migrate
import           Database.PostgreSQL.Transact as T
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

aroundAll :: forall a. ((a -> IO ()) -> IO ()) -> SpecWith a -> Spec
aroundAll withFunc specWith = do
  (var, stopper, asyncer) <- runIO $
    (,,) <$> newEmptyMVar <*> newEmptyMVar <*> newIORef Nothing
  let theStart :: IO a
      theStart = do
        thread <- async $ withFunc $ \x -> do
          putMVar var x
          takeMVar stopper

        writeIORef asyncer $ Just thread

        takeMVar var

      theStop :: a -> IO ()
      theStop _ = do
        putMVar stopper ()
        traverse_ wait =<< readIORef asyncer

  beforeAll theStart $ afterAll theStop $ specWith

schemaName :: String
schemaName = "complicated_name"

withConn :: Temp.DB -> (Connection -> IO a) -> IO a
withConn db f = do
  let connStr = toConnectionString db
  E.bracket (connectPostgreSQL connStr) close f

withSetup :: (Pool Connection -> IO ()) -> IO ()
withSetup f = either throwIO pure <=< withDbCache $ \dbCache -> do
{-
  let opts = [ ("log_min_duration_statement", "9ms")
             , ("shared_preload_libraries", "'auto_explain'")
             , ("session_preload_libraries", "'auto_explain'")
             , ("auto_explain.log_analyze", "1")
             , ("auto_explain.log_buffers", "1")
             , ("auto_explain.log_timing", "1")
             , ("auto_explain.log_triggers", "1")
             , ("auto_explain.log_verbose", "1")
             , ("auto_explain.log_min_duration", "10ms")
             , ("auto_explain.log_nested_statements", "1")
             , ("auto_explain.sample_rate", "1")
             , ("auto_explain.log_level", "WARNING")
             , ("log_connections", "off")
             , ("log_disconnections", "off")
             ]
-}
  migratedConfig <- either throwIO pure =<<
      cacheAction
        ".test-cache/6"
        (flip withConn (migrate schemaName))
        (defaultConfig <> cacheConfig dbCache)
  withConfig migratedConfig $ \db -> do
    f =<< createPool
      (connectPostgreSQL $ toConnectionString db)
      close
      2
      60
      50

withConnection :: (Connection -> IO ()) -> Pool Connection -> IO ()
withConnection = flip withResource

withReadCommitted :: T.DB () -> Pool Connection -> IO ()
withReadCommitted action pool = withResource pool $
  T.runDBT action ReadCommitted

runDB :: Connection -> T.DB a -> IO a
runDB conn action = T.runDBT action ReadCommitted conn

spec :: Spec
spec = describe "Database.Queue" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "basic" $ do
    it "is okay to migrate multiple times" $ withConnection $
      liftIO . migrate schemaName

    it "empty locks nothing" $ withReadCommitted $
      (either throwM return =<< (withPayloadDB schemaName 8 return))
        `shouldReturn` Nothing
    it "empty gives count 0" $ withReadCommitted $
      getCountDB schemaName `shouldReturn` 0
    it "enqueuesDB/withPayloadDB" $ withReadCommitted $ T.rollback $ do
        payloadId <- enqueueDB schemaName $ String "Hello"
        getCountDB schemaName `shouldReturn` 1

        either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
          pId `shouldBe` payloadId
          pValue `shouldBe` String "Hello"
          )

        getCountDB schemaName `shouldReturn` 0

    it "enqueuesDB/withPayloadDB/retries" $ withReadCommitted $ T.rollback $ do
      void $ enqueueDB schemaName $ String "Hello"
      getCountDB schemaName `shouldReturn` 1

      xs <- replicateM 7 $ withPayloadDB schemaName 8 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      either throwM (const $ pure ()) =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pAttempts `shouldBe` 7
        pValue `shouldBe` String "Hello"
        )
    it "enqueuesDB/withPayloadDB/timesout" $ withReadCommitted $ T.rollback $  do
      void $ enqueueDB schemaName $ String "Hello"
      getCountDB schemaName `shouldReturn` 1

      xs <- replicateM 2 $ withPayloadDB schemaName 1 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      getCountDB schemaName `shouldReturn` 0

    it "selects the oldest first" $ withReadCommitted $ T.rollback $ do
      payloadId0 <- enqueueDB schemaName $ String "Hello"
      liftIO $ threadDelay 100

      payloadId1 <- enqueueDB schemaName $ String "Hi"

      getCountDB schemaName `shouldReturn` 2

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId0
        pValue `shouldBe` String "Hello"
        )

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId1
        pValue `shouldBe` String "Hi"
        )

      getCountDB schemaName `shouldReturn` 0

  aroundAll withSetup $ describe "basic" $ do
    it "enqueues and dequeues concurrently withPayload" $ \testDB -> do
      let withPool' = flip withConnection testDB
          elementCount = 1000 :: Int
          expected = [0 .. elementCount - 1]

      ref <- newTVarIO []

      loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
        lastCount <- either throwM return <=< withPayload schemaName c 0 $ \(Payload {..}) -> do
          atomically $ do
            xs <- readTVar ref
            writeTVar ref $ pValue : xs
            return $ length xs + 1

        when (lastCount < elementCount) next

      forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
         forM_ xs $ \i -> enqueue schemaName c $ toJSON i

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
        Payload {..} <- dequeue schemaName c
        lastCount <- atomically $ do
          xs <- readTVar ref
          writeTVar ref $ pValue : xs
          return $ length xs + 1

        when (lastCount < elementCount) next

      forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
         forM_ xs $ \i -> enqueue schemaName c $ toJSON i

      waitAnyCancel loopThreads
      xs <- atomically $ readTVar ref
      let Just decoded = mapM (decode . encode) xs
      sort decoded `shouldBe` sort expected
