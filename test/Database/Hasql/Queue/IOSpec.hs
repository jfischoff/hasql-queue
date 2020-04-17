{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Database.Hasql.Queue.IOSpec where
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Control.Exception as E
import           Control.Monad
import           Data.Aeson
import           Data.Function
import           Data.IORef
import           Data.List
import           Hasql.Queue.IO
import           Hasql.Queue.Migrate
import           Test.Hspec                     (SpecWith, Spec, describe, it, afterAll, beforeAll, runIO)
import           Test.Hspec.Expectations.Lifted
import           Data.List.Split
import           Database.Postgres.Temp as Temp
import           Data.Pool
import           Data.Foldable
import           Crypto.Hash.SHA1 (hash)
import qualified Data.ByteString.Base64.URL as Base64
import qualified Data.ByteString.Char8 as BSC
import           Hasql.Connection
import           Hasql.Session
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Int
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


enqueuDequeueSpecs
  :: (Connection -> E.Value Int32 -> [Int32] -> IO a)
  -> (WithNotifyHandlers -> Connection -> D.Value Int32 -> Int -> IO [b])
  -> (b -> Int32)
  -> SpecWith (Pool Connection)
enqueuDequeueSpecs enqueuer withFunc extractor = forM_ [[1], [1,2]] $ \initial -> do
  it "dequeueWith blocks until something is enqueued: before" $ withConnection $ \conn -> do
      void $ enqueuer conn E.int4 initial
      res <- withFunc mempty conn D.int4 $ length initial
      (sort . fmap extractor) res `shouldBe` sort initial

  it "dequeueWith blocks until something is enqueued: during" $ withConnection $ \conn -> do
    afterActionMVar  <- newEmptyMVar
    beforeNotifyMVar <- newEmptyMVar

    let handlers = WithNotifyHandlers
          { withNotifyHandlersAfterAction = putMVar afterActionMVar ()
          , withNotifyHandlersBefore      = takeMVar beforeNotifyMVar
          }

    -- This is the definition of IO.dequeue
    resultThread <- async $ withFunc handlers conn D.int4 $ length initial
    takeMVar afterActionMVar

    void $ enqueuer conn E.int4 initial

    putMVar beforeNotifyMVar ()

    fmap (sort . map extractor) (wait (resultThread)) `shouldReturn` sort initial

  it "dequeue blocks until something is enqueued: after" $ withConnection $ \conn -> do
    resultThread <- async $ withFunc mempty conn D.int4 $ length initial
    void $ enqueuer conn E.int4 initial

    fmap (sort . fmap extractor) (wait resultThread) `shouldReturn` sort initial

data FailedWithPayload = FailedWithPayload
  deriving (Show, Eq, Typeable)

instance Exception FailedWithPayload

spec :: Spec
spec = describe "Hasql.Queue.IO" $ do
  aroundAll withSetup $ describe "basic" $ do
    -- I need to test that enqueue occurs before everything
    -- enqueue during the Session.dequeue
    -- After Session.dequeue
    enqueuDequeueSpecs enqueue_ dequeueWith       pValue
    enqueuDequeueSpecs enqueue  dequeueWith       pValue
    enqueuDequeueSpecs enqueue_ dequeueValuesWith id
    enqueuDequeueSpecs enqueue  dequeueValuesWith id

    it "withPayload blocks until something is enqueued: before" $ withConnection $ \conn -> do
      void $ enqueue conn E.int4 [1]
      res <- withPayloadWith @IOException mempty conn D.int4 1 pure
      pValue res `shouldBe` 1

    it "withPayload blocks until something is enqueued: during" $ withConnection $ \conn -> do
      afterActionMVar  <- newEmptyMVar
      beforeNotifyMVar <- newEmptyMVar

      let handlers = WithNotifyHandlers
            { withNotifyHandlersAfterAction = putMVar afterActionMVar ()
            , withNotifyHandlersBefore      = takeMVar beforeNotifyMVar
            }

      -- This is the definition of IO.dequeue
      resultThread <- async $ withPayloadWith @IOException handlers conn D.int4 1 pure
      takeMVar afterActionMVar

      void $ enqueue conn E.int4 [1]

      putMVar beforeNotifyMVar ()

      fmap pValue (wait resultThread) `shouldReturn` 1

    it "withPayload blocks until something is enqueued: after" $ withConnection $ \conn -> do
      resultThread <- async $ withPayloadWith @IOException mempty conn D.int4 1 pure
      void $ enqueue conn E.int4 [1]

      fmap pValue (wait resultThread) `shouldReturn` 1

    it "withPayload fails and sets the retries to +1" $ withConnection $ \conn -> do
      [payloadId] <- enqueue conn E.int4 [1]
      handle (\FailedWithPayload -> pure ()) $ withPayload conn D.int4 0 $ \_ -> throwIO FailedWithPayload
      Just Payload {..} <- getPayload conn D.int4 payloadId

      pState `shouldBe` Failed
      pAttempts  `shouldBe` 1

    it "withPayload succeeds even if the first attempt fails" $ withConnection $ \conn -> do
      [payloadId] <- enqueue conn E.int4 [1]

      ref <- newIORef (0 :: Int)

      withPayloadWith @FailedWithPayload mempty conn D.int4 1 (\_ -> do
        count <- readIORef ref
        writeIORef ref $ count + 1
        when (count < 1) $ throwIO FailedWithPayload
        pure '!') `shouldReturn` '!'

      Just Payload {..} <- getPayload conn D.int4 payloadId

      pState `shouldBe` Dequeued
      -- Failed attempts I guess
      pAttempts  `shouldBe` 1

    it "enqueues and dequeues concurrently withPayload" $ \testDB -> do
      let withPool' = flip withConnection testDB
          elementCount = 1000 :: Int
          expected = [0 .. elementCount - 1]

      ref <- newTVarIO []

      loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
        lastCount <- withPayload c D.int4 1 $ \(Payload {..}) -> do
          atomically $ do
            xs <- readTVar ref
            writeTVar ref $ pValue : xs
            return $ length xs + 1

        when (lastCount < elementCount) next

      forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
         forM_ xs $ \i -> enqueue c E.int4 [fromIntegral i]

      _ <- waitAnyCancel loopThreads
      xs <- atomically $ readTVar ref
      let Just decoded = mapM (decode . encode) xs
      sort decoded `shouldBe` sort expected

    it "enqueue returns a PayloadId that cooresponds to the entry it added" $ withConnection $ \conn -> do
      [payloadId] <- enqueue conn E.int4 [1]
      Just actual <- getPayload conn D.int4 payloadId

      pValue actual `shouldBe` 1

  aroundAll withSetup $ describe "basic" $ do
    it "enqueues and dequeues concurrently dequeue" $ \testDB -> do
      let withPool' = flip withConnection testDB
          elementCount = 1000 :: Int
          expected = [0 .. elementCount - 1]

      ref <- newTVarIO []

      loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
        [Payload {..}] <- dequeue c D.int4 1
        lastCount <- atomically $ do
          xs <- readTVar ref
          writeTVar ref $ pValue : xs
          return $ length xs + 1

        when (lastCount < elementCount) next

      forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
         forM_ xs $ \i -> enqueue c E.int4 [fromIntegral i]

      _ <- waitAnyCancel loopThreads
      xs <- atomically $ readTVar ref
      let Just decoded = mapM (decode . encode) xs
      sort decoded `shouldBe` sort expected
