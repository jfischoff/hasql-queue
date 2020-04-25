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
import           Data.ByteString (ByteString)
import           Hasql.Connection
import           Hasql.Session
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Int
import           Data.Typeable
import qualified Hasql.Queue.Internal as I
import           Hasql.Queue.Internal (Payload (..))

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

runThrow :: Session a -> Connection -> IO a
runThrow sess conn = either (throwIO . I.QueryException) pure =<< run sess conn

getCount :: Connection -> IO Int64
getCount = runThrow I.getCount

getPayload :: Connection -> D.Value a -> I.PayloadId -> IO (Maybe (I.Payload a))
getPayload conn decoder payloadId = runThrow (I.getPayload decoder payloadId) conn

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

channel :: ByteString
channel = "hey"

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

{-
runNoTransaction :: Pool Connection -> Session a -> IO a
runNoTransaction pool session = withResource pool $ \conn ->
  either (throwIO . userError . show) pure =<< run action conn
-}

data FailedwithDequeue = FailedwithDequeue
  deriving (Show, Eq, Typeable)

instance Exception FailedwithDequeue

spec :: Spec
spec = describe "Hasql.Queue.IO" $ do
  aroundAll withSetup $ describe "basic" $ do
    it "withDequeue blocks until something is enqueued: before" $ withConnection $ \conn -> do
      void $ enqueue channel conn E.int4 [1]
      res <- withDequeueWith @IOException mempty channel conn D.int4 1 1 pure
      res `shouldBe` [1]
      getCount conn `shouldReturn` 0

    it "withDequeue blocks until something is enqueued: during" $ withConnection $ \conn -> do
      afterActionMVar  <- newEmptyMVar
      beforeNotifyMVar <- newEmptyMVar

      let handlers = WithNotifyHandlers
            { withNotifyHandlersAfterAction = putMVar afterActionMVar ()
            , withNotifyHandlersBefore      = takeMVar beforeNotifyMVar
            }

      -- This is the definition of IO.dequeue
      resultThread <- async $ withDequeueWith @IOException handlers channel conn D.int4 1 1 pure
      takeMVar afterActionMVar

      void $ enqueue "hey" conn E.int4 [1]

      putMVar beforeNotifyMVar ()

      wait resultThread `shouldReturn` [1]

    it "withDequeue blocks until something is enqueued: after" $ withConnection $ \conn -> do
      resultThread <- async $ withDequeueWith @IOException mempty channel conn D.int4 1 1 pure
      void $ enqueue channel conn E.int4 [1]

      wait resultThread `shouldReturn` [1]

    it "withDequeue fails and sets the retries to +1" $ withConnection $ \conn -> do
      [payloadId] <- runThrow (I.enqueuePayload E.int4 [1]) conn
      handle (\FailedwithDequeue -> pure ()) $ withDequeue channel conn D.int4 0 1 $ \_ -> throwIO FailedwithDequeue
      Just Payload {..} <- getPayload conn D.int4 payloadId

      pState `shouldBe` I.Failed
      pAttempts  `shouldBe` 1

    it "withDequeue succeeds even if the first attempt fails" $ withConnection $ \conn -> do
      [payloadId] <- runThrow (I.enqueuePayload E.int4 [1]) conn

      ref <- newIORef (0 :: Int)

      withDequeueWith @FailedwithDequeue mempty channel conn D.int4 1 1 (\_ -> do
        count <- readIORef ref
        writeIORef ref $ count + 1
        when (count < 1) $ throwIO FailedwithDequeue
        pure '!') `shouldReturn` '!'

      Just Payload {..} <- getPayload conn D.int4 payloadId

      pState `shouldBe` I.Dequeued
      -- Failed attempts I guess
      pAttempts  `shouldBe` 1

    it "enqueues and dequeues concurrently withDequeue" $ \testDB -> do
      let withPool' = flip withConnection testDB
          elementCount = 1000 :: Int
          expected = [0 .. elementCount - 1]

      ref <- newTVarIO []

      loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
        lastCount <- withDequeue channel c D.int4 1 1 $ \[x] -> do
          atomically $ do
            xs <- readTVar ref
            writeTVar ref $ x : xs
            return $ length xs + 1

        when (lastCount < elementCount) next

      forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
         forM_ xs $ \i -> enqueue channel c E.int4 [fromIntegral i]

      _ <- waitAnyCancel loopThreads
      xs <- atomically $ readTVar ref
      let Just decoded = mapM (decode . encode) xs
      sort decoded `shouldBe` sort expected

    it "enqueue returns a PayloadId that cooresponds to the entry it added" $ withConnection $ \conn -> do
      [payloadId] <- runThrow (I.enqueuePayload E.int4 [1]) conn
      Just actual <- getPayload conn D.int4 payloadId

      pValue actual `shouldBe` 1
