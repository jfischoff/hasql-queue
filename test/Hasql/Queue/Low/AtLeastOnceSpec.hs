{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Hasql.Queue.Low.AtLeastOnceSpec where
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Control.Exception as E
import           Control.Monad
import           Data.Aeson
import           Data.Function
import           Data.IORef
import           Data.List
import           Hasql.Queue.Low.AtLeastOnce
import           Test.Hspec                     (Spec, describe, it)
import           Test.Hspec.Expectations.Lifted
import           Data.List.Split
import           Data.ByteString (ByteString)
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Int
import           Data.Typeable
import qualified Hasql.Queue.Internal as I
import           Hasql.Queue.Internal (Payload (..))
import           Hasql.Queue.TestUtils

getCount :: Connection -> IO Int64
getCount = I.runThrow I.getCount

getPayload :: Connection -> D.Value a -> I.PayloadId -> IO (Maybe (I.Payload a))
getPayload conn decoder payloadId = I.runThrow (I.getPayload decoder payloadId) conn

channel :: ByteString
channel = "hey"

{-
runNoTransaction :: Pool Connection -> Session a -> IO a
runNoTransaction pool session = withResource pool $ \conn ->
  either (throwIO . userError . show) pure =<< run action conn
-}

data FailedwithDequeue = FailedwithDequeue
  deriving (Show, Eq, Typeable)

instance Exception FailedwithDequeue

spec :: Spec
spec = describe "Hasql.Queue.Low.AtLeastOnce" $ do
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
            , withNotifyHandlersBeforeNotification      = takeMVar beforeNotifyMVar
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
      [payloadId] <- I.runThrow (I.enqueuePayload E.int4 [1]) conn
      handle (\FailedwithDequeue -> pure ()) $ withDequeue channel conn D.int4 0 1 $ \_ -> throwIO FailedwithDequeue
      Just Payload {..} <- getPayload conn D.int4 payloadId

      pState `shouldBe` I.Failed
      pAttempts  `shouldBe` 1

    it "withDequeue succeeds even if the first attempt fails" $ withConnection $ \conn -> do
      [payloadId] <- I.runThrow (I.enqueuePayload E.int4 [1]) conn

      ref <- newIORef (0 :: Int)

      withDequeueWith @FailedwithDequeue mempty channel conn D.int4 1 1 (\_ -> do
        count <- readIORef ref
        writeIORef ref $ count + 1
        when (count < 1) $ throwIO FailedwithDequeue
        pure '!') `shouldReturn` '!'

      getPayload conn D.int4 payloadId `shouldReturn` Nothing

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
      [payloadId] <- I.runThrow (I.enqueuePayload E.int4 [1]) conn
      Just actual <- getPayload conn D.int4 payloadId

      pValue actual `shouldBe` 1
