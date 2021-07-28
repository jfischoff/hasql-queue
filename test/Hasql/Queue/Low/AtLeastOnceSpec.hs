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
import           Data.Text(Text)
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Int
import           Data.Typeable
import qualified Hasql.Queue.Internal as I
import           Hasql.Queue.Internal (Payload (..))
import           Hasql.Queue.TestUtils
import           System.Timeout

getCount :: Connection -> IO Int64
getCount = I.runThrow I.getCount

getPayload :: Connection -> D.Value a -> I.PayloadId -> IO (Maybe (I.Payload a))
getPayload conn decoder payloadId = I.runThrow (I.getPayload decoder payloadId) conn

channel :: Text
channel = "hey"

data FailedwithDequeue = FailedwithDequeue
  deriving (Show, Eq, Typeable)

instance Exception FailedwithDequeue

spec :: Spec
spec = describe "Hasql.Queue.Low.AtLeastOnce" $ aroundAll withSetup $ describe "enqueue/withDequeue" $ do
  it "enqueue nothing timesout" $ withConnection $ \conn -> do
    enqueue channel conn E.int4 []
    timeout 100000 (withDequeue channel conn D.int4 1 1 pure) `shouldReturn` Nothing

  it "enqueue 1 gives 1" $ withConnection $ \conn -> do
    enqueue channel conn E.int4 [1]
    withDequeue channel conn D.int4 1 1 pure `shouldReturn` [1]

  it "dequeue timesout after enqueueing everything" $ withConnection $ \conn -> do
    timeout 100000 (withDequeue channel conn D.int4 1 1 pure) `shouldReturn` Nothing

  it "dequeueing is in FIFO order" $ withConnection $ \conn -> do
    enqueue channel conn E.int4 [1]
    enqueue channel conn E.int4 [2]
    withDequeue channel conn D.int4 1 1 pure `shouldReturn` [1]
    withDequeue channel conn D.int4 1 1 pure `shouldReturn` [2]

  it "dequeueing a batch of elements works" $ withConnection $ \conn -> do
    enqueue channel conn E.int4 [1, 2, 3]
    withDequeue channel conn D.int4 1 2 pure `shouldReturn` [1, 2]

    withDequeue channel conn D.int4 1 1 pure `shouldReturn` [3]

  it "withDequeue blocks until something is enqueued: before" $ withConnection $ \conn -> do
    void $ enqueue channel conn E.int4 [1]
    res <- withDequeue channel conn D.int4 1 1 pure
    res `shouldBe` [1]

  it "withDequeue blocks until something is enqueued: during" $ withConnection $ \conn -> do
    afterActionMVar  <- newEmptyMVar
    beforeNotifyMVar <- newEmptyMVar

    let handlers = I.WithNotifyHandlers
          { withNotifyHandlersAfterAction        = putMVar afterActionMVar ()
          , withNotifyHandlersBeforeNotification = takeMVar beforeNotifyMVar
          }

    -- This is the definition of IO.dequeue
    resultThread <- async $ withDequeueWith @IOError handlers channel conn D.int4 1 1 pure
    takeMVar afterActionMVar

    void $ enqueue "hey"  conn E.int4 [1]

    putMVar beforeNotifyMVar ()

    wait resultThread `shouldReturn` [1]

  it "withDequeue blocks until something is enqueued: after" $ withConnection2 $ \(conn1, conn2) -> do
    thread <- async $ withDequeue channel conn1 D.int4 1 1 pure
    timeout 100000 (wait thread) `shouldReturn` Nothing

    enqueue channel conn2 E.int4 [1]

    wait thread `shouldReturn` [1]

  -- TODO redo just using failures
  it "withDequeue fails and sets the retries to +1" $ withConnection $ \conn -> do
    enqueue channel conn E.int4 [1]
    handle (\(_ :: IOError) -> pure ()) $ withDequeue channel conn D.int4 0 1 $ \_ -> throwIO $ userError "hey"
    xs <- failures conn D.int4 Nothing 1

    map snd xs `shouldBe` [1]

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

  it "enqueue returns a PayloadId that corresponds to the entry it added" $ withConnection $ \conn -> do
    [payloadId] <- I.runThrow (I.enqueuePayload E.int4 [1]) conn
    Just actual <- getPayload conn D.int4 payloadId

    pId actual `shouldBe` payloadId
