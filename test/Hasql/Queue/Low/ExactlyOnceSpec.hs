module Hasql.Queue.Low.ExactlyOnceSpec where
import           Control.Exception as E
import           Hasql.Queue.Low.ExactlyOnce
import qualified Hasql.Queue.Internal as I
import           Test.Hspec                     (Spec, describe, it)
import           Test.Hspec.Expectations.Lifted
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Typeable
import           Data.Int
import           Hasql.Queue.TestUtils
import           System.Timeout
import           Data.ByteString(ByteString)
import           Control.Concurrent.Async
import           Hasql.Queue.Internal (runThrow)
import           Hasql.Session
import           Control.Concurrent
import           Control.Monad

-- Fix this to be more of what I would expec

newtype TooManyRetries = TooManyRetries Int64
  deriving (Show, Eq, Typeable)

instance Exception TooManyRetries

channel :: ByteString
channel = "channel"

runSession :: Session [a] -> Session [a]
runSession = id

spec :: Spec
spec = describe "Hasql.Queue.High.ExactlyOnce" $ do
  aroundAll withSetup $ describe "enqueue/withDequeue" $ do
    it "enqueue nothing timesout" $ withConnection $ \conn -> do
      runThrow (enqueue channel E.int4 []) conn
      timeout 100000 (withDequeue channel conn D.int4 1 runSession) `shouldReturn` Nothing

    it "enqueue 1 gives 1" $ withConnection $ \conn -> do
      runThrow (enqueue channel E.int4 [1]) conn
      withDequeue channel conn D.int4 1 runSession `shouldReturn` [1]

    it "dequeue timesout after enqueueing everything" $ withConnection $ \conn -> do
      timeout 100000 (withDequeue channel conn D.int4 1 runSession) `shouldReturn` Nothing

    it "dequeueing is in FIFO order" $ withConnection $ \conn -> do
      runThrow (enqueue channel E.int4 [1]) conn
      runThrow (enqueue channel E.int4 [2]) conn
      withDequeue channel conn D.int4 1 runSession `shouldReturn` [1]
      withDequeue channel conn D.int4 1 runSession `shouldReturn` [2]

    it "dequeueing a batch of elements works" $ withConnection $ \conn -> do
      runThrow (enqueue channel E.int4 [1, 2, 3]) conn
      withDequeue channel conn D.int4 1 runSession `shouldReturn` [1, 2]

      withDequeue channel conn D.int4 1 runSession `shouldReturn` [3]

    it "withDequeue blocks until something is enqueued: before" $ withConnection $ \conn -> do
      void $ runThrow (enqueue channel E.int4 [1]) conn
      res <- withDequeue channel conn D.int4 1 id
      res `shouldBe` [1]

    it "withDequeue blocks until something is enqueued: during" $ withConnection $ \conn -> do
      afterActionMVar  <- newEmptyMVar
      beforeNotifyMVar <- newEmptyMVar

      let handlers = I.WithNotifyHandlers
            { withNotifyHandlersAfterAction        = putMVar afterActionMVar ()
            , withNotifyHandlersBeforeNotification = takeMVar beforeNotifyMVar
            }

      -- This is the definition of IO.dequeue
      resultThread <- async $ withDequeueWith handlers channel conn D.int4 1 id
      takeMVar afterActionMVar

      void $ runThrow (enqueue "hey" E.int4 [1]) conn

      putMVar beforeNotifyMVar ()

      wait resultThread `shouldReturn` [1]

    it "withDequeue blocks until something is enqueued: after" $ withConnection2 $ \(conn1, conn2) -> do
      thread <- async $ withDequeue channel conn1 D.int4 1 runSession
      timeout 100000 (wait thread) `shouldReturn` Nothing

      runThrow (enqueue channel E.int4 [1]) conn2

      wait thread `shouldReturn` [1]
