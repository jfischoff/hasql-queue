module Hasql.Queue.Low.ExactlyOnceSpec where
import           Control.Exception as E
import           Hasql.Queue.Low.ExactlyOnce
import           Test.Hspec                     (Spec, describe, parallel, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.Core.Spec (sequential)
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Typeable
import           Data.Int
import           Hasql.Queue.TestUtils
import           System.Timeout
import           Data.ByteString(ByteString)
import           Control.Concurrent.Async
import           Hasql.Queue.Internal (runThrow)
import           Hasql.Connection
import           Hasql.Session

-- Fix this to be more of what I would expec

newtype TooManyRetries = TooManyRetries Int64
  deriving (Show, Eq, Typeable)

instance Exception TooManyRetries

channel :: ByteString
channel = "channel"

runSession :: Connection -> Session [a] -> IO [a]
runSession conn sess = runThrow sess conn

spec :: Spec
spec = describe "Hasql.Queue.High.ExactlyOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "enqueue/dequeue" $ do
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

    it "dequeueing blocks until something is enqueued" $ withConnection2 $ \(conn1, conn2) -> do
      thread <- async $ withDequeue channel conn1 D.int4 1 runSession
      timeout 100000 (wait thread) `shouldReturn` Nothing

      runThrow (enqueue channel E.int4 [1]) conn2

      wait thread `shouldReturn` [1]
