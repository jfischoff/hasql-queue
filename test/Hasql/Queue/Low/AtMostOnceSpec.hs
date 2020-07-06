module Hasql.Queue.Low.AtMostOnceSpec where
import           Hasql.Queue.Low.AtMostOnce
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Test.Hspec                     (Spec, describe, parallel, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.Core.Spec (sequential)
import           Hasql.Queue.TestUtils
import           Data.Text(Text)
import           System.Timeout
import           Control.Concurrent.Async

channel :: Text
channel = "channel"

spec :: Spec
spec = describe "Hasql.Queue.Low.AtMostOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "enqueue/dequeue" $ do
    it "enqueue nothing timesout" $ withConnection $ \conn -> do
      enqueue channel conn E.int4 []
      timeout 100000 (dequeue channel conn D.int4 1) `shouldReturn` Nothing

    it "enqueue 1 gives 1" $ withConnection $ \conn -> do
      enqueue channel conn E.int4 [1]
      dequeue channel conn D.int4 1 `shouldReturn` [1]

    it "dequeue timesout after enqueueing everything" $ withConnection $ \conn -> do
      timeout 100000 (dequeue channel conn D.int4 1) `shouldReturn` Nothing

    it "dequeueing is in FIFO order" $ withConnection $ \conn -> do
      enqueue channel conn E.int4 [1]
      enqueue channel conn E.int4 [2]
      dequeue channel conn D.int4 1 `shouldReturn` [1]
      dequeue channel conn D.int4 1 `shouldReturn` [2]

    it "dequeueing a batch of elements works" $ withConnection $ \conn -> do
      enqueue channel conn E.int4 [1, 2, 3]
      dequeue channel conn D.int4 2 `shouldReturn` [1, 2]

      dequeue channel conn D.int4 2 `shouldReturn` [3]

    it "dequeueing blocks until something is enqueued" $ withConnection2 $ \(conn1, conn2) -> do
      thread <- async $ dequeue channel conn1 D.int4 1
      timeout 100000 (wait thread) `shouldReturn` Nothing

      enqueue channel conn2 E.int4 [1]

      wait thread `shouldReturn` [1]
