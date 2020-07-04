module Hasql.Queue.High.AtMostOnceSpec where
import           Hasql.Queue.High.AtMostOnce
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Test.Hspec                     (Spec, describe, parallel, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.Core.Spec (sequential)
import           Hasql.Queue.TestUtils

spec :: Spec
spec = describe "Hasql.Queue.High.AtMostOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "enqueue/dequeue" $ do
    it "enqueue nothing gives nothing" $ withConnection $ \conn -> do
      enqueue conn E.int4 []
      dequeue conn D.int4 1 `shouldReturn` []

    it "enqueue 1 gives 1" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1]
      dequeue conn D.int4 1 `shouldReturn` [1]

    it "dequeue give nothing after enqueueing everything" $ withConnection $ \conn -> do
      dequeue conn D.int4 1 `shouldReturn` []

    it "dequeueing is in FIFO order" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1]
      enqueue conn E.int4 [2]
      dequeue conn D.int4 1 `shouldReturn` [1]
      dequeue conn D.int4 1 `shouldReturn` [2]

    it "dequeueing a batch of elements works" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1, 2, 3]
      dequeue conn D.int4 2 `shouldReturn` [1, 2]

      dequeue conn D.int4 2 `shouldReturn` [3]
