module Hasql.Queue.High.AtLeastOnceSpec where
import           Hasql.Queue.High.AtLeastOnce
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Test.Hspec                     (Spec, describe, parallel, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.Core.Spec (sequential)
import           Hasql.Queue.TestUtils
import qualified Hasql.Queue.Internal as I
import           Control.Exception as E
import           Hasql.Connection
import           Data.Typeable
import           Data.IORef
import           Control.Monad

data FailedwithDequeue = FailedwithDequeue
  deriving (Show, Eq, Typeable)

instance Exception FailedwithDequeue

withDequeueNoFilter :: Connection
                    -- ^ Connection
                    -> D.Value a
                    -- ^ Payload decoder
                    -> Int
                    -- ^ Retry count
                    -> Int
                    -- ^ Element count
                    -> ([a] -> IO b)
                    -- ^ Continuation
                    -> IO (Maybe b)
withDequeueNoFilter c = withDequeue c ""

getPayload :: Connection -> D.Value a -> I.PayloadId -> IO (Maybe (I.Payload a))
getPayload conn decoder payloadId = I.runThrow (I.getPayload decoder payloadId) conn

spec :: Spec
spec = describe "Hasql.Queue.High.AtLeastOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "enqueue/dequeue" $ do
    it "enqueue nothing gives nothing" $ withConnection $ \conn -> do
      enqueue conn E.int4 []
      withDequeueNoFilter conn D.int4 1 1 pure `shouldReturn` Nothing

    it "enqueue 1 gives 1" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1]
      withDequeueNoFilter conn D.int4 1 1 pure `shouldReturn` Just [1]

    it "dequeue give nothing after enqueueing everything" $ withConnection $ \conn -> do
      withDequeueNoFilter conn D.int4 1 1 pure `shouldReturn` Nothing

    it "dequeueing is in FIFO order" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1]
      enqueue conn E.int4 [2]
      withDequeueNoFilter conn D.int4 1 1 pure `shouldReturn` Just [1]
      withDequeueNoFilter conn D.int4 1 1 pure `shouldReturn` Just [2]

    it "dequeueing a batch of elements works" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1, 2, 3]
      withDequeueNoFilter conn D.int4 1 2 pure `shouldReturn` Just [1, 2]

      withDequeueNoFilter conn D.int4 1 2 pure `shouldReturn` Just [3]

    it "withDequeueNoFilter fails if a non IOError is thrown" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1]
      handle (\FailedwithDequeue -> pure Nothing) $
        withDequeueNoFilter conn D.int4 2 1 $ \_ -> throwIO FailedwithDequeue

      failures conn D.int4 Nothing 1 `shouldReturn` []
      withDequeueNoFilter conn D.int4 0 1 pure `shouldReturn` Just [1]

    it "withDequeueNoFilter fails if throws occur and retry is zero" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1]
      handle (\(_ :: IOError) -> pure Nothing) $
        withDequeueNoFilter conn D.int4 0 1 $ \_ -> throwIO $ userError "hey"

      [(pId, x)] <- failures conn D.int4 Nothing 1
      x `shouldBe` 1
      delete conn [pId]

    it "withDequeueNoFilter succeeds even if the first attempt fails" $ withConnection $ \conn -> do
      enqueue conn E.int4 [1]

      ref <- newIORef (0 :: Int)

      withDequeueNoFilter conn D.int4 1 1 (\_ -> do
        count <- readIORef ref
        writeIORef ref $ count + 1
        when (count < 1) $ throwIO $ userError "hey"
        pure '!') `shouldReturn` Just '!'

      withDequeueNoFilter conn D.int4 1 1 pure `shouldReturn` Nothing
      readIORef ref `shouldReturn` 2

    it "failures paging works" $ withConnection $ \conn -> do
      enqueue conn E.int4 [2]
      enqueue conn E.int4 [3]

      handle (\(_ :: IOError) -> pure Nothing) $
        withDequeueNoFilter conn D.int4 0 1 $ \_ -> throwIO $ userError "fds"
      handle (\(_ :: IOError) -> pure Nothing) $
        withDequeueNoFilter conn D.int4 0 1 $ \_ -> throwIO $ userError "fds"

      [(next, x)] <- failures conn D.int4 Nothing 1
      x `shouldBe` 2
      fmap (fmap snd) (failures conn D.int4 (Just next) 2) `shouldReturn` [3]
