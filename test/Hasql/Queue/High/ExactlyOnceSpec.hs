{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Hasql.Queue.High.ExactlyOnceSpec where
import           Hasql.Queue.Internal
import           Control.Concurrent
import           Control.Exception as E
import           Control.Monad
import           Hasql.Queue.High.ExactlyOnce
import qualified Hasql.Queue.Low.ExactlyOnce as Low
import           Hasql.Queue.Migrate
import           Test.Hspec                     (Spec, describe, parallel, it)
import           Test.Hspec.Expectations.Lifted
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Test.Hspec.Core.Spec (sequential)
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Typeable
import           Data.Int
import           Hasql.Queue.TestUtils

newtype TooManyRetries = TooManyRetries Int64
  deriving (Show, Eq, Typeable)

instance Exception TooManyRetries

spec :: Spec
spec = describe "Hasql.Queue.High.ExactlyOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "basic" $ do
    it "is okay to migrate multiple times" $ withConnection $ \conn ->
      liftIO $ migrate conn "int4"

    it "empty locks nothing" $ \pool -> do
      runReadCommitted pool (withDequeue D.int4 8 1 return) >>= \x ->
        x `shouldBe` Nothing
    it "empty gives count 0" $ \pool ->
      runReadCommitted pool getCount `shouldReturn` 0

    it "failed paging works" $ \pool -> do
      runImplicitTransaction pool $ enqueue E.int4 [1,2,3,4]

      replicateM_ 8 $ E.handle (\(_ :: TooManyRetries) -> pure ()) $
        runImplicitTransaction pool $ do
          void $ withDequeue D.int4 1 1 $ const $
            throwM $ TooManyRetries 1

      (a, b) <- runImplicitTransaction pool $ do
        (next, xs) <- failed D.int4 Nothing 2
        (_, ys) <- failed D.int4 (Just next) 2

        pure (xs, ys)

      a `shouldBe` [1,2]
      b `shouldBe` [3,4]

    it "enqueue/withDequeue" $ \pool -> do
      (withDequeueResult, firstCount, secondCount) <- runReadCommitted pool $ do
        Low.enqueue "hey" E.int4 [1]
        firstCount <- getCount
        withDequeueResult <- withDequeue D.int4 8 1 (`shouldBe` [1])

        secondCount <- getCount
        pure (withDequeueResult, firstCount, secondCount)

      firstCount `shouldBe` 1
      secondCount `shouldBe` 0
      withDequeueResult `shouldBe` Just ()

    it "enqueue/withDequeue/retries" $ \pool -> do
      runImplicitTransaction pool $ enqueue E.int4 [1]

      e <- E.try $ runImplicitTransaction pool $ do
        theCount <- getCount

        void $ withDequeue D.int4 8 1 $ const $
            throwM $ TooManyRetries theCount

      (e :: Either TooManyRetries ()) `shouldBe` Left (TooManyRetries 1)

      runImplicitTransaction pool (dequeuePayload D.int4 1) >>= \[(Payload {..})] -> do
        pAttempts `shouldBe` 1
        pValue `shouldBe` 1

      runImplicitTransaction pool $ enqueue E.int4 [1]

      e1 <- E.try $ runImplicitTransaction pool $ do
        theCount <- getCount

        void $ withDequeue D.int4 8 1 $ const $
            throwM $ TooManyRetries theCount

      (e1 :: Either TooManyRetries ()) `shouldBe` Left (TooManyRetries 1)

      replicateM_ 6 $ E.handle (\(_ :: TooManyRetries) -> pure ()) $ runImplicitTransaction pool $ do
          void $ withDequeue D.int4 8 1 $ const $
            throwM $ TooManyRetries 1

      runImplicitTransaction pool (dequeuePayload D.int4 1) >>= \[(Payload {..})] -> do
        pAttempts `shouldBe` 7
        pValue `shouldBe` 1

    it "enqueue/withDequeue/timesout" $ \pool -> do
      e <- E.try $ runReadCommitted pool $ do
        enqueue E.int4 [1]
        firstCount <- getCount

        void $ withDequeue D.int4 0 1 $ const $
            throwM $ TooManyRetries firstCount

      (e :: Either TooManyRetries ())`shouldBe` Left (TooManyRetries 1)

      runReadCommitted pool getCount `shouldReturn` 0

    it "selects the oldest first" $ \pool -> do
      (firstCount, firstwithDequeueResult, secondwithDequeueResult, secondCount) <- runReadCommitted pool $ do
        enqueue E.int4 [1]
        liftIO $ threadDelay 100

        enqueue E.int4 [2]

        firstCount <- getCount

        firstwithDequeueResult   <- withDequeue D.int4 8 1 (`shouldBe` [1])
        secondwithDequeueResult <- withDequeue D.int4 8 1 (`shouldBe` [2])

        secondCount <- getCount
        pure (firstCount, firstwithDequeueResult, secondwithDequeueResult, secondCount)

      firstCount `shouldBe` 2
      firstwithDequeueResult `shouldBe` Just ()

      secondCount `shouldBe` 0
      secondwithDequeueResult `shouldBe` Just ()
