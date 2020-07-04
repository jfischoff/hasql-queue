{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Hasql.Queue.High.ExactlyOnceSpec where
import           Control.Exception as E
import           Hasql.Queue.High.ExactlyOnce
import           Test.Hspec                     (Spec, describe, parallel, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.Core.Spec (sequential)
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Typeable
import           Data.Int
import           Hasql.Queue.TestUtils

-- Fix this to be more of what I would expec

newtype TooManyRetries = TooManyRetries Int64
  deriving (Show, Eq, Typeable)

instance Exception TooManyRetries

spec :: Spec
spec = describe "Hasql.Queue.High.ExactlyOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "enqueue/dequeue" $ do
    it "enqueue nothing gives nothing" $ withReadCommitted $ do
      enqueue E.int4 []
      dequeue D.int4 1 `shouldReturn` []

    it "enqueue 1 gives 1" $ withReadCommitted $ do
      enqueue E.int4 [1]
      dequeue D.int4 1 `shouldReturn` [1]

    it "dequeue give nothing after enqueueing everything" $ withReadCommitted $ do
      dequeue D.int4 1 `shouldReturn` []

    it "dequeueing is in FIFO order" $ withReadCommitted $ do
      enqueue E.int4 [1]
      enqueue E.int4 [2]
      dequeue D.int4 1 `shouldReturn` [1]
      dequeue D.int4 1 `shouldReturn` [2]

    it "dequeueing a batch of elements works" $ withReadCommitted $ do
      enqueue E.int4 [1, 2, 3]
      dequeue D.int4 2 `shouldReturn` [1, 2]

      dequeue D.int4 2 `shouldReturn` [3]
