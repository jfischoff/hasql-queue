{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
{-# LANGUAGE ScopedTypeVariables   #-}
module Database.Hasql.Queue.PartitionSpec where
import Hasql.Queue.Partition
import Hasql.Queue.IO
import Database.Hasql.Queue.TestUtils
import Control.Concurrent.STM
import Test.Hspec (Spec, describe, it, shouldBe, pending)
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Control.Concurrent
import           Data.IORef

spec :: Spec
spec = describe "Hasql.Queue.PartitionSpec" $ aroundAll withSetup $ do

  it "does nothing when the tables are empty" $ withConnection $ \conn -> do
    pending
    mconn <- newMVar conn
    stop =<< start PartitionManagerConfig
      { pmcConnection         = withMVar mconn
      , pmcDelay              = 1
      , pmcRangeLength        = 1
      , pmcPartitionCount     = 2
      , pmcHandlers           = PartitionManagerHandlers
          { pmcExceptionLogger      = mempty
          , pmcStartLoop            = mempty
          , pmcRecreatePartitions   = (`shouldBe` 0)
          , pmcActualPartitionCount = (`shouldBe` 2)
          , pmcDropCount            = (`shouldBe` 0)
          }
      }


  it "starts and drop one table" $ withConnection $ \conn -> do
    mconn <- newMVar conn
    waiter <- newTVarIO False

    let waitFor b = atomically $ do
          check . (b==) =<< readTVar waiter
          writeTVar waiter $ not b

    partitionManager <- start PartitionManagerConfig
      { pmcConnection         = withMVar mconn
      , pmcDelay              = 0
      , pmcRangeLength        = 1
      , pmcPartitionCount     = 2
      , pmcHandlers           = PartitionManagerHandlers
          { pmcExceptionLogger      = print
          , pmcStartLoop            = waitFor True
          , pmcRecreatePartitions   = (`shouldBe` 0)
          , pmcActualPartitionCount = (`shouldBe` 2)
          , pmcDropCount            = \x -> (x`shouldBe` 0) >> waitFor True
          }
      }

    withMVar mconn $ \c -> enqueue "d" c E.int4 [1]
    waitFor False
    waitFor False
    waitFor False

    stop partitionManager

  it "starts and drop one table" $ withConnection $ \conn -> do
    waiter <- newTVarIO False
    loopCounter <- newIORef (0 :: Int)
    mconn <- newMVar conn

    let waitFor b = atomically $ do
          check . (b==) =<< readTVar waiter
          writeTVar waiter $ not b

    partitionManager <- start PartitionManagerConfig
      { pmcConnection         = withMVar mconn
      , pmcDelay              = 0.0
      , pmcRangeLength        = 1
      , pmcPartitionCount     = 2
      , pmcHandlers           = PartitionManagerHandlers
          { pmcExceptionLogger      = print
          , pmcStartLoop            = do
              waitFor True
              modifyIORef loopCounter (+1)
          , pmcRecreatePartitions   = (`shouldBe` 0)
          , pmcActualPartitionCount = (`shouldBe` 2)
          , pmcDropCount            = \x -> do
              readIORef loopCounter >>= \case
                1 -> x `shouldBe` 0
                2 -> x `shouldBe` 0
                3 -> x `shouldBe` 1
                4 -> x `shouldBe` 0
                _ -> error "withConnection"
          }
      }

    withMVar mconn $ \c -> enqueue "d" c E.int4 [1]
    waitFor False
    putStrLn "hey"
    withMVar mconn $ \c -> enqueue "d" c E.int4 [1]
    waitFor False
    withMVar mconn $ \c -> withDequeue "d" c D.int4 1 1 pure
    waitFor False
    withMVar mconn $ \c -> enqueue "d" c E.int4 [1]
    waitFor False

    stop partitionManager
