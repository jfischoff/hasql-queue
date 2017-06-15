{-# LANGUAGE RecordWildCards, OverloadedStrings #-}
module Database.QueueSpec (spec, main) where
import Database.Queue
import Test.Hspec (Spec, hspec, it)
import Test.Hspec.Expectations.Lifted
import Test.Setup
import Data.Aeson
import Control.Concurrent
import Data.IORef
import Control.Monad
import Data.Function
import Data.List
import Control.Concurrent.Async

main :: IO ()
main = hspec spec

spec :: Spec
spec = describeDB "Database.Queue" $ do
  itDB "empty locks nothing" $ do
    tryLockDB `shouldReturn` Nothing

  itDB "enqueues/locks/dequeues" $ do
    payloadId <- enqueueDB $ String "Hello"
    Just Payload {..} <- tryLockDB

    pId `shouldBe` payloadId
    pValue `shouldBe` String "Hello"
    tryLockDB `shouldReturn` Nothing

    dequeueDB pId `shouldReturn` ()
    tryLockDB `shouldReturn` Nothing

  it "enqueues and dequeues concurrently tryLock" $ \testDB -> do
    ref <- newIORef []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      mpayload <- runDB testDB tryLockDB
      case mpayload of
        Nothing -> next
        Just x  -> do
          lastCount <- atomicModifyIORef ref $ \xs -> (pValue x : xs, length xs + 1)
          withConnection testDB $ flip dequeue (pId x)
          when (lastCount < 1001) next

    -- Fork a hundred threads and enqueue an index
    forM_ [0 .. 1000 :: Int] $ \i -> forkIO $ void $ withConnection testDB $ flip enqueue $ toJSON i

    let expected = [0 .. 1000 :: Int]

    waitAnyCancel loopThreads
    Just decoded <- mapM (decode . encode) <$> readIORef ref
    sort decoded `shouldBe` sort expected

  it "enqueues and dequeues concurrently lock" $ \testDB -> do
    ref <- newIORef []

    let elementCount = 1000 :: Int

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      x <- withConnection testDB lock
      lastCount <- atomicModifyIORef ref $ \xs -> (pValue x : xs, length xs + 1)
      withConnection testDB $ flip dequeue (pId x)
      when (lastCount < elementCount) next

    -- Fork a hundred threads and enqueue an index
    forM_ [0 .. elementCount - 1] $ \i -> forkIO $ void $ withConnection testDB $
      flip enqueue $ toJSON i

    let expected = [0 .. elementCount - 1]

    waitAnyCancel loopThreads
    Just decoded <- mapM (decode . encode) <$> readIORef ref
    sort decoded `shouldBe` sort expected
