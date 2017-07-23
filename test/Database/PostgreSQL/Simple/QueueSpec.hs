{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module Database.PostgreSQL.Simple.QueueSpec (spec, main) where
import           Control.Concurrent
import           Control.Concurrent.Async
import           Control.Monad
import           Data.Aeson
import           Data.Function
import           Data.IORef
import           Data.List
import           Database.PostgreSQL.Simple.Queue
import           Database.PostgreSQL.Simple.Queue.Migrate
import           Test.Hspec                     (Spec, hspec, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.DB

main :: IO ()
main = hspec spec

tableName :: String
tableName = "queue"

spec :: Spec
spec = describeDB (migrate tableName) "Database.Queue" $ do
  itDB "empty locks nothing" $
    tryLockDB tableName `shouldReturn` Nothing

  itDB "empty gives count 0" $
    getCountDB tableName `shouldReturn` 0

  itDB "enqueues/tryLocks/unlocks" $ do
    payloadId <- enqueueDB tableName $ String "Hello"
    getCountDB tableName `shouldReturn` 1

    Just Payload {..} <- tryLockDB tableName
    getCountDB tableName `shouldReturn` 0

    pId `shouldBe` payloadId
    pValue `shouldBe` String "Hello"
    tryLockDB tableName `shouldReturn` Nothing

    unlockDB tableName pId
    getCountDB tableName `shouldReturn` 1

  itDB "tryLocks/dequeues" $ do
    Just Payload {..} <- tryLockDB tableName
    getCountDB tableName `shouldReturn` 0

    dequeueDB tableName pId `shouldReturn` ()
    tryLockDB tableName `shouldReturn` Nothing

  it "enqueues and dequeues concurrently tryLock" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newIORef []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      mpayload <- withPool' $ tryLock tableName
      case mpayload of
        Nothing -> next
        Just Payload {..}  -> do
          lastCount <- atomicModifyIORef ref
                     $ \xs -> (pValue : xs, length xs + 1)
          withPool' $ \c -> dequeue tableName c pId
          when (lastCount < elementCount) next

    -- Fork a hundred threads and enqueue an index
    forM_ [0 .. elementCount - 1] $ \i ->
      forkIO $ void $ withPool' $ \c -> enqueue tableName c $ toJSON i

    waitAnyCancel loopThreads
    Just decoded <- mapM (decode . encode) <$> readIORef ref
    sort decoded `shouldBe` sort expected

  it "enqueues and dequeues concurrently lock" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newIORef []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      Payload {..} <- withPool' $ lock tableName
      lastCount <- atomicModifyIORef ref
                 $ \xs -> (pValue : xs, length xs + 1)
      withPool' $ \c -> dequeue tableName c pId
      when (lastCount < elementCount) next

    -- Fork a hundred threads and enqueue an index
    forM_ [0 .. elementCount - 1] $ \i -> forkIO $ void $ withPool' $ \c ->
      enqueue tableName c $ toJSON i

    waitAnyCancel loopThreads
    Just decoded <- mapM (decode . encode) <$> readIORef ref
    sort decoded `shouldBe` sort expected
