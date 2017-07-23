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

schemaName :: String
schemaName = "queue"

spec :: Spec
spec = describeDB (migrate schemaName) "Database.Queue" $ do
  itDB "empty locks nothing" $
    tryLockDB schemaName `shouldReturn` Nothing

  itDB "empty gives count 0" $
    getCountDB schemaName `shouldReturn` 0

  itDB "enqueues/tryLocks/unlocks" $ do
    payloadId <- enqueueDB schemaName $ String "Hello"
    getCountDB schemaName `shouldReturn` 1

    Just Payload {..} <- tryLockDB schemaName
    getCountDB schemaName `shouldReturn` 0

    pId `shouldBe` payloadId
    pValue `shouldBe` String "Hello"
    tryLockDB schemaName `shouldReturn` Nothing

    unlockDB schemaName pId
    getCountDB schemaName `shouldReturn` 1

  itDB "tryLocks/dequeues" $ do
    Just Payload {..} <- tryLockDB schemaName
    getCountDB schemaName `shouldReturn` 0

    dequeueDB schemaName pId `shouldReturn` ()
    tryLockDB schemaName `shouldReturn` Nothing

  it "enqueues and dequeues concurrently tryLock" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newIORef []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      mpayload <- withPool' $ tryLock schemaName
      case mpayload of
        Nothing -> next
        Just Payload {..}  -> do
          lastCount <- atomicModifyIORef ref
                     $ \xs -> (pValue : xs, length xs + 1)
          withPool' $ \c -> dequeue schemaName c pId
          when (lastCount < elementCount) next

    -- Fork a hundred threads and enqueue an index
    forM_ [0 .. elementCount - 1] $ \i ->
      forkIO $ void $ withPool' $ \c -> enqueue schemaName c $ toJSON i

    waitAnyCancel loopThreads
    Just decoded <- mapM (decode . encode) <$> readIORef ref
    sort decoded `shouldBe` sort expected

  it "enqueues and dequeues concurrently lock" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newIORef []

    loopThreads <- replicateM 10 $ async $ fix $ \next -> do
      Payload {..} <- withPool' $ lock schemaName
      lastCount <- atomicModifyIORef ref
                 $ \xs -> (pValue : xs, length xs + 1)
      withPool' $ \c -> dequeue schemaName c pId
      when (lastCount < elementCount) next

    -- Fork a hundred threads and enqueue an index
    forM_ [0 .. elementCount - 1] $ \i -> forkIO $ void $ withPool' $ \c ->
      enqueue schemaName c $ toJSON i

    waitAnyCancel loopThreads
    Just decoded <- mapM (decode . encode) <$> readIORef ref
    sort decoded `shouldBe` sort expected
