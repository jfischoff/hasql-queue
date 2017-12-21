{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}
module Database.PostgreSQL.Simple.QueueSpec (spec, main) where
import           Control.Concurrent
import           Control.Concurrent.STM
import           Control.Concurrent.Async
import           Control.Monad
import           Data.Aeson
import           Data.Function
import           Data.List
import           Database.PostgreSQL.Simple.Queue
import           Database.PostgreSQL.Simple.Queue.Migrate
import           Database.PostgreSQL.Transact (getConnection)
import           Test.Hspec                     (Spec, hspec, it)
import           Test.Hspec.Expectations.Lifted
import           Test.Hspec.DB
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Data.List.Split
import           Data.Either


main :: IO ()
main = hspec spec

schemaName :: String
schemaName = "complicated_name"

spec :: Spec
spec = describeDB (migrate schemaName) "Database.Queue" $ do

  it "is okay to migrate multiple times" $ \db -> runDB db $ do
      conn <- getConnection
      replicateM_ 2 $ liftIO $ migrate schemaName conn

  itDB "empty locks nothing" $
    (either throwM return =<< (withPayloadDB schemaName 8 return))
      `shouldReturn` Nothing

  itDB "empty gives count 0" $
    getCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withPayloadDB" $ \conn -> do
    runDB conn $ do
      payloadId <- enqueueDB schemaName $ String "Hello"
      getCountDB schemaName `shouldReturn` 1

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId
        pValue `shouldBe` String "Hello"
        )

      -- read committed but still 0. I don't depend on this but I want to see if it
      -- stays like this.
      getCountDB schemaName `shouldReturn` 0

    runDB conn $ getCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withPayloadDB/retries" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB schemaName $ String "Hello"
      getCountDB schemaName `shouldReturn` 1

      xs <- replicateM 7 $ withPayloadDB schemaName 8 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pAttempts `shouldBe` 7
        pValue `shouldBe` String "Hello"
        )

    runDB conn $ getCountDB schemaName `shouldReturn` 0

  it "enqueuesDB/withPayloadDB/timesout" $ \conn -> do
    runDB conn $ do
      void $ enqueueDB schemaName $ String "Hello"
      getCountDB schemaName `shouldReturn` 1

      xs <- replicateM 2 $ withPayloadDB schemaName 1 (\(Payload {..}) ->
          throwM $ userError "not enough tries"
        )

      all isLeft xs `shouldBe` True

    runDB conn $ getCountDB schemaName `shouldReturn` 0

  it "selects the oldest first" $ \conn -> do
    runDB conn $ do
      payloadId0 <- enqueueDB schemaName $ String "Hello"
      liftIO $ threadDelay 1000000

      payloadId1 <- enqueueDB schemaName $ String "Hi"

      getCountDB schemaName `shouldReturn` 2

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId0
        pValue `shouldBe` String "Hello"
        )

      either throwM return =<< withPayloadDB schemaName 8 (\(Payload {..}) -> do
        pId `shouldBe` payloadId1
        pValue `shouldBe` String "Hi"
        )

    runDB conn $ getCountDB schemaName `shouldReturn` 0

  it "enqueues and dequeues concurrently withPayload" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newTVarIO []

    loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
      lastCount <- either throwM return <=< withPayload schemaName c 0 $ \(Payload {..}) -> do
        atomically $ do
          xs <- readTVar ref
          writeTVar ref $ pValue : xs
          return $ length xs + 1

      when (lastCount < elementCount) next

    forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
       forM_ xs $ \i -> enqueue schemaName c $ toJSON i

    waitAnyCancel loopThreads
    xs <- atomically $ readTVar ref
    let Just decoded = mapM (decode . encode) xs
    sort decoded `shouldBe` sort expected

  it "enqueues and dequeues concurrently dequeue" $ \testDB -> do
    let withPool' = withPool testDB
        elementCount = 1000 :: Int
        expected = [0 .. elementCount - 1]

    ref <- newTVarIO []

    loopThreads <- replicateM 35 $ async $ withPool' $ \c -> fix $ \next -> do
      Payload {..} <- dequeue schemaName c
      lastCount <- atomically $ do
        xs <- readTVar ref
        writeTVar ref $ pValue : xs
        return $ length xs + 1

      when (lastCount < elementCount) next

    forM_ (chunksOf (elementCount `div` 11) expected) $ \xs -> forkIO $ void $ withPool' $ \c ->
       forM_ xs $ \i -> enqueue schemaName c $ toJSON i

    waitAnyCancel loopThreads
    xs <- atomically $ readTVar ref
    let Just decoded = mapM (decode . encode) xs
    sort decoded `shouldBe` sort expected
