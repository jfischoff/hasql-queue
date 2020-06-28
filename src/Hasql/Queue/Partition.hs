module Hasql.Queue.Partition where
import qualified Control.Immortal as I
import Hasql.Queue.Internal
import           Hasql.Connection
import           Control.Exception
import           Control.Concurrent

data PartitionManager = PartitionManager
  { partitionManagerThread :: I.Thread
  }

data PartitionManagerHandlers = PartitionManagerHandlers
  { pmcExceptionLogger      :: SomeException -> IO ()
  , pmcStartLoop            :: IO ()
  , pmcRecreatePartitions   :: Int -> IO ()
  , pmcActualPartitionCount :: Int -> IO ()
  , pmcDropCount            :: Int -> IO ()
  }

instance Semigroup PartitionManagerHandlers where
  x <> y = PartitionManagerHandlers
    { pmcExceptionLogger      = pmcExceptionLogger      x <> pmcExceptionLogger      y
    , pmcStartLoop            = pmcStartLoop            x <> pmcStartLoop            y
    , pmcRecreatePartitions   = pmcRecreatePartitions   x <> pmcRecreatePartitions   y
    , pmcActualPartitionCount = pmcActualPartitionCount x <> pmcActualPartitionCount y
    , pmcDropCount            = pmcDropCount            x <> pmcDropCount            y
    }

instance Monoid PartitionManagerHandlers where
  mempty = PartitionManagerHandlers mempty mempty mempty mempty mempty
  mappend = (<>)

data PartitionManagerConfig = PartitionManagerConfig
  { pmcConnection         :: forall a. ((Connection -> IO a) -> IO a)
  , pmcDelay              :: Double
  , pmcRangeLength        :: Int
  , pmcPartitionCount     :: Int
  , pmcHandlers           :: PartitionManagerHandlers
  }

makeDefaultPartitionManagerConfig
  :: (forall a. ((Connection -> IO a) -> IO a))
  -> PartitionManagerConfig
makeDefaultPartitionManagerConfig pmcConnection =
  let pmcDelay          = 10
      pmcRangeLength    = 100000
      pmcPartitionCount = 2
      pmcHandlers       = mempty
  in PartitionManagerConfig {..}

start :: PartitionManagerConfig -> IO PartitionManager
start PartitionManagerConfig {..} = do
  let PartitionManagerHandlers {..} = pmcHandlers
  -- TODO make sure there are the right number of partitions
  actualPartitionCount <- pmcConnection $ runThrow countPartitions
  pmcActualPartitionCount actualPartitionCount
  let partitionsToCreate = pmcPartitionCount - actualPartitionCount
  pmcRecreatePartitions partitionsToCreate

  pmcConnection (runThrow (createPartitions partitionsToCreate pmcRangeLength))

  fmap PartitionManager $ I.create $ \t -> I.onUnexpectedFinish t (\e -> either pmcExceptionLogger pure e) $ do
    pmcStartLoop
    dropCount <- pmcConnection $ runThrow dropDequeuedPartitions
    pmcDropCount dropCount
    pmcConnection $ runThrow (createPartitions dropCount pmcRangeLength)
    threadDelay $ floor $ pmcDelay * 1000000

stop :: PartitionManager -> IO ()
stop PartitionManager {..} = do
  I.stop partitionManagerThread
  I.wait partitionManagerThread

with :: PartitionManagerConfig -> IO a -> IO a
with config action = bracket (start config) stop $ \_ -> action
