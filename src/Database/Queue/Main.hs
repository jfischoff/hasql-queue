{-# LANGUAGE RecordWildCards, LambdaCase, DeriveDataTypeable, NamedFieldPuns, FlexibleContexts #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.Queue.Main where
import Database.Queue
import Control.Monad
import qualified Database.PostgreSQL.Simple.Options as PostgreSQL
import Database.PostgreSQL.Simple
import Data.Int
import Data.Pool
import Data.Monoid
import Options.Generic
import Data.Typeable
import System.Exit
import Data.Default
import Control.Monad.Trans.Control
import Control.Monad.IO.Class
import Control.Concurrent.Async.Lifted

data PartialOptions = PartialOptions
  { threadCount :: Last Int
  , dbOptions   :: PostgreSQL.PartialOptions
  } deriving (Show, Eq, Typeable)

instance Monoid PartialOptions where
  mempty = PartialOptions mempty mempty
  mappend x y =
    PartialOptions
      (threadCount x <> threadCount y)
      (dbOptions   x <> dbOptions   y)

instance Default PartialOptions where
  def = PartialOptions (return 1) def

instance ParseRecord PartialOptions where
  parseRecord
     =  PartialOptions
    <$> parseRecord
    <*> parseRecord

data Options = Options
  { oThreadCount :: Int
  , oDBOptions   :: PostgreSQL.Options
  } deriving (Show, Eq)

completeOptions :: PartialOptions -> Either [String] Options
completeOptions = \case
  PartialOptions { threadCount = Last (Just oThreadCount), dbOptions } ->
    Options oThreadCount <$> PostgreSQL.completeOptions dbOptions
  _ -> Left ["Missing threadCount"]

defaultMain :: (MonadIO m, MonadBaseControl IO m) => Text -> (Payload -> Int64 -> m ()) -> m ()
defaultMain name f = do
  poptions <- liftIO $ getRecord name
  options  <- liftIO
            $ either (\err -> putStrLn (unlines err) >> exitWith (ExitFailure 64))
                     return
                     $ completeOptions $ def <> poptions
  run f options

run :: forall m. (MonadIO m, MonadBaseControl IO m)
    => (Payload -> Int64 -> m ())
    -> Options -> m ()
run f Options {..} = do
  connectionPool <- liftIO $ createPool
    (PostgreSQL.run oDBOptions) close 1 60 (max 1 $ oThreadCount `div` 2)

  threads :: [Async (StM m ())] <- replicateM oThreadCount $ async $ void $
    forever $ do
      payload <- liftIO $ withResource connectionPool lock
      count   <- liftIO $ withResource connectionPool getCount
      f payload count
      liftIO $ withResource connectionPool $ flip dequeue (pId payload)

  _ :: (Async (StM m ()), ()) <- waitAnyCancel threads
  return ()