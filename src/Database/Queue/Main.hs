{-# LANGUAGE DeriveDataTypeable    #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns        #-}
{-# LANGUAGE RecordWildCards       #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-|
 This module provides a simple way to create executables for consuming queue
 payloads. It provides a `defaultMain' function which takes in a executable
 name and processing function. It returns a main function which will parse
 database options on from the command line and spawn a specified number of
 worker threads.

 Here is a simple example that logs out queue payloads

 @
  defaultMain "queue-logger" $ \payload count -> do
    print payload
    print count
 @

 The worker threads do not attempt to handle exceptions. If an exception is
 thrown on any threads, all threads are cancel and 'defaultMain' returns. The
 assumption is the queue executable will get run by a process watcher that
 can log failures.

 For a more complicated example, see the code for the async-email-example
 documented in 'Database.Queue.Examples.EmailQueue.EmailQueue'.
-}
module Database.Queue.Main
  ( -- * Options
    PartialOptions
  , Options
  , completeOptions
    -- * Entry Points
  , defaultMain
  , run
  ) where
import           Control.Concurrent.Async.Lifted
import           Control.Monad
import           Control.Monad.IO.Class
import           Control.Monad.Trans.Control
import           Data.Default
import           Data.Int
import           Data.Monoid
import           Data.Pool
import           Data.Typeable
import           Database.PostgreSQL.Simple
import qualified Database.PostgreSQL.Simple.Options as PostgreSQL
import           Database.Queue
import           Options.Generic
import           System.Exit

{-| The 'PartialOptions' provide a 'Monoid' for combining options used by
    'defaultMain'. The following command line arguments are used to
    construct a 'PartialOptions'

 @
   --thread-count INT

   Either a connection string
   --connectString ARG
   or individual db properties are provided
   --host STRING
   --port INT
   --user STRING
   --password STRING
   --database STRING
 @
-}
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

-- | The default 'threadCount' is 1.
--   The default db options are specified in
--   'Database.PostgreSQL.Simple.Options.PartialOptions'
instance Default PartialOptions where
  def = PartialOptions (return 1) def

instance ParseRecord PartialOptions where
  parseRecord
     =  PartialOptions
    <$> parseFields Nothing (Just "thread-count")
    <*> parseRecord

-- | Final Options used by 'run'.
data Options = Options
  { oThreadCount :: Int
  , oDBOptions   :: PostgreSQL.Options
  } deriving (Show, Eq)

-- | Convert a 'PartialOptions' to a final 'Options'
completeOptions :: PartialOptions -> Either [String] Options
completeOptions = \case
  PartialOptions { threadCount = Last (Just oThreadCount), dbOptions } ->
    Options oThreadCount <$> PostgreSQL.completeOptions dbOptions
  _ -> Left ["Missing threadCount"]

{-| This function is a helper for creating queue consumer executables.
 It takes in a executable
 name and processing function. It returns a main function which will parse
 database options on from the command line and spawn a specified number of
 worker threads. See 'PartialOptions' for command line documentation.

 Here is a simple example that logs out queue payloads

 @
  defaultMain "queue-logger" $ \payload count -> do
    print payload
    print count
 @

 The worker threads do not attempt to handle exceptions. If an exception is
 thrown on any threads, all threads are cancel and 'defaultMain' returns. The
 assumption is the queue executable will get run by a process watcher that
 can log failures.

 For a more complicated example, see the code for the async-email-example
 documented in 'Database.Queue.Examples.EmailQueue.EmailQueue'.

-}
defaultMain :: (MonadIO m, MonadBaseControl IO m)
            => Text
            -- ^ Executable name. Used when command line help
            --   is printed
            -> (Payload -> Int64 -> m ())
            -- ^ Processing function. Takes a 'Payload' to process
            --   and the current number of 'Enqueued' 'Payload's
            --   remaining.
            -> m ()
defaultMain name f = do
  poptions <- liftIO $ getRecord name
  options  <- liftIO
            $ either (\err -> putStrLn (unlines err) >> exitWith (ExitFailure 64))
                     return
                     $ completeOptions $ def <> poptions
  run f options

-- | 'run' is called by 'defaultMain' after command line argument parsing
--   is performed. Useful is wants to embed consumer threads inside a
--   larger application.
run :: forall m. (MonadIO m, MonadBaseControl IO m)
    => (Payload -> Int64 -> m ())
    -- ^ Processing function. Takes a 'Payload' to process
    --   and the current number of 'Enqueued' 'Payload's
    --   remaining.
    -> Options
    -- ^ Options for thread creation and db connections.
    -> m ()
run f Options {..} = do
  -- Creates a pool with half as many connections as threads. Should
  -- probably make the number of connection configurable.
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
