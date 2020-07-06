{-|
A high throughput 'Session' based API for a PostgreSQL backed queue.
-}
module Hasql.Queue.Low.AtLeastOnce
  ( enqueue
  , withDequeue
  -- ** Listing API
  , I.PayloadId
  , failures
  , delete
  -- ** Advanced API
  , withDequeueWith
  , I.WithNotifyHandlers (..)
  ) where

import qualified Hasql.Queue.Low.ExactlyOnce as E
import qualified Hasql.Queue.Internal as I
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Control.Exception
import           Data.Function
import           Data.Text (Text)
import           Control.Monad.IO.Class

{-|Enqueue a list of payloads.
-}
enqueue :: Text
        -- ^ Notification channel name. Any valid PostgreSQL identifier
        -> Connection
        -- ^ Connection
        -> E.Value a
        -- ^ Payload encoder
        -> [a]
        -- ^ List of payloads to enqueue
        -> IO ()
enqueue channel conn encoder xs = I.runThrow (E.enqueue channel encoder xs) conn

{-|
Wait for the next payload and process it. If the continuation throws an
exception the payloads are put back in the queue. 'IOError' is caught
and 'withDequeue' will retry up to the retry count. If 'withDequeue' fails
after too many retries the final exception is rethrown. If individual payloads are
are attempted more than the retry count they are set as "failed". See 'failures'
to receive the list of failed payloads.

If the queue is empty 'withDequeue' will block until it recieves a notification
from the PostgreSQL server.
-}
withDequeue :: Text
            -- ^ Notification channel name. Any valid PostgreSQL identifier
            -> Connection
            -- ^ Connection
            -> D.Value a
            -- ^ Payload decoder
            -> Int
            -- ^ Retry count
            -> Int
            -- ^ Element count
            -> ([a] -> IO b)
            -- ^ Continuation
            -> IO b
withDequeue = withDequeueWith @IOError mempty

{-|
Retrieve the payloads that have entered a failed state. See 'withDequeue' for how that
occurs. The function returns a list of values and an id. The id is used the starting
place for the next batch of values. If 'Nothing' is passed the list starts at the
beginning.
-}
failures :: Connection
         -> D.Value a
         -- ^ Payload decoder
         -> Maybe I.PayloadId
         -- ^ Starting position of payloads. Pass 'Nothing' to
         --   start at the beginning
         -> Int
         -- ^ Count
         -> IO [(I.PayloadId, a)]
failures conn decoder mPayload count = I.runThrow (I.failures decoder mPayload count) conn

{-|
Permantently remove a failed payload.
-}
delete :: Connection
       -> [I.PayloadId]
       -> IO ()
delete conn xs = I.runThrow (I.delete xs) conn

{-|
A more general configurable version of 'withDequeue'. Unlike 'withDequeue' one
can specify the exception that causes a retry. Additionally event
handlers can be specified to observe the internal behavior of the
retry loop.
-}
withDequeueWith :: forall e a b
                 . Exception e
                => I.WithNotifyHandlers
                -- ^ Event handlers for events that occur as 'withDequeWith' loops
                -> Text
                -- ^ Notification channel name. Any valid PostgreSQL identifier
                -> Connection
                -- ^ Connection
                -> D.Value a
                -- ^ Payload decoder
                -> Int
                -- ^ Retry count
                -> Int
                -- ^ Element count
                -> ([a] -> IO b)
                -- ^ Continuation
                -> IO b
withDequeueWith withNotifyHandlers channel conn decoder retryCount count f = (fix $ \restart i -> do
    let action = I.withDequeue decoder retryCount count f >>= \case
          Nothing -> liftIO $ throwIO I.NoRows
          Just x  -> pure x

    try (I.withNotifyWith withNotifyHandlers channel conn action) >>= \case
      Right x -> pure x
      Left (e :: e) ->
        if i < retryCount then
          restart $ i + 1
        else
          throwIO e
  ) 0
