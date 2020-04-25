{-|
IO based API for a PostgreSQL backed queue. The API utilizes PostgreSQL
notifications.
-}
module Hasql.Queue.IO
  ( enqueue
  , withDequeue
  -- ** Listing API
  , I.PayloadId
  , failed
  , dequeued
  -- ** Advanced API
  , withDequeueWith
  , I.WithNotifyHandlers (..)
  ) where

import qualified Hasql.Queue.Session as S
import qualified Hasql.Queue.Internal as I
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Control.Exception
import           Data.Function
import           Data.ByteString (ByteString)

{-|Enqueue a payload.
-}
enqueue :: ByteString
        -- ^ Notification channel name. Any valid PostgreSQL identifier
        -> Connection
        -- ^ Connection
        -> E.Value a
        -- ^ Payload encoder
        -> [a]
        -- ^ List of payloads to enqueue
        -> IO ()
enqueue channel conn encoder xs = I.runThrow (S.enqueueNotify channel encoder xs) conn

{-|
Wait for the next payload and process it. If the continuation throws an
exception the payloads are put back in the queue. An 'IOError' is caught
and 'withDequeue' will retry up to the retry count. If 'withDequeue' fails
after too many retries the final exception is rethrown. If individual payloads are
are attempted more than the retry count they are set as "failed". See 'failed'
to receive the list of failed payloads.

If the queue is empty 'withDequeue' will block until it recieves a notification
from the PostgreSQL server.
-}
withDequeue :: ByteString
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
failed :: Connection -> D.Value a -> Maybe I.PayloadId -> Int -> IO (I.PayloadId, [a])
failed conn decoder mPayload count = I.runThrow (S.failed decoder mPayload count) conn

{-|
Retrieve the payloads that have been successfully dequeued.
The function returns a list of values and an id. The id is used the starting
place for the next batch of values. If 'Nothing' is passed the list starts at the
beginning.
-}
dequeued :: Connection -> D.Value a -> Maybe I.PayloadId -> Int -> IO (I.PayloadId, [a])
dequeued conn decoder mPayload count = I.runThrow (S.dequeued decoder mPayload count) conn

{-|
A more general configurable version of 'withDequeue'. Unlike 'withDequeue' one
can specify the
-}
withDequeueWith :: forall e a b
                 . Exception e
                => I.WithNotifyHandlers
                -- Event handlers for events that occur as 'withDequeWith' loops
                -> ByteString
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
    try (I.withNotifyWith withNotifyHandlers channel conn (I.withDequeue decoder retryCount count f) id) >>= \case
      Right x -> pure x
      Left (e :: e) ->
        if i < retryCount then
          restart $ i + 1
        else
          throwIO e
  ) 0
