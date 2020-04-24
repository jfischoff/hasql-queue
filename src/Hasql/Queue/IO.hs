module Hasql.Queue.IO
  ( enqueue
  , withDequeue
  , withDequeueWith
  , failed
  , dequeued
  , I.WithNotifyHandlers (..)
  , I.QueryException (..)
  , I.PayloadId
  ) where

import qualified Hasql.Queue.Session as S
import qualified Hasql.Queue.Internal as I
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Control.Exception
import           Data.Function
import           Data.ByteString (ByteString)

{-|Enqueue a payload into the queue.
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

Attempt to get a payload and process it. If the function passed in throws an exception
return it on the left side of the `Either`. Re-add the payload up to some passed in
maximum. Return `Nothing` is the `payloads` table is empty otherwise the result is an `a`
from the payload ingesting function.

-}
withDequeue :: ByteString
            -> Connection
            -> D.Value a
            -> Int
            -- ^ Retry count
            -> Int
            -- ^ Element count
            -> ([a] -> IO b)
            -> IO b
withDequeue = withDequeueWith @IOError mempty

-- The issue I have is the retry logic is it will retry a certain numbers of times
-- but that doesn't mean that it is retrying the same element each time.
-- It could return 8 times and it could be a different payload.
withDequeueWith :: forall e a b
                 . Exception e
                => I.WithNotifyHandlers
                -> ByteString
                -> Connection
                -> D.Value a
                -> Int
                -- ^ Retry count
                -> Int
                -- ^ Element count
                -> ([a] -> IO b)
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

failed :: Connection -> D.Value a -> Maybe I.PayloadId -> Int -> IO (I.PayloadId, [a])
failed conn decoder mPayload count = I.runThrow (S.failed decoder mPayload count) conn

dequeued :: Connection -> D.Value a -> Maybe I.PayloadId -> Int -> IO (I.PayloadId, [a])
dequeued conn decoder mPayload count = I.runThrow (S.dequeued decoder mPayload count) conn
