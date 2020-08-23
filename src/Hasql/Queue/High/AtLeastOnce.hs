module Hasql.Queue.High.AtLeastOnce where
import qualified Hasql.Queue.High.ExactlyOnce as H
import qualified Hasql.Queue.Internal as I
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Control.Exception
import           Data.Function
import           Data.ByteString (ByteString)

{-|Enqueue a list of payloads.
-}
enqueue :: Connection
        -- ^ Connection
        -> E.Value a
        -- ^ Payload encoder
        -> [a]
        -- ^ List of payloads to enqueue
        -> IO ()
enqueue conn encoder xs = I.runThrow (H.enqueue encoder xs) conn

{-|
Wait for the next payload and process it. If the continuation throws an
exception the payloads are put back in the queue. 'IOError' is caught
and 'withDequeue' will retry up to the retry count. If 'withDequeue' fails
after too many retries the final exception is rethrown. If individual payloads are
are attempted more than the retry count they are set as "failed". See 'failures'
to receive the list of failed payloads.

If the queue is empty 'withDequeue' return 'Nothing'. If there are
any entries 'withDequeue' will wrap the list in 'Just'.
-}
withDequeue :: Connection
            -- ^ Connection
            -> ByteString
            -- ^ Optional filter
            -> D.Value a
            -- ^ Payload decoder
            -> Int
            -- ^ Retry count
            -> Int
            -- ^ Element count
            -> ([a] -> IO b)
            -- ^ Continuation
            -> IO (Maybe b)
withDequeue = withDequeueWith @IOError


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
                => Connection
                -- ^ Connection
                -> ByteString
                -- ^ Optional filter
                -> D.Value a
                -- ^ Payload decoder
                -> Int
                -- ^ Retry count
                -> Int
                -- ^ Element count
                -> ([a] -> IO b)
                -- ^ Continuation
                -> IO (Maybe b)
withDequeueWith conn theFilter decoder retryCount count f = (fix $ \restart i -> do
    try (flip I.runThrow conn $ I.withDequeue theFilter decoder retryCount count f) >>= \case
      Right x -> pure x
      Left (e :: e) ->
        if i < retryCount then
          restart $ i + 1
        else
          throwIO e
  ) 0
