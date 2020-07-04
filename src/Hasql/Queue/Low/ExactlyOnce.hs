module Hasql.Queue.Low.ExactlyOnce
  ( enqueue
  , withDequeue
  , withDequeueWith
  ) where
import qualified Hasql.Queue.High.ExactlyOnce as H
import Control.Exception
import           Data.ByteString (ByteString)
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Hasql.Connection
import qualified Hasql.Queue.Internal as I
import Control.Monad.IO.Class

{-|Enqueue a payload send a notification on the
specified channel.
-}
enqueue :: ByteString
        -- ^ Notification channel name. Any valid PostgreSQL identifier
        -> E.Value a
        -- ^ Payload encoder
        -> [a]
        -- ^ List of payloads to enqueue
        -> Session ()
enqueue channel theEncoder values = do
  H.enqueue theEncoder values
  sql $ "NOTIFY " <> channel

dequeueOrRollbackAndThrow :: D.Value a -> Int -> Session [a]
dequeueOrRollbackAndThrow theDecoder dequeueCount = H.dequeue theDecoder dequeueCount >>= \case
  [] -> liftIO $ throwIO I.NoRows
  xs -> pure xs

withDequeue :: ByteString
            -- ^ Notification channel name. Any valid PostgreSQL identifier
            -> Connection
            -- ^ Connection
            -> D.Value a
            -- ^ Payload decoder
            -> Int
            -- ^ Batch count
            -> (Session [a] -> Session b)
            -- ^ Transaction runner
            -> IO b
withDequeue = withDequeueWith mempty

withDequeueWith :: I.WithNotifyHandlers
                -- ^ Event handlers for events that occur as 'withDequeWith' loops
                -> ByteString
                -- ^ Notification channel name. Any valid PostgreSQL identifier
                -> Connection
                -- ^ Connection
                -> D.Value a
                -- ^ Payload decoder
                -> Int
                -- ^ Batch count
                -> (Session [a] -> Session b)
                -- ^ Transaction runner
                -> IO b
withDequeueWith withNotifyHandlers channel conn theDecoder dequeueCount runner
  = I.withNotifyWith withNotifyHandlers channel conn
  $ runner (dequeueOrRollbackAndThrow theDecoder dequeueCount)
