module Hasql.Queue.Low.ExactlyOnce
  ( enqueue
  , withDequeue
  ) where
import qualified Hasql.Queue.High.ExactlyOnce as H
import Control.Exception
import           Data.ByteString (ByteString)
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import Data.Typeable
import           Hasql.Session
import           Hasql.Connection
import qualified Hasql.Queue.Internal as I
import Data.Function
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

data NoRows = NoRows
  deriving (Show, Eq, Typeable)

instance Exception NoRows

dequeueOrRollbackAndThrow :: D.Value a -> Int -> Session [a]
dequeueOrRollbackAndThrow theDecoder dequeueCount = H.dequeue theDecoder dequeueCount >>= \case
  [] -> liftIO $ throwIO NoRows
  xs -> pure xs

withDequeue :: ByteString
            -- ^ Notification channel name. Any valid PostgreSQL identifier
            -> Connection
            -- ^ Connection
            -> D.Value a
            -- ^ Payload decoder
            -> Int
            -- ^ Batch count
            -> (Connection -> Session [a] -> IO b)
            -- ^ Transaction runner
            -> IO b
withDequeue channel conn theDecoder dequeueCount runner = bracket_
  (I.execute conn $ "LISTEN " <> channel)
  (I.execute conn $ "UNLISTEN " <> channel)
  $ fix $ \restart -> do
      try (runner conn (dequeueOrRollbackAndThrow theDecoder dequeueCount)) >>= \case
        Left NoRows -> do
          I.notifyPayload channel conn
          restart
        Right x -> pure x
