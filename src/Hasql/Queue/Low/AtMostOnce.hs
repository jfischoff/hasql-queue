module Hasql.Queue.Low.AtMostOnce where
import qualified Hasql.Queue.Low.ExactlyOnce as E
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Text(Text)
import qualified Hasql.Queue.Internal as I


{-|Enqueue a payload.
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
Dequeue a list of payloads.
-}
dequeue :: Text
        -- ^ Notification channel name. Any valid PostgreSQL identifier
        -> Connection
        -- ^ Connection
        -> D.Value a
        -- ^ Payload decoder
        -> Int
        -- ^ Element count
        -> IO [a]
dequeue channel conn theDecoder batchCount =
  E.withDequeue channel conn theDecoder batchCount id
