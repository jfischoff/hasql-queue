module Hasql.Queue.High.AtMostOnce where
import qualified Hasql.Queue.High.ExactlyOnce as H
import qualified Hasql.Queue.Internal as I
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Data.Text (Text)

enqueue :: Text
        -> Connection
        -- ^ Connection
        -> E.Value a
        -- ^ Payload encoder
        -> [a]
        -- ^ List of payloads to enqueue
        -> IO ()
enqueue schemaName conn encoder xs = I.runThrow (I.withSchema schemaName $ H.enqueue encoder xs) conn

dequeue :: Text
        -> Connection
        -- ^ Connection
        -> D.Value a
        -- ^ Payload decoder
        -> Int
        -- ^ Element count
        -> IO [a]
dequeue schemaName conn theDecoder batchCount = I.runThrow (I.withSchema schemaName $ H.dequeue theDecoder batchCount) conn
