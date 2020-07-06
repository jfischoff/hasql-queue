module Hasql.Queue.High.AtMostOnce where
import qualified Hasql.Queue.High.ExactlyOnce as H
import qualified Hasql.Queue.Internal as I
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D

{-|Enqueue a payload.
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
Dequeue a list of payloads
-}
dequeue :: Connection
        -- ^ Connection
        -> D.Value a
        -- ^ Payload decoder
        -> Int
        -- ^ Element count
        -> IO [a]
dequeue conn theDecoder batchCount = I.runThrow (H.dequeue theDecoder batchCount) conn
