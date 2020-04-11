module Hasql.Queue.IO where

import           Hasql.Queue.Session as S
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Control.Exception

enque :: Connection -> E.Value a -> [a] -> IO [PayloadId]
enque = undefined

enque_ :: Connection -> E.Value a -> [a] -> IO ()
enque_ = undefined

dequeue :: Connection -> D.Value a -> Int -> IO [Payload a]
dequeue = undefined

dequeueValues :: Connection -> D.Value a -> Int -> IO [a]
dequeueValues = undefined

withPayload :: Connection -> D.Value a -> Int -> (Payload a -> IO b) -> IO (Either SomeException b)
withPayload = undefined
