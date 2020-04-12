module Hasql.Queue.IO where

import           Hasql.Queue.Session as S
import           Hasql.Connection
import           Hasql.Session
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Control.Exception
import           Data.ByteString (ByteString)
import           Data.Typeable
import           Data.Function
import           Data.String
import qualified Hasql.Queue.Session as S
import           Hasql.Notification
import           Control.Monad

-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
-- TODO remove
newtype QueryException = QueryException QueryError
  deriving (Eq, Show, Typeable)

queryErrorToSomeException :: QueryError -> SomeException
queryErrorToSomeException = toException . QueryException

instance Exception QueryException

execute :: Connection -> ByteString -> IO ()
execute conn theSql = either (throwIO . userError . show) pure =<< run (sql theSql) conn

enque :: Connection -> E.Value a -> [a] -> IO [PayloadId]
enque = undefined

enque_ :: Connection -> E.Value a -> [a] -> IO ()
enque_ = undefined

dequeue :: Connection -> D.Value a -> Int -> IO [Payload a]
dequeue = undefined

dequeueValues :: Connection -> D.Value a -> Int -> IO [a]
dequeueValues = undefined

transaction :: Session a -> Session a
transaction inner = do
  sql "BEGIN"
  r <- inner
  sql "COMMIT"
  pure r

joinLeft :: Either a (Either a b) -> Either a b
joinLeft = \case
  Left x -> Left x
  Right x -> case x of
    Left y -> Left y
    Right y -> Right y

notifyName :: IsString s => s
notifyName = fromString "postgresql_simple_enqueue"

-- Block until a payload notification is fired. Fired during insertion.
notifyPayload :: Connection -> IO ()
notifyPayload conn = do
  Notification {..} <- either throwIO pure =<< getNotification conn
  unless (notificationChannel == notifyName) $ notifyPayload conn

{-|

Attempt to get a payload and process it. If the function passed in throws an exception
return it on the left side of the `Either`. Re-add the payload up to some passed in
maximum. Return `Nothing` is the `payloads` table is empty otherwise the result is an `a`
from the payload ingesting function.

-}
withPayload :: Connection
            -> D.Value a
            -> Int
            -- ^ retry count
            -> (Payload a -> IO b)
            -> IO (Either SomeException b)
withPayload conn decoder retryCount f = bracket_
  (execute conn $ "LISTEN " <> notifyName)
  (execute conn $ "UNLISTEN " <> notifyName)
  $ fix
  $ \continue ->
    run (transaction $ S.withPayload decoder retryCount f) conn >>= \case
      Left a -> throwIO $ QueryException a
      Right a -> case a of
        Left x -> return $ Left x
        Right Nothing -> do
          notifyPayload conn
          continue
        Right (Just x) -> return $ Right x
