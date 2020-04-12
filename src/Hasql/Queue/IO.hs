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
import           Hasql.Notification
import           Control.Monad

-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
-- TODO remove
newtype QueryException = QueryException QueryError
  deriving (Eq, Show, Typeable)

instance Exception QueryException

runThrow :: Session a -> Connection -> IO a
runThrow sess conn = either (throwIO . QueryException) pure =<< run sess conn

execute :: Connection -> ByteString -> IO ()
execute conn theSql = runThrow (sql theSql) conn

notifyName :: IsString s => s
notifyName = fromString "postgresql_simple_enqueue"

-- Block until a payload notification is fired. Fired during insertion.
notifyPayload :: Connection -> IO ()
notifyPayload conn = do
  Notification {..} <- either throwIO pure =<< getNotification conn
  unless (notificationChannel == notifyName) $ notifyPayload conn

transaction :: Session a -> Session a
transaction inner = do
  sql "BEGIN"
  r <- inner
  sql "COMMIT"
  pure r

withNotify :: Connection -> Session a -> (a -> Maybe b) -> IO b
withNotify conn action theCast = bracket_
  (execute conn $ "LISTEN " <> notifyName)
  (execute conn $ "UNLISTEN " <> notifyName)
  $ fix
  $ \continue -> do
      x <- runThrow (transaction action) conn
      case theCast x of
        Nothing -> do
          notifyPayload conn
          continue
        Just xs -> pure xs

enqueue :: Connection -> E.Value a -> [a] -> IO [PayloadId]
enqueue conn encoder xs = runThrow (S.enqueueNotify encoder xs) conn

enqueue_ :: Connection -> E.Value a -> [a] -> IO ()
enqueue_ conn encoder xs = runThrow (S.enqueueNotify_ encoder xs) conn

nonEmpty :: [a] -> Maybe [a]
nonEmpty = \case
  [] -> Nothing
  ys -> pure ys

-- TODO Handle >= 0
-- Should this return nonEmpty?
dequeue :: Connection -> D.Value a -> Int -> IO [Payload a]
dequeue conn decoder count = withNotify conn (S.dequeue decoder count) nonEmpty

dequeueValues :: Connection -> D.Value a -> Int -> IO [a]
dequeueValues conn decoder count = withNotify conn (S.dequeueValues decoder count) nonEmpty

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
withPayload conn decoder retryCount f = withNotify conn (S.withPayload decoder retryCount f) sequenceA
