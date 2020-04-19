module Hasql.Queue.IO
  ( notifyPayload
  , withNotify
  , enqueue
  , dequeue
  , dequeueWith
  , withPayload
  , withPayloadWith
  , withNotifyWith
  , WithNotifyHandlers (..)
  , QueryException (..)
  ) where

import qualified Hasql.Queue.Session as S
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

-- | To aid in observability and white box testing
data WithNotifyHandlers = WithNotifyHandlers
  { withNotifyHandlersAfterAction :: IO ()
  , withNotifyHandlersBefore      :: IO ()
  }

instance Semigroup WithNotifyHandlers where
  x <> y = WithNotifyHandlers
    { withNotifyHandlersAfterAction = withNotifyHandlersAfterAction x <> withNotifyHandlersAfterAction y
    , withNotifyHandlersBefore      = withNotifyHandlersBefore      x <> withNotifyHandlersBefore      y
    }

instance Monoid WithNotifyHandlers where
  mempty = WithNotifyHandlers mempty mempty

withNotifyWith :: WithNotifyHandlers -> Connection -> Session a -> (a -> Maybe b) -> IO b
withNotifyWith WithNotifyHandlers {..} conn action theCast = bracket_
  (execute conn $ "LISTEN " <> notifyName)
  (execute conn $ "UNLISTEN " <> notifyName)
  $ fix $ \restart -> do
    x <- runThrow (transaction action) conn
    withNotifyHandlersAfterAction
    case theCast x of
      Nothing -> do
        -- TODO record the time here
        withNotifyHandlersBefore
        notifyPayload conn
        restart
      Just xs -> pure xs

withNotify :: Connection -> Session a -> (a -> Maybe b) -> IO b
withNotify = withNotifyWith mempty

enqueue :: Connection -> E.Value a -> [a] -> IO ()
enqueue conn encoder xs = runThrow (S.enqueueNotify encoder xs) conn

nonEmpty :: [a] -> Maybe [a]
nonEmpty = \case
  [] -> Nothing
  ys -> pure ys

-- TODO Handle >= 0
-- Should this return nonEmpty? <- No. That would be annoying to import.
dequeue :: Connection -> D.Value a -> Int -> IO [a]
dequeue = dequeueWith mempty

dequeueWith :: WithNotifyHandlers -> Connection -> D.Value a -> Int -> IO [a]
dequeueWith withNotifyHandlers conn decoder count
  | count < 1 = pure []
  | otherwise = withNotifyWith withNotifyHandlers conn (S.dequeue decoder count) nonEmpty

{-|

Attempt to get a payload and process it. If the function passed in throws an exception
return it on the left side of the `Either`. Re-add the payload up to some passed in
maximum. Return `Nothing` is the `payloads` table is empty otherwise the result is an `a`
from the payload ingesting function.

-}
withPayload :: forall a b. Connection
            -> D.Value a
            -> Int
            -- ^ retry count
            -> (a -> IO b)
            -> IO b
withPayload = withPayloadWith @IOException mempty

-- The issue I have is the retry logic is it will retry a certain numbers of times
-- but that doesn't mean that it is retrying the same element each time.
-- It could return 8 times and it could be a different payload.
withPayloadWith :: forall e a b
                 . Exception e
                => WithNotifyHandlers
                -> Connection
                -> D.Value a
                -> Int
                -- ^ retry count
                -> (a -> IO b)
                -> IO b
withPayloadWith withNotifyHandlers conn decoder retryCount f = (fix $ \restart i -> do
    try (withNotifyWith withNotifyHandlers conn (S.withPayload decoder retryCount f) id) >>= \case
      Right x -> pure x
      Left (e :: e) ->
        if i < retryCount then
          restart $ i + 1
        else
          throwIO e
  ) 0
