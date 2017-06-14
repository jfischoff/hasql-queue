{-# LANGUAGE OverloadedStrings, GeneralizedNewtypeDeriving, ScopedTypeVariables, LambdaCase #-}
{-# LANGUAGE QuasiQuotes, RecordWildCards, FlexibleContexts #-}
module Database.Queue where
import Database.PostgreSQL.Simple (Connection, Only (..), SqlError (..))
import qualified Database.PostgreSQL.Simple as Simple
import Database.PostgreSQL.Transact
import Data.Aeson
import Data.Time
import Database.PostgreSQL.Simple.Transaction
import Database.PostgreSQL.Simple.FromRow
import Database.PostgreSQL.Simple.ToRow
import Database.PostgreSQL.Simple.FromField
import Database.PostgreSQL.Simple.ToField
import Data.UUID
import Data.Foldable
import Control.Monad
import Data.Maybe
import Control.Monad.IO.Class
import System.Random
import Data.Text (Text)
import qualified Data.Text as T
import Database.PostgreSQL.Simple.SqlQQ
import Database.PostgreSQL.Simple.Notification
import Data.Function
import Data.Int
import Control.Exception as E
import Control.Monad.Trans.Control
import Control.Monad.Trans.Reader

newtype PayloadId = PayloadId { unPayloadId :: UUID }
  deriving (Eq, Show, FromField, ToField)

instance FromRow PayloadId where
  fromRow = fromOnly <$> fromRow

instance ToRow PayloadId where
  toRow = toRow . Only

data State = Enqueued | Locked | Dequeued
  deriving (Show, Eq, Ord, Enum, Bounded)

instance ToField State where
  toField = toField . \case
    Enqueued -> "enqueued" :: Text
    Locked   -> "locked"
    Dequeued -> "dequeued"

instance FromField State where
  fromField f y = do
     name <- typename f
     if name == "state" then case y of
       Nothing -> returnError UnexpectedNull f "status can't be NULL"
       Just y' -> case y' of
         "enqueued" -> return Enqueued
         "locked"   -> return Locked
         "dequeued" -> return Dequeued
         x -> returnError ConversionFailed f (show x)
     else
       returnError Incompatible f $
         "Expect type name to be status but it was " ++ show name

data Payload = Payload
  { pId      :: PayloadId
  , pValue   :: Value
  , pCreated :: UTCTime
  , pState   :: State
  } deriving (Show, Eq)

instance FromRow Payload where
  fromRow = Payload <$> field <*> field <*> field <*> field

enqueueDB :: Value -> DB PayloadId
enqueueDB value = do
  pid <- liftIO randomIO
  execute [sql| INSERT INTO payloads (id, value)
                VALUES (?, ?);
                NOTIFY enqueue;
          |]
          (pid, value)
  return $ PayloadId pid

enqueue :: Connection -> Value -> IO PayloadId
enqueue conn value = runDBT (enqueueDB value) ReadCommitted conn

unlock = undefined -- use for testing lock and unlock until everything is done

tryLockDB :: DB (Maybe Payload)
tryLockDB = do
  next <- listToMaybe <$> query_
    [sql| SELECT id, value, created, status
          FROM payloads
          WHERE status='enqueued'
          LIMIT 1
    |]
  for_ next $ \p -> void $ execute
    [sql| UPDATE payloads
          SET status='locked'
          WHERE id=?
    |] $
    pId p
  return next

-- This should also return the count
tryLock :: Connection -> IO (Maybe Payload)
tryLock = runDBTSerializable tryLockDB

notifyPayload :: Connection -> IO ()
notifyPayload conn = do
  Notification {..} <- getNotification conn
  if notificationChannel == "enqueue" then
    return ()
  else
    notifyPayload conn

lock :: Connection -> IO Payload
lock conn = do
  Simple.execute_ conn "LISTEN enqueue"
  fix $ \next -> do
    m <- tryLock conn
    case m of
      Nothing -> do
        notifyPayload conn
        next
      Just x -> do
        Simple.execute_ conn "UNLISTEN enqueue"
        return x

dequeue :: PayloadId -> DB ()
dequeue queueId
  = void
  $ execute
      [sql| UPDATE payloads
            SET status='dequeued'
            WHERE id=?
      |]
      queueId

getCountDB :: DB Int64
getCountDB = fromOnly . head <$> query_ [sql|
    SELECT count(*)
    FROM payloads
    WHERE status='enqueued'
  |]

getCount :: Connection -> IO Int64
getCount = runDBTSerializable getCountDB