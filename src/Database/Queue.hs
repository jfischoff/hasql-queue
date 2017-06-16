{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
module Database.Queue
  ( PayloadId (..)
  , State (..)
  , Payload (..)
  , enqueueDB
  , tryLockDB
  , unlockDB
  , getCountDB
  , dequeueDB
  , enqueue
  , tryLock
  , lock
  , unlock
  , dequeue
  , getCount
  )where
import           Control.Monad
import           Control.Monad.Catch
import           Control.Monad.IO.Class
import           Data.Aeson
import qualified Data.ByteString                         as BS
import           Data.Function
import           Data.Int
import           Data.Maybe
import           Data.Text                               (Text)
import           Data.Time
import           Data.UUID
import           Database.PostgreSQL.Simple              (Connection, Only (..))
import qualified Database.PostgreSQL.Simple              as Simple
import           Database.PostgreSQL.Simple.FromField
import           Database.PostgreSQL.Simple.FromRow
import           Database.PostgreSQL.Simple.Notification
import           Database.PostgreSQL.Simple.SqlQQ
import           Database.PostgreSQL.Simple.ToField
import           Database.PostgreSQL.Simple.ToRow
import           Database.PostgreSQL.Simple.Transaction
import           Database.PostgreSQL.Transact
import           System.Random
-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
newtype PayloadId = PayloadId { unPayloadId :: UUID }
  deriving (Eq, Show, FromField, ToField)

instance FromRow PayloadId where
  fromRow = fromOnly <$> fromRow

instance ToRow PayloadId where
  toRow = toRow . Only

-- | A 'Payload' can exist in three states in the queue, 'Enqueued', 'Locked'
--   and 'Dequeue'. A 'Payload' starts in the 'Enqueued' state and is 'Locked'
--   so some sort of process can occur with it, usually something in 'IO'.
--   Once the processing is complete, the `Payload' is moved the 'Dequeued'
--   state, which is the terminal state.
data State = Enqueued | Locked | Dequeued
  deriving (Show, Eq, Ord, Enum, Bounded)

instance ToField State where
  toField = toField . \case
    Enqueued -> "enqueued" :: Text
    Locked   -> "locked"
    Dequeued -> "dequeued"

instance FromField State where
  fromField f y = do
     n <- typename f
     if n == "state_t" then case y of
       Nothing -> returnError UnexpectedNull f "state can't be NULL"
       Just y' -> case y' of
         "enqueued" -> return Enqueued
         "locked"   -> return Locked
         "dequeued" -> return Dequeued
         x          -> returnError ConversionFailed f (show x)
     else
       returnError Incompatible f $
         "Expect type name to be state but it was " ++ show n

data Payload = Payload
  { pId      :: PayloadId
  , pValue   :: Value
  , pCreated :: UTCTime
  , pState   :: State
  } deriving (Show, Eq)

instance FromRow Payload where
  fromRow = Payload <$> field <*> field <*> field <*> field
-------------------------------------------------------------------------------
---  DB API
-------------------------------------------------------------------------------
handleUniqueViolation :: MonadCatch m => m a -> m a -> m a
handleUniqueViolation handler act = catch act $ \e ->
  if Simple.sqlState e == "23505" &&
     "duplicate key" `BS.isPrefixOf` Simple.sqlErrorMsg e then
    handler
  else
    throwM e

enqueueDB :: Value -> DB PayloadId
enqueueDB value = handleUniqueViolation (enqueueDB value) $ do
  pid <- liftIO randomIO
  execute [sql| INSERT INTO payloads (id, value)
                VALUES (?, ?);
                NOTIFY enqueue;
          |]
          (pid, value)
  return $ PayloadId pid

tryLockDB :: DB (Maybe Payload)
tryLockDB = listToMaybe <$> query_
    [sql| UPDATE payloads
          SET state='locked'
          WHERE id in
            ( SELECT id
              FROM payloads
              WHERE state='enqueued'
              LIMIT 1
            )
          RETURNING id, value, created, state
    |]

unlockDB :: PayloadId -> DB ()
unlockDB payloadId = void $ execute
  [sql| UPDATE payloads
        SET state='enqueued'
        WHERE id=? AND state='locked'
  |]
  payloadId

getCountDB :: DB Int64
getCountDB = fromOnly . head <$> query_
  [sql| SELECT count(*)
        FROM payloads
        WHERE state='enqueued'
  |]

dequeueDB :: PayloadId -> DB ()
dequeueDB payloadId = void $ execute
  [sql| UPDATE payloads
        SET state='dequeued'
        WHERE id=?
  |]
  payloadId
-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------
enqueue :: Connection -> Value -> IO PayloadId
enqueue conn value = runDBT (enqueueDB value) ReadCommitted conn

tryLock :: Connection -> IO (Maybe Payload)
tryLock = runDBTSerializable tryLockDB

notifyPayload :: Connection -> IO ()
notifyPayload conn = do
  Notification {..} <- getNotification conn
  unless (notificationChannel == "enqueue") $ notifyPayload conn

lock :: Connection -> IO Payload
lock conn = bracket_
  (Simple.execute_ conn "LISTEN enqueue")
  (Simple.execute_ conn "UNLISTEN enqueue")
  $ fix $ \continue -> do
      m <- tryLock conn
      case m of
        Nothing -> do
          notifyPayload conn
          continue
        Just x -> return x

unlock :: Connection -> PayloadId -> IO ()
unlock conn x = runDBTSerializable (unlockDB x) conn

dequeue :: Connection -> PayloadId -> IO ()
dequeue conn x = runDBTSerializable (dequeueDB x) conn

getCount :: Connection -> IO Int64
getCount = runDBT getCountDB ReadCommitted
