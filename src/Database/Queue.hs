{-| This module implements a persistent durable queue for efficently processing
    arbitrary payloads which can be represented as JSON.

    Typically a producer would enqueue a new payload as part of larger database
    transaction

 @
   createAccount userRecord = do
      'runDBTSerializable' $ do
         createUserDB userRecord
         'enqueueDB' $ makeVerificationEmail userRecord
 @

In another thread or process, the consumer would drain the queue.

 @
    forever $ do
      -- Attempt get a payload or block until one is available
      payload <- 'lock' conn

      -- Perform application specifc parsing of the payload value
      case fromJSON $ 'pValue' payload of
        Success x -> sendEmail x -- Perform application specific processing
        Error err -> logErr err

      -- Remove the payload from future processing
      'dequeue' conn $ 'pId' payload
 @

-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
module Database.Queue
  ( -- * Types
    PayloadId (..)
  , State (..)
  , Payload (..)
  -- * DB API
  , enqueueDB
  , tryLockDB
  , unlockDB
  , dequeueDB
  , getCountDB
  -- * IO API
  , enqueue
  , tryLock
  , lock
  , unlock
  , dequeue
  , getCount
  ) where
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
--   and 'Dequeued'. A 'Payload' starts in the 'Enqueued' state and is 'Locked'
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

-- Converting from enumerations is annoying :(
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

-- The fundemental record stored in the queue. The queue is a single table
-- and each row consists of a 'Payload'
data Payload = Payload
  { pId         :: PayloadId
  , pValue      :: Value
  -- ^ The JSON value of a payload
  , pState      :: State
  , pCreatedAt  :: UTCTime
  , pModifiedAt :: UTCTime
  } deriving (Show, Eq)

instance FromRow Payload where
  fromRow = Payload <$> field <*> field <*> field <*> field <*> field
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

{-| Enqueue a new JSON value into the queue. This particularly function
    can be composed as part of a larger database transaction. For instance,
    a single transaction could create a user and enqueue a email message.
-}
enqueueDB :: Value -> DB PayloadId
enqueueDB value = handleUniqueViolation (enqueueDB value) $ do
  pid <- liftIO randomIO
  execute [sql| INSERT INTO payloads (id, value)
                VALUES (?, ?);
                NOTIFY enqueue;
          |]
          (pid, value)
  return $ PayloadId pid

{-| Return a the oldest 'Payload' in the 'Enqueued' state, or 'Nothing'
    if there are no payloads. This function is not necessarily useful by
    itself, since there are not many use cases where it needs to be combined
    with other transactions. See 'tryLock' the IO API version, or for a
    blocking version utilizing PostgreSQL's NOTIFY and LISTEN, see 'lock'
-}
tryLockDB :: DB (Maybe Payload)
tryLockDB = listToMaybe <$> query_
  [sql| UPDATE payloads
        SET state='locked'
        WHERE id in
          ( SELECT id
            FROM payloads
            WHERE state='enqueued'
            ORDER BY created_at ASC
            LIMIT 1
          )
        RETURNING id, value, state, created_at, modified_at
  |]

{-| Transition a 'Payload' from the 'Locked' state to the 'Enqueued' state.
    Useful for responding to asynchronous exceptions during a unexpected
    shutdown. In general the IO API version, 'unlock', is probably more
    useful. The DB version is provided for completeness.
-}
unlockDB :: PayloadId -> DB ()
unlockDB payloadId = void $ execute
  [sql| UPDATE payloads
        SET state='enqueued'
        WHERE id=? AND state='locked'
  |]
  payloadId

-- | Transition a 'Payload' to the 'Dequeued' state.
dequeueDB :: PayloadId -> DB ()
dequeueDB payloadId = void $ execute
  [sql| UPDATE payloads
        SET state='dequeued'
        WHERE id=?
  |]
  payloadId

-- | Get the number of rows in the 'Enqueued' state.
getCountDB :: DB Int64
getCountDB = fromOnly . head <$> query_
  [sql| SELECT count(*)
        FROM payloads
        WHERE state='enqueued'
  |]
-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------
{-| Enqueue a new JSON value into the queue. See 'enqueueDB' for a version
    which can be composed with other queries in a single transaction.
-}
enqueue :: Connection -> Value -> IO PayloadId
enqueue conn value = runDBT (enqueueDB value) ReadCommitted conn

{-| Return a the oldest 'Payload' in the 'Enqueued' state or 'Nothing'
    if there are no payloads. For a blocking version utilizing PostgreSQL's
    NOTIFY and LISTEN, see 'lock'. This functions runs 'tryLockDB' as a
    'Serializable' transaction.
-}
tryLock :: Connection -> IO (Maybe Payload)
tryLock = runDBTSerializable tryLockDB

notifyPayload :: Connection -> IO ()
notifyPayload conn = do
  Notification {..} <- getNotification conn
  unless (notificationChannel == "enqueue") $ notifyPayload conn

{-| Return the oldest 'Payload' in the 'Enqueued' state or block until a
    payload arrives. This function utilizes PostgreSQL's LISTEN and NOTIFY
    functionality to avoid excessively polling of the DB while
    waiting for new payloads, without scarficing promptness.
-}
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

{-| Transition a 'Payload' from the 'Locked' state to the 'Enqueued' state.
    Useful for responding to asynchronous exceptions during a unexpected
    shutdown. For a DB API version see 'unlockDB'
-}
unlock :: Connection -> PayloadId -> IO ()
unlock conn x = runDBTSerializable (unlockDB x) conn

-- | Transition a 'Payload' to the 'Dequeued' state. his functions runs
--   'dequeueDB' as a 'Serializable' transaction.
dequeue :: Connection -> PayloadId -> IO ()
dequeue conn x = runDBTSerializable (dequeueDB x) conn

{-| Get the number of rows in the 'Enqueued' state. This function runs
    'getCountDB' in a 'ReadCommitted' transaction.
-}
getCount :: Connection -> IO Int64
getCount = runDBT getCountDB ReadCommitted
