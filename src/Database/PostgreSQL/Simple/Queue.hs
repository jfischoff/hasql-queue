{-| This module utilize PostgreSQL to implement a durable queue for efficently processing
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

 For a more complete example or a consumer, utilizing the provided
 'Database.PostgreSQL.Simple.Queue.Main.defaultMain', see
 'Database.PostgreSQL.Simple.Queue.Examples.EmailQueue.EmailQueue'.

This modules provides two flavors of functions, a DB API and an IO API.
Most operations are provided in both flavors, with the exception of 'lock'.
'lock' blocks and would not be that useful as part of a larger transaction
since it would keep the transaction open for a potentially long time. Although
both flavors are provided, in general one versions is more useful for typical
use cases.

-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE QuasiQuotes                #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE OverloadedStrings          #-}
module Database.PostgreSQL.Simple.Queue
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
import           Data.Aeson
import           Data.Function
import           Data.Int
import           Data.Maybe
import           Data.Text                               (Text)
import           Data.Time
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
import           Data.Monoid
import           Data.String
-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
newtype PayloadId = PayloadId { unPayloadId :: Int64 }
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
notifyName :: IsString s => String -> s
notifyName tableName = fromString $ tableName <> "_enqueue"

{-| Enqueue a new JSON value into the queue. This particularly function
    can be composed as part of a larger database transaction. For instance,
    a single transaction could create a user and enqueue a email message.

 @
   createAccount userRecord = do
      'runDBTSerializable' $ do
         createUserDB userRecord
         'enqueueDB' $ makeVerificationEmail userRecord
 @
-}
enqueueDB :: String -> Value -> DB PayloadId
enqueueDB tableName value =
  fmap head $ query ([sql|
    NOTIFY |] <> " " <> notifyName tableName <> ";" <> [sql|
    INSERT INTO|] <> " " <> fromString tableName <> " " <> [sql|(value)
    VALUES (?)
    RETURNING id;|]
    )
    $ Only value

{-| Return a the oldest 'Payload' in the 'Enqueued' state, or 'Nothing'
    if there are no payloads. This function is not necessarily useful by
    itself, since there are not many use cases where it needs to be combined
    with other transactions. See 'tryLock' the IO API version, or for a
    blocking version utilizing PostgreSQL's NOTIFY and LISTEN, see 'lock'
-}
tryLockDB :: String -> DB (Maybe Payload)
tryLockDB tableName = fmap listToMaybe $ query_ $
  [sql| UPDATE|] <> " " <> fromString tableName <> " " <> [sql|
        SET state='locked'
        WHERE id in
          ( SELECT id
            FROM|] <> " " <> fromString tableName <> " " <> [sql|
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
unlockDB :: String -> PayloadId -> DB ()
unlockDB tableName payloadId = void $ execute (
  [sql| UPDATE|] <> " " <> fromString tableName <> " " <> [sql|
        SET state='enqueued'
        WHERE id=? AND state='locked'
  |])
  payloadId

-- | Transition a 'Payload' to the 'Dequeued' state.
dequeueDB :: String -> PayloadId -> DB ()
dequeueDB tableName payloadId = void $ execute (
  [sql| UPDATE|] <> " " <> fromString tableName <> " " <> [sql|
        SET state='dequeued'
        WHERE id=?
  |])
  payloadId

-- | Get the number of rows in the 'Enqueued' state.
getCountDB :: String -> DB Int64
getCountDB tableName = fmap (fromOnly . head) $ query_ $
  [sql| SELECT count(*)
        FROM|] <> " " <> fromString tableName <> " " <> [sql|
        WHERE state='enqueued'
  |]
-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------
{-| Enqueue a new JSON value into the queue. See 'enqueueDB' for a version
    which can be composed with other queries in a single transaction.
-}
enqueue :: String -> Connection -> Value -> IO PayloadId
enqueue tableName conn value = runDBT (enqueueDB tableName value) ReadCommitted conn

{-| Return a the oldest 'Payload' in the 'Enqueued' state or 'Nothing'
    if there are no payloads. For a blocking version utilizing PostgreSQL's
    NOTIFY and LISTEN, see 'lock'. This functions runs 'tryLockDB' as a
    'Serializable' transaction.
-}
tryLock :: String -> Connection -> IO (Maybe Payload)
tryLock tableName conn = runDBTSerializable (tryLockDB tableName) conn

notifyPayload :: String -> Connection -> IO ()
notifyPayload tableName conn = do
  Notification {..} <- getNotification conn
  unless (notificationChannel == notifyName tableName) $ notifyPayload tableName conn

{-| Return the oldest 'Payload' in the 'Enqueued' state or block until a
    payload arrives. This function utilizes PostgreSQL's LISTEN and NOTIFY
    functionality to avoid excessively polling of the DB while
    waiting for new payloads, without scarficing promptness.
-}
lock :: String -> Connection -> IO Payload
lock tableName conn = bracket_
  (Simple.execute_ conn $ "LISTEN " <> notifyName tableName)
  (Simple.execute_ conn $ "UNLISTEN " <> notifyName tableName)
  $ fix $ \continue -> do
      m <- tryLock tableName conn
      case m of
        Nothing -> do
          notifyPayload tableName conn
          continue
        Just x -> return x

{-| Transition a 'Payload' from the 'Locked' state to the 'Enqueued' state.
    Useful for responding to asynchronous exceptions during a unexpected
    shutdown. For a DB API version see 'unlockDB'
-}
unlock :: String -> Connection -> PayloadId -> IO ()
unlock tableName conn x = runDBTSerializable (unlockDB tableName x) conn

-- | Transition a 'Payload' to the 'Dequeued' state. his functions runs
--   'dequeueDB' as a 'Serializable' transaction.
dequeue :: String -> Connection -> PayloadId -> IO ()
dequeue tableName conn x = runDBTSerializable (dequeueDB tableName x) conn

{-| Get the number of rows in the 'Enqueued' state. This function runs
    'getCountDB' in a 'ReadCommitted' transaction.
-}
getCount :: String -> Connection -> IO Int64
getCount tableName = runDBT (getCountDB tableName) ReadCommitted
