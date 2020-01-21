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
  , setup
  , enqueueDB
  , dequeueDB
  , withPayloadDB
  , getCountDB
  -- * IO API
  , enqueue
  , tryDequeue
  , dequeue
  , withPayload
  , getCount
  ) where
import           Control.Monad
import           Control.Monad.Catch
import           Data.Aeson
import           Data.Function
import           Data.Int
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
import           Data.String
import           Control.Monad.IO.Class
import           Data.Maybe

-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
newtype PayloadId = PayloadId { unPayloadId :: Int64 }
  deriving (Eq, Show, FromField, ToField)

instance FromRow PayloadId where
  fromRow = fromOnly <$> fromRow

instance ToRow PayloadId where
  toRow = toRow . Only

-- The fundemental record stored in the queue. The queue is a single table
-- and each row consists of a 'Payload'
data Payload = Payload
  { pId         :: PayloadId
  , pValue      :: Value
  -- ^ The JSON value of a payload
  , pState      :: State
  , pAttempts   :: Int
  , pCreatedAt  :: UTCTime
  , pModifiedAt :: UTCTime
  } deriving (Show, Eq)

instance FromRow Payload where
  fromRow = Payload <$> field <*> field <*> field <*> field <*> field <*> field

-- | A 'Payload' can exist in three states in the queue, 'Enqueued',
--   and 'Dequeued'. A 'Payload' starts in the 'Enqueued' state and is locked
--   so some sort of process can occur with it, usually something in 'IO'.
--   Once the processing is complete, the `Payload' is moved the 'Dequeued'
--   state, which is the terminal state.
data State = Enqueued | Dequeued
  deriving (Show, Eq, Ord, Enum, Bounded)

instance ToField State where
  toField = toField . \case
    Enqueued -> "enqueued" :: Text
    Dequeued -> "dequeued"

-- Converting from enumerations is annoying :(
instance FromField State where
  fromField f y = do
     n <- typename f
     if n == "state_t" then case y of
       Nothing -> returnError UnexpectedNull f "state can't be NULL"
       Just y' -> case y' of
         "enqueued" -> return Enqueued
         "dequeued" -> return Dequeued
         x          -> returnError ConversionFailed f (show x)
     else
       returnError Incompatible f $
         "Expect type name to be state but it was " ++ show n

-------------------------------------------------------------------------------
---  DB API
-------------------------------------------------------------------------------
notifyName :: IsString s => s
notifyName = fromString "postgresql_simple_enqueue"

{-|
Prepare all the statements.
-}
setupDB :: DB ()
setupDB = void $ execute_
  [sql|
  PREPARE enqueue (int, jsonb) AS
    INSERT INTO payloads (attempts, value)
    VALUES ($1, $2)
    RETURNING id;

  PREPARE dequeue AS
    UPDATE payloads
    SET state='dequeued'
    WHERE id in
      ( SELECT p1.id
        FROM payloads AS p1
        WHERE p1.state='enqueued'
        ORDER BY p1.modified_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
      )
    RETURNING id, value, state, attempts, created_at, modified_at;

  PREPARE get_enqueue AS
    SELECT id, value, state, attempts, created_at, modified_at
    FROM payloads
    WHERE state='enqueued'
    ORDER BY modified_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1;

  PREPARE update_state (int8) AS
    UPDATE payloads SET state='dequeued' WHERE id = $1;

  PREPARE get_count AS
    SELECT count(*)
    FROM payloads
    WHERE state='enqueued';
  |]

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
enqueueDB :: Value -> DB PayloadId
enqueueDB value = enqueueWithDB value 0

enqueueWithDB :: Value -> Int -> DB PayloadId
enqueueWithDB value attempts =
  fmap head $ query "NOTIFY postgresql_simple_enqueue; EXECUTE enqueue(?, ?)" (attempts, value)

retryDB :: Value -> Int -> DB PayloadId
retryDB value attempts = enqueueWithDB value $ attempts + 1

-- | Transition a 'Payload' to the 'Dequeued' state.
dequeueDB :: DB (Maybe Payload)
dequeueDB = fmap listToMaybe $ query_ "EXECUTE dequeue"

{-|

Attempt to get a payload and process it. If the function passed in throws an exception
return it on the left side of the `Either`. Re-add the payload up to some passed in
maximum. Return `Nothing` is the `payloads` table is empty otherwise the result is an `a`
from the payload ingesting function.

-}
withPayloadDB :: Int
              -- ^ retry count
              -> (Payload -> IO a)
              -- ^ payload processing function
              -> DB (Either SomeException (Maybe a))
withPayloadDB retryCount f
  = query_ "EXECUTE get_enqueue"
 >>= \case
    [] -> return $ return Nothing
    [payload@Payload {..}] -> do
      execute "EXECUTE update_state(?)" pId

      -- Retry on failure up to retryCount
      handle (\e -> when (pAttempts < retryCount)
                         (void $ retryDB pValue pAttempts)
                 >> return (Left e)
             )
             $ Right . Just <$> liftIO (f payload)
    xs -> return
        $ Left
        $ toException
        $ userError
        $ "LIMIT is 1 but got more than one row: "
        ++ show xs

-- | Get the number of rows in the 'Enqueued' state.
getCountDB :: DB Int64
getCountDB = fmap (fromOnly . head) $ query_ "EXECUTE get_count"
-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------
{-|
Prepare all the statements.
-}
setup :: Connection -> IO ()
setup conn = runDBT setupDB ReadCommitted conn

{-| Enqueue a new JSON value into the queue. See 'enqueueDB' for a version
    which can be composed with other queries in a single transaction.
-}
enqueue :: Connection -> Value -> IO PayloadId
enqueue conn value = runDBT (enqueueDB value) ReadCommitted conn

-- Block until a payload notification is fired. Fired during insertion.
notifyPayload :: Connection -> IO ()
notifyPayload conn = do
  Notification {..} <- getNotification conn
  unless (notificationChannel == notifyName) $ notifyPayload conn

{-| Return a the oldest 'Payload' in the 'Enqueued' state or 'Nothing'
    if there are no payloads. For a blocking version utilizing PostgreSQL's
    NOTIFY and LISTEN, see 'dequeue'. This functions runs 'dequeueDb' as a
    'ReadCommitted' transaction.

  See `withPayload' for an alternative interface that will automatically return
  the payload to the 'Enqueued' state if an exception occurs.
-}
tryDequeue :: Connection -> IO (Maybe Payload)
tryDequeue conn = runDBT dequeueDB ReadCommitted conn

-- | Transition a 'Payload' to the 'Dequeued' state. his functions runs
--   'dequeueDB' as a 'Serializable' transaction.
dequeue :: Connection -> IO Payload
dequeue conn = bracket_
  (Simple.execute_ conn $ "LISTEN " <> notifyName)
  (Simple.execute_ conn $ "UNLISTEN " <> notifyName)
  $ fix $ \continue -> do
      m <- tryDequeue conn
      case m of
        Nothing -> do
          notifyPayload conn
          continue
        Just x -> return x

{-| Return the oldest 'Payload' in the 'Enqueued' state or block until a
    payload arrives. This function utilizes PostgreSQL's LISTEN and NOTIFY
    functionality to avoid excessively polling of the DB while
    waiting for new payloads, without scarficing promptness.
-}
withPayload :: Connection
            -> Int
            -- ^ retry count
            -> (Payload -> IO a)
            -> IO (Either SomeException a)
withPayload conn retryCount f = bracket_
  (Simple.execute_ conn $ "LISTEN " <> notifyName)
  (Simple.execute_ conn $ "UNLISTEN " <> notifyName)
  $ fix
  $ \continue -> runDBT (withPayloadDB retryCount f) ReadCommitted conn
  >>= \case
    Left x -> return $ Left x
    Right Nothing -> do
      notifyPayload conn
      continue
    Right (Just x) -> return $ Right x

{-| Get the number of rows in the 'Enqueued' state. This function runs
    'getCountDB' in a 'ReadCommitted' transaction.
-}
getCount :: Connection -> IO Int64
getCount = runDBT getCountDB ReadCommitted
