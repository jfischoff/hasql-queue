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

This modules provides two flavors of functions, a Session API and an IO API.
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
module Database.Hasql.Queue
  ( -- * Types
    PayloadId (..)
  , payloadIdDecoder
  , payloadIdRow
  , payloadIdEncoder
  , State (..)
  , stateDecoder
  , Payload (..)
  , payloadDecoder
  , enqueue
  , enqueueNoNotify
  , enqueueNoNotifyDB
  , dequeue
  , tryDequeue
  , enqueueDB
  , dequeueDB
  , getCount
  , getCountDB
  , withPayloadDB
  , withPayload
  , execute
  -- * Session API
  ) where

import           Data.String.Here.Uninterpolated
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Connection
import           Hasql.Statement
import           Hasql.Session
import           Data.Time
import           Data.Int
import           Data.Aeson
import           Data.Functor.Contravariant
import           Control.Exception
import           Hasql.Notification
import           Data.String
import           Control.Monad (unless)
import           Data.ByteString (ByteString)
import           Data.Function
import           Data.Bitraversable
import           Data.Traversable
import           Data.Bifunctor
import           Control.Monad.IO.Class
import           Control.Monad ((<=<))
import           Control.Monad.Trans.Class
import           Control.Monad.Trans.Maybe
import           Control.Monad.Trans.Except
import           Data.Typeable

-------------------------------------------------------------------------------
---  Types
-------------------------------------------------------------------------------
-- TODO remove
newtype QueryException = QueryException QueryError
  deriving (Eq, Show, Typeable)

queryErrorToSomeException :: QueryError -> SomeException
queryErrorToSomeException = toException . QueryException

instance Exception QueryException

newtype PayloadId = PayloadId { unPayloadId :: Int64 }
  deriving (Eq, Show)

payloadIdEncoder :: E.Value PayloadId
payloadIdEncoder = unPayloadId >$< E.int8

payloadIdDecoder :: D.Value PayloadId
payloadIdDecoder = PayloadId <$> D.int8

payloadIdRow :: D.Row PayloadId
payloadIdRow = D.column (D.nonNullable payloadIdDecoder)

-- | A 'Payload' can exist in three states in the queue, 'Enqueued',
--   and 'Dequeued'. A 'Payload' starts in the 'Enqueued' state and is locked
--   so some sort of process can occur with it, usually something in 'IO'.
--   Once the processing is complete, the `Payload' is moved the 'Dequeued'
--   state, which is the terminal state.
data State = Enqueued | Dequeued | Failed
  deriving (Show, Eq, Ord, Enum, Bounded)

stateDecoder :: D.Value State
stateDecoder = D.enum $ \txt ->
  if txt == "enqueued" then
    pure Enqueued
  else if txt == "dequeued" then
    pure Dequeued
  else if txt == "failed" then
    pure Failed
  else Nothing


-- | The fundemental record stored in the queue. The queue is a single table
-- and each row consists of a 'Payload'
data Payload a = Payload
  { pId         :: PayloadId
  , pState      :: State
  , pAttempts   :: Int
  , pModifiedAt :: Int
  , pValue      :: a
  } deriving (Show, Eq)

-- | 'Payload' decoder
payloadDecoder :: D.Value a -> D.Row (Payload a)
payloadDecoder thePayloadDecoder
   =  Payload
  <$> payloadIdRow
  <*> D.column (D.nonNullable stateDecoder)
  <*> D.column (D.nonNullable $ fromIntegral <$> D.int4)
  <*> D.column (D.nonNullable $ fromIntegral <$> D.int4)
  <*> D.column (D.nonNullable thePayloadDecoder)

-- TODO make an `enqueueNoNotifyDB`
enqueueNoNotifyDB :: E.Value a -> a -> Session PayloadId
enqueueNoNotifyDB theEncoder value = do
  let theQuery = [here|
        INSERT INTO payloads (attempts, value)
        VALUES (0, $1)
        RETURNING id
        |]
      theStatement = Statement theQuery encoder decoder True
      encoder = E.param $ E.nonNullable theEncoder
      decoder = D.singleRow (D.column (D.nonNullable payloadIdDecoder))

  statement value theStatement

enqueueNoNotify :: Connection -> E.Value a -> a -> IO PayloadId
enqueueNoNotify conn theEncoder val = either (throwIO . userError . show) pure
  =<< run (transaction $ enqueueNoNotifyDB theEncoder val) conn


{-| Enqueue a new JSON value into the queue. This particularly function
    can be composed as part of a larger database transaction. For instance,
    a single transaction could create a user and enqueue a email message.

 @
   createAccountDB userRecord = do
     createUserDB userRecord
     'enqueueDB' $ makeVerificationEmail userRecord
 @
-}
enqueueDB :: E.Value a -> a -> Session PayloadId
enqueueDB theEncoder value = do
  sql "NOTIFY postgresql_simple_enqueue"
  enqueueNoNotifyDB theEncoder value

{-| Enqueue a new JSON value into the queue. See 'enqueueDB' for a version
    which can be composed with other queries in a single transaction.
-}
enqueue :: Connection -> E.Value a -> a -> IO PayloadId
enqueue conn theEncoder val =
  either (throwIO . userError . show) pure
    =<< run (transaction $ enqueueDB theEncoder val) conn

dequeueDB :: D.Value a -> Session (Maybe (Payload a))
dequeueDB valueDecoder = do
  let theQuery = [here|
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
        RETURNING id, state, attempts, modified_at, value
        |]
      encoder = mempty
      decoder = D.rowMaybe (payloadDecoder valueDecoder)
      theStatement = Statement theQuery encoder decoder True
  statement () theStatement

{-| Return a the oldest 'Payload' in the 'Enqueued' state or 'Nothing'
    if there are no payloads. For a blocking version utilizing PostgreSQL's
    NOTIFY and LISTEN, see 'dequeue'. This functions runs 'dequeueDb' as a
    'ReadCommitted' transaction.

  See `withPayload' for an alternative interface that will automatically return
  the payload to the 'Enqueued' state if an exception occurs.
-}
tryDequeue :: Connection -> D.Value a -> IO (Maybe (Payload a))
tryDequeue conn decoder =
  either (throwIO . userError . show) pure
    =<< run (transaction $ dequeueDB decoder) conn

notifyName :: IsString s => s
notifyName = fromString "postgresql_simple_enqueue"

-- Block until a payload notification is fired. Fired during insertion.
notifyPayload :: Connection -> IO ()
notifyPayload conn = do
  Notification {..} <- either throwIO pure =<< getNotification conn
  unless (notificationChannel == notifyName) $ notifyPayload conn

execute :: Connection -> ByteString -> IO ()
execute conn theSql = either (throwIO . userError . show) pure =<< run (sql theSql) conn

-- | Transition a 'Payload' to the 'Dequeued' state. his functions runs
--   'dequeueDB' as a 'Serializable' transaction.
dequeue :: Connection -> D.Value a -> IO (Payload a)
dequeue conn decoder = bracket_
  (execute conn $ "LISTEN " <> notifyName)
  (execute conn $ "UNLISTEN " <> notifyName)
  $ fix $ \continue -> do
      m <- tryDequeue conn decoder
      case m of
        Nothing -> do
          notifyPayload conn
          continue
        Just x -> return x


{-| Get the number of rows in the 'Enqueued' state. This function runs
    'getCountDB' in a 'ReadCommitted' transaction.
-}
getCount :: Connection -> IO Int64
getCount conn = either (throwIO . userError . show) pure =<< run getCountDB conn

state :: E.Params a -> D.Result b -> ByteString -> Statement a b
state enc dec theSql = Statement theSql enc dec True

-- | Get the number of rows in the 'Enqueued' state.
getCountDB :: Session Int64
getCountDB = do

  let decoder = D.singleRow (D.column (D.nonNullable D.int8))
      theSql = [here|
            SELECT count(*)
            FROM payloads
            WHERE state='enqueued';
        |]
      theStatement = Statement theSql mempty decoder True
  statement () theStatement

getEnqueue :: D.Value a -> Session (Maybe (Payload a))
getEnqueue decoder = statement () $ state mempty (D.rowMaybe $ payloadDecoder decoder) [here|
    SELECT id, state, attempts, modified_at, value
    FROM payloads
    WHERE state='enqueued'
    ORDER BY modified_at ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 1;
  |]


setDequeued :: PayloadId -> Session ()
setDequeued thePid = statement thePid $ state (E.param (E.nonNullable payloadIdEncoder)) D.noResult [here|
    UPDATE payloads SET state='dequeued' WHERE id = $1
  |]

setEnqueueWithCount :: PayloadId -> Int -> Session ()
setEnqueueWithCount thePid retries = do
  let encoder = (fst >$< E.param (E.nonNullable payloadIdEncoder)) <>
                (snd >$< E.param (E.nonNullable E.int4))
  statement (thePid, fromIntegral retries) $ state encoder D.noResult [here|
    UPDATE payloads SET state='enqueued', modified_at=nextval('modified_index'), attempts=$2 WHERE id = $1
  |]

setFailed :: PayloadId -> Session ()
setFailed thePid = do
  let encoder = E.param (E.nonNullable payloadIdEncoder)
  statement thePid $ state encoder D.noResult [here|
    UPDATE payloads SET state='failed' WHERE id = $1
  |]

withPayloadDB :: D.Value a
              -> Int
              -- ^ retry count
              -> (Payload a -> IO b)
              -- ^ payload processing function
              -> Session (Either SomeException (Maybe b))
withPayloadDB decoder retryCount f = getEnqueue decoder >>= \case
  Nothing -> pure (Right Nothing)
  Just payload@Payload {..} -> fmap Just <$> do
    setDequeued pId

    let updateStateOnFailure = if pAttempts < retryCount
          then setEnqueueWithCount pId (pAttempts + 1)
          else setFailed pId

    bisequenceA
      .   bimap (\e -> updateStateOnFailure >> return e) pure
      =<< liftIO (try $ f payload)

{-| Return the oldest 'Payload' in the 'Enqueued' state or block until a
    payload arrives. This function utilizes PostgreSQL's LISTEN and NOTIFY
    functionality to avoid excessively polling of the Session while
    waiting for new payloads, without scarficing promptness.
-}

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
    run (transaction $ withPayloadDB decoder retryCount f) conn >>= \case
      Left a -> throwIO $ QueryException a
      Right a -> case a of
        Left x -> return $ Left x
        Right Nothing -> do
          notifyPayload conn
          continue
        Right (Just x) -> return $ Right x

{-


-------------------------------------------------------------------------------
---  Session API
-------------------------------------------------------------------------------



-- | Transition a 'Payload' to the 'Dequeued' state.






-------------------------------------------------------------------------------
---  IO API
-------------------------------------------------------------------------------

-}
