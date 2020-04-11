module Hasql.Queue.Session where
import           Hasql.Connection
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Hasql.Session
import           Data.Int
import           Data.Functor.Contravariant
import           Data.String.Here.Uninterpolated
import           Hasql.Statement
import           Control.Exception
import           Data.Bifunctor
import           Data.Bitraversable
import           Control.Monad.IO.Class
import           Data.ByteString (ByteString)

-- | A 'Payload' can exist in three states in the queue, 'Enqueued',
--   and 'Dequeued'. A 'Payload' starts in the 'Enqueued' state and is locked
--   so some sort of process can occur with it, usually something in 'IO'.
--   Once the processing is complete, the `Payload' is moved the 'Dequeued'
--   state, which is the terminal state.
data State = Enqueued | Dequeued | Failed
  deriving (Show, Eq, Ord, Enum, Bounded)

state :: E.Params a -> D.Result b -> ByteString -> Statement a b
state enc dec theSql = Statement theSql enc dec True

stateDecoder :: D.Value State
stateDecoder = D.enum $ \txt ->
  if txt == "enqueued" then
    pure Enqueued
  else if txt == "dequeued" then
    pure Dequeued
  else if txt == "failed" then
    pure Failed
  else Nothing

newtype PayloadId = PayloadId { unPayloadId :: Int64 }
  deriving (Eq, Show)

-- | The fundemental record stored in the queue. The queue is a single table
-- and each row consists of a 'Payload'
data Payload a = Payload
  { pId         :: PayloadId
  , pState      :: State
  -- TODO do I need this?
  , pAttempts   :: Int
  , pModifiedAt :: Int
  -- TODO rename. I don't need this either.
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

payloadIdEncoder :: E.Value PayloadId
payloadIdEncoder = unPayloadId >$< E.int8

payloadIdDecoder :: D.Value PayloadId
payloadIdDecoder = PayloadId <$> D.int8

payloadIdRow :: D.Row PayloadId
payloadIdRow = D.column (D.nonNullable payloadIdDecoder)

enque :: E.Value a -> [a] -> Session [PayloadId]
enque theEncoder values = do
  let theQuery = [here|
        INSERT INTO payloads (attempts, value)
        SELECT 0, * FROM unnest($1)
        RETURNING id
        |]
      encoder = E.param $ E.nonNullable $ E.foldableArray $ E.nonNullable theEncoder
      decoder = D.rowList (D.column (D.nonNullable payloadIdDecoder))
      theStatement = Statement theQuery encoder decoder True

  statement values theStatement

enque_ :: E.Value a -> [a] -> Session ()
enque_ theEncoder values = do
  let theQuery = [here|
        INSERT INTO payloads (attempts, value)
        SELECT 0, * FROM unnest($1)
        |]
      encoder = E.param $ E.nonNullable $ E.foldableArray $ E.nonNullable theEncoder
      decoder = D.noResult
      theStatement = Statement theQuery encoder decoder True

  statement values theStatement

enqueueNotify :: E.Value a -> [a] -> Session [PayloadId]
enqueueNotify theEncoder values = do
  sql "NOTIFY postgresql_simple_enqueue"
  enque theEncoder values

enqueueNotify_ :: E.Value a -> [a] -> Session ()
enqueueNotify_ theEncoder values = do
  sql "NOTIFY postgresql_simple_enqueue"
  enque_ theEncoder values

dequeue :: D.Value a -> Int -> Session [Payload a]
dequeue valueDecoder count = do
  let theQuery = [here|
      UPDATE payloads
      SET state='dequeued'
      WHERE id in
        ( SELECT p1.id
          FROM payloads AS p1
          WHERE p1.state='enqueued'
          ORDER BY p1.modified_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        )
      RETURNING id, state, attempts, modified_at, value
      |]
      encoder = E.param $ E.nonNullable $ fromIntegral >$< E.int4
      decoder = D.rowList $ payloadDecoder valueDecoder
      theStatement = Statement theQuery encoder decoder True
  statement count theStatement

dequeueValues :: D.Value a -> Int -> Session [a]
dequeueValues valueDecoder count = do
  let theQuery = [here|
      UPDATE payloads
      SET state='dequeued'
      WHERE id in
        ( SELECT p1.id
          FROM payloads AS p1
          WHERE p1.state='enqueued'
          ORDER BY p1.modified_at ASC
          FOR UPDATE SKIP LOCKED
          LIMIT $1
        )
      RETURNING value
      |]
      encoder = E.param $ E.nonNullable $ fromIntegral >$< E.int4
      decoder = D.rowList $ D.column $ D.nonNullable valueDecoder
      theStatement = Statement theQuery encoder decoder True
  statement count theStatement

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

withPayload :: D.Value a -> Int -> (Payload a -> IO b) -> Session (Either SomeException (Maybe b))
withPayload decoder retryCount f = getEnqueue decoder >>= \case
  Nothing -> pure (Right Nothing)
  Just payload@Payload {..} -> fmap Just <$> do
    setDequeued pId

    let updateStateOnFailure = if pAttempts < retryCount
          then setEnqueueWithCount pId (pAttempts + 1)
          else setFailed pId

    bisequenceA
      .   bimap (\e -> updateStateOnFailure >> return e) pure
      =<< liftIO (try $ f payload)
