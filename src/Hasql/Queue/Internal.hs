module Hasql.Queue.Internal where
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Data.Int
import           Data.Functor.Contravariant
import           Data.String.Here.Uninterpolated
import           Hasql.Statement
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

stateEncoder :: E.Value State
stateEncoder = E.enum $ \case
  Enqueued -> "enqueued"
  Dequeued -> "dequeued"
  Failed   -> "failed"

initialPayloadId :: PayloadId
initialPayloadId = PayloadId (-1)

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

enqueueNotifyPayload :: E.Value a -> [a] -> Session [PayloadId]
enqueueNotifyPayload theEncoder values = do
  res <- enqueuePayload theEncoder values
  sql "NOTIFY postgresql_simple_enqueue"
  pure res

-- TODO include special cases for single element insertion
enqueuePayload :: E.Value a -> [a] -> Session [PayloadId]
enqueuePayload theEncoder values = do
  let theQuery = [here|
        INSERT INTO payloads (attempts, value)
        SELECT 0, * FROM unnest($1)
        RETURNING id
        |]
      encoder = E.param $ E.nonNullable $ E.foldableArray $ E.nonNullable theEncoder
      decoder = D.rowList (D.column (D.nonNullable payloadIdDecoder))
      theStatement = Statement theQuery encoder decoder True

  statement values theStatement

dequeuePayload :: D.Value a -> Int -> Session [Payload a]
dequeuePayload valueDecoder count = do
  let multipleQuery = [here|
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
      multipleEncoder = E.param $ E.nonNullable $ fromIntegral >$< E.int4

      singleQuery = [here|
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

      singleEncoder = mempty

      decoder = D.rowList $ payloadDecoder valueDecoder

      theStatement = case count of
        1 -> Statement singleQuery singleEncoder decoder True
        _ -> Statement multipleQuery multipleEncoder decoder True
  statement count theStatement

-- | Get the 'Payload' given a 'PayloadId'
getPayload :: D.Value a -> PayloadId -> Session (Maybe (Payload a))
getPayload decoder payloadId = do
  let theQuery = [here|
    SELECT id, state, attempts, modified_at, value
    FROM payloads
    WHERE id = $1
  |]

      encoder = E.param (E.nonNullable payloadIdEncoder)
  statement payloadId $ Statement theQuery encoder (D.rowMaybe $ payloadDecoder decoder) True


-- | Get the number of rows in the 'Enqueued' state.
getCount :: Session Int64
getCount = do
  let decoder = D.singleRow (D.column (D.nonNullable D.int8))
      theSql = [here|
            SELECT count(*)
            FROM payloads
            WHERE state='enqueued';
        |]
      theStatement = Statement theSql mempty decoder True
  statement () theStatement