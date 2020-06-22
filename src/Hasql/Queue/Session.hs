{-|
'Session' based API for a PostgreSQL backed queue.
-}
module Hasql.Queue.Session
  ( enqueue
  , enqueueNotify
  , dequeue
  -- ** Listing API
  , PayloadId
  , failed
  , dequeued
  , createPartitions
  ) where
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Data.Functor.Contravariant
import           Data.String.Here.Uninterpolated
import           Hasql.Statement
import           Hasql.Queue.Internal
import           Data.Maybe
import           Data.ByteString (ByteString)
import qualified Data.Text as T
import           Control.Monad
import           Data.List

{-|Enqueue a payload.
-}
enqueue :: E.Value a
        -- ^ Payload encoder
        -> [a]
        -- ^ List of payloads to enqueue
        -> Session ()
enqueue theEncoder = \case
  [x] -> do
    let theQuery =
          [here|
            INSERT INTO payloads (attempts, value)
            VALUES (0, $1)
          |]

        encoder = E.param $ E.nonNullable theEncoder

    statement x $ Statement theQuery encoder D.noResult True

  xs -> do
    let theQuery =
          [here|
            INSERT INTO payloads (attempts, value)
            SELECT 0, * FROM unnest($1)
          |]

        encoder = E.param $ E.nonNullable $ E.foldableArray $ E.nonNullable theEncoder

    statement xs $ Statement theQuery encoder D.noResult True

{-|Enqueue a payload send a notification on the
specified channel.
-}
enqueueNotify :: ByteString
              -- ^ Notification channel name. Any valid PostgreSQL identifier
              -> E.Value a
              -- ^ Payload encoder
              -> [a]
              -- ^ List of payloads to enqueue
              -> Session ()
enqueueNotify channel theEncoder values = do
  enqueue theEncoder values
  sql $ "NOTIFY " <> channel

{-|
Dequeue a list of payloads
-}
dequeue :: D.Value a
        -- ^ Payload decoder
        -> Int
        -- ^ Element count
        -> Session [a]
dequeue valueDecoder count = do
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
        RETURNING value
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
        RETURNING value
      |]

      singleEncoder = mempty

      decoder = D.rowList $ D.column $ D.nonNullable $ valueDecoder

      theStatement = case count of
        1 -> Statement singleQuery singleEncoder decoder True
        _ -> Statement multipleQuery multipleEncoder decoder True
  statement count theStatement

fst3 :: (a, b, c) -> a
fst3 (x, _, _) = x

snd3 :: (a, b, c) -> b
snd3 (_, x, _) = x

trd3 :: (a, b, c) -> c
trd3 (_, _, x) = x

listState :: State -> D.Value a -> Maybe PayloadId -> Int -> Session (PayloadId, [a])
listState theState valueDecoder mPayloadId count = do
  let theQuery = [here|
        SELECT id, value
        FROM payloads
        WHERE state = ($1 :: state_t)
          AND id > $2
        ORDER BY id ASC
        LIMIT $3
        |]
      encoder = (fst3 >$< E.param (E.nonNullable stateEncoder))
             <> (snd3 >$< E.param (E.nonNullable payloadIdEncoder))
             <> (trd3 >$< E.param (E.nonNullable E.int4))

      decoder =  D.rowList
              $  (,)
             <$> D.column (D.nonNullable payloadIdDecoder)
             <*> D.column (D.nonNullable valueDecoder)
      theStatement = Statement theQuery encoder decoder True

      defaultPayloadId = fromMaybe initialPayloadId mPayloadId

  idsAndValues <- statement (theState, defaultPayloadId, fromIntegral count) theStatement
  pure $ case idsAndValues of
    [] -> (defaultPayloadId, [])
    xs -> (fst $ last xs, map snd xs)

{-|
Retrieve the payloads that have entered a failed state. See 'withDequeue' for how that
occurs. The function returns a list of values and an id. The id is used the starting
place for the next batch of values. If 'Nothing' is passed the list starts at the
beginning.
-}
failed :: D.Value a
       -- ^ Payload decoder
       -> Maybe PayloadId
       -- ^ Starting position of payloads. Pass 'Nothing' to
       --   start at the beginning
       -> Int
       -- ^ Count
       -> Session (PayloadId, [a])
failed = listState Failed

{-|
Retrieve the payloads that have been successfully dequeued.
The function returns a list of values and an id. The id is used the starting
place for the next batch of values. If 'Nothing' is passed the list starts at the
beginning.
-}
dequeued :: D.Value a
         -- ^ Payload decoder
         -> Maybe PayloadId
         -- ^ Starting position of payloads. Pass 'Nothing' to
         --   start at the beginning
         -> Int
         -- ^ Count
         -> Session (PayloadId, [a])
dequeued = listState Dequeued

createPartitionTable :: Int -> Int -> Session ()
createPartitionTable start end = do
  let theQuery = [here|
          SELECT create_partition_table($1, $2)
        |]

      encoder = (fst >$< E.param (E.nonNullable E.int4))
             <> (snd >$< E.param (E.nonNullable E.int4))

      theStatement = Statement theQuery encoder D.noResult True

  statement (fromIntegral start, fromIntegral end) theStatement

createPartitions :: Int -> Int -> Session ()
createPartitions partitionCount rangeLength = do
  let theQuery = [here|
        SELECT pg_catalog.pg_get_expr(c.relpartbound, c.oid)
        FROM pg_catalog.pg_class p
           , pg_catalog.pg_class c
           , pg_catalog.pg_inherits i
        WHERE c.oid=i.inhrelid AND i.inhparent = p.oid AND p.relname='payloads'
        ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
            , c.oid::pg_catalog.regclass::pg_catalog.text;
        |]

      decoder = D.rowList $ D.column (D.nonNullable D.text)

      theStatement = Statement theQuery mempty decoder True

  rangeExpressions <- statement () theStatement

  let toEndTime expression =
        let parts    = words $ T.unpack expression
            endBlob   = parts !! 5

            dropParens :: Read a => String -> a
            dropParens x = read $ reverse $ drop 2 $ reverse $ drop 2 x
        in dropParens endBlob
      nextTime = maybe 0 (+1) $ listToMaybe $ reverse $ sort $ map toEndTime rangeExpressions

      go count startIndex = do
        createPartitionTable startIndex (startIndex + rangeLength)
        let nextCount = count - 1
        when (nextCount > 0) $ go nextCount (startIndex + rangeLength)

  go partitionCount nextTime
