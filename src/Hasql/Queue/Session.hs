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
  ) where
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Data.Functor.Contravariant
import           Data.String.Here.Uninterpolated
import           Hasql.Statement
import           Hasql.Queue.Internal
import           Data.ByteString (ByteString)
import           Data.Maybe


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
        DELETE FROM payloads
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
        DELETE FROM payloads
        WHERE id =
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
