{-|
A high throughput 'Session' based API for a PostgreSQL backed queue.
-}
module Hasql.Queue.High.ExactlyOnce
  ( enqueue
  , dequeue
  ) where
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Data.Functor.Contravariant
import           Data.String.Here.Uninterpolated
import           Hasql.Statement



{-|Enqueue a payload.
-}
enqueue :: E.Value a
        -- ^ Payload encoder
        -> [a]
        -- ^ List of payloads to enqueue
        -> Session ()
enqueue theEncoder = \case
  []  -> pure ()
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


{-|
Dequeue a list of payloads
-}
dequeue :: D.Value a
        -- ^ Payload decoder
        -> Int
        -- ^ Element count
        -> Session [a]
dequeue valueDecoder count
  | count <= 0 = pure []
  | otherwise = do
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
