module Hasql.Queue.Internal where
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Database.PostgreSQL.LibPQ.Notify
import           Data.Function(fix)
import           Hasql.Connection
import           Data.Int
import           Data.Functor.Contravariant
import           Data.String.Here.Uninterpolated
import           Hasql.Statement
import           Data.ByteString (ByteString)
import           Control.Exception
import           Control.Monad.IO.Class
import           Data.Typeable
import qualified Database.PostgreSQL.LibPQ as PQ
import qualified Data.Text as T
import           Control.Monad
import           Data.List
import           Data.Bifunctor
import           Data.Maybe
import           Debug.Trace


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

{-|
Internal payload id. Used by the public api as continuation token
for pagination.
-}
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

incrementAttempts :: Int -> [PayloadId] -> Session ()
incrementAttempts retryCount pids = do
  let theQuery = [here|
        UPDATE payloads
        SET state=CASE WHEN attempts >= $1 THEN 'failed' :: state_t ELSE 'enqueued' END
          , attempts=attempts+1
        WHERE id = ANY($2)
        |]
      encoder = (fst >$< E.param (E.nonNullable E.int4)) <>
                (snd >$< E.param (E.nonNullable $ E.foldableArray $ E.nonNullable payloadIdEncoder))

      theStatement = Statement theQuery encoder D.noResult True

  statement (fromIntegral retryCount, pids) theStatement

-- | Dequeue and
--   Move to Internal
-- This should use bracketOnError
withDequeue :: D.Value a -> Int -> Int -> ([a] -> IO b) -> Session (Maybe b)
withDequeue decoder retryCount count f = do
  sql "BEGIN;SAVEPOINT temp"
  dequeuePayload decoder count >>= \case
    [] ->  Nothing <$ sql "COMMIT"
    xs -> fmap Just $ do
      liftIO (try $ f $ fmap pValue xs) >>= \case
        Left  (e :: SomeException) -> do
           sql "ROLLBACK TO SAVEPOINT temp; RELEASE SAVEPOINT temp"
           let pids = fmap pId xs
           incrementAttempts retryCount pids
           sql "COMMIT"
           liftIO (throwIO e)
        Right x ->  x <$ sql "COMMIT"

-- TODO remove
newtype QueryException = QueryException QueryError
  deriving (Eq, Show, Typeable)

instance Exception QueryException

runThrow :: Session a -> Connection -> IO a
runThrow sess conn = either (throwIO . QueryException) pure =<< run sess conn

execute :: Connection -> ByteString -> IO ()
execute conn theSql = runThrow (sql theSql) conn

-- Block until a payload notification is fired. Fired during insertion.
notifyPayload :: ByteString -> Connection -> IO ()
notifyPayload channel conn = fix $ \restart -> do
  PQ.Notify {..} <- either throwIO pure =<< withLibPQConnection conn getNotification
  unless (notifyRelname == channel) restart

-- | To aid in observability and white box testing
data WithNotifyHandlers = WithNotifyHandlers
  { withNotifyHandlersAfterAction        :: IO ()
  -- ^ An event that is trigger after the initial action, e.g.
  --   before dequeue is called.
  , withNotifyHandlersBeforeNotification :: IO ()
  -- ^ An event that is triggered before the blocking on a
  --   notification.
  }

instance Semigroup WithNotifyHandlers where
  x <> y = WithNotifyHandlers
    { withNotifyHandlersAfterAction = withNotifyHandlersAfterAction x <> withNotifyHandlersAfterAction y
    , withNotifyHandlersBeforeNotification = withNotifyHandlersBeforeNotification      x <> withNotifyHandlersBeforeNotification      y
    }

instance Monoid WithNotifyHandlers where
  mempty = WithNotifyHandlers mempty mempty

withNotifyWith :: WithNotifyHandlers -> ByteString -> Connection -> Session a -> (a -> Maybe b) -> IO b
withNotifyWith WithNotifyHandlers {..} channel conn action theCast = bracket_
  -- TODO use format
  (execute conn $ "LISTEN " <> channel)
  (execute conn $ "UNLISTEN " <> channel)
  $ fix $ \restart -> do
    x <- runThrow action conn
    withNotifyHandlersAfterAction
    case theCast x of
      Nothing -> do
        -- TODO record the time here
        withNotifyHandlersBeforeNotification
        notifyPayload channel conn
        restart
      Just xs -> pure xs

createPartitionTable :: Int -> Int -> Session ()
createPartitionTable start end = do
  let theQuery = [here|
          SELECT create_partition_table($1, $2)
        |]

      encoder = (fst >$< E.param (E.nonNullable E.int4))
             <> (snd >$< E.param (E.nonNullable E.int4))

      theStatement = Statement theQuery encoder D.noResult True

  statement (fromIntegral start, fromIntegral end) theStatement

toEndTime :: T.Text -> Int
toEndTime expression =
  let parts    = words $ T.unpack $ trace ("expression " ++ T.unpack expression) expression
      endBlob   = parts !! 5

      dropParens x = read $ reverse $ drop 2 $ reverse $ drop 2 x
  in dropParens endBlob

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

  let nextTime = maybe 0 (+1) $ listToMaybe $ reverse $ sort $ map toEndTime rangeExpressions

      go count startIndex = do
        when (count > 0) $ do
          createPartitionTable startIndex (startIndex + rangeLength)

          let nextCount = count - 1
          go nextCount (startIndex + rangeLength)

  go partitionCount nextTime

countPartitions :: Session Int
countPartitions = do
  let theQuery = [here|
        SELECT COUNT(c.oid)
        FROM pg_catalog.pg_class p
           , pg_catalog.pg_class c
           , pg_catalog.pg_inherits i
        WHERE c.oid=i.inhrelid AND i.inhparent = p.oid AND p.relname='payloads'
        |]

      decoder = D.singleRow $ D.column (D.nonNullable D.int4)

      theStatement = Statement theQuery mempty decoder True

  fromIntegral <$> statement () theStatement

dropDequeuedPartitions :: Session Int
dropDequeuedPartitions = do
  let theQuery = [here|
      SELECT c.relname, pg_catalog.pg_get_expr(c.relpartbound, c.oid)
      FROM pg_catalog.pg_class p
         , pg_catalog.pg_class c
         , pg_catalog.pg_inherits i
      WHERE c.oid=i.inhrelid AND i.inhparent = p.oid AND p.relname='payloads'
      ORDER BY pg_catalog.pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
          , c.oid::pg_catalog.regclass::pg_catalog.text;
      |]

      decoder = D.rowList
         $  (,)
        <$> D.column (D.nonNullable D.text)
        <*> D.column (D.nonNullable D.text)

      theStatement = Statement theQuery mempty decoder True

  tableNamesAndRangeExpressions <- statement () theStatement

  let oldestPartitions = map fst $ sortOn snd $ map (second toEndTime) tableNamesAndRangeExpressions
      oldestPartitionPairs = zip oldestPartitions $ tail oldestPartitions

      go :: [(T.Text, T.Text)] -> Session Int
      go [] = pure 0
      go (x:xs) = do
        let dropQuery = "SELECT drop_partition_table($1, $2)"

            dropEncoder =  (fst >$< E.param (E.nonNullable E.text))
                        <> (snd >$< E.param (E.nonNullable E.text))
            dropDecoder = D.singleRow $ D.column (D.nonNullable D.bool)

            dropStatment = Statement dropQuery dropEncoder dropDecoder True

        statement x dropStatment >>= \case
          True -> fmap (1 +) $ go xs
          False -> pure 0

  go oldestPartitionPairs
