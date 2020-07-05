module Hasql.Queue.Internal where
import qualified Hasql.Encoders as E
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Database.PostgreSQL.LibPQ.Notify
import           Control.Monad (unless)
import           Data.Function(fix)
import           Hasql.Connection
import           Data.Int
import           Data.Functor.Contravariant
import           Data.String.Here.Uninterpolated
import           Hasql.Statement
import           Data.ByteString (ByteString)
import           Control.Exception
import           Data.Typeable
import qualified Database.PostgreSQL.LibPQ as PQ
import           Data.Maybe
import           Control.Monad.IO.Class
import qualified Data.Text.Encoding as TE
import           Data.Text (Text)

-- | A 'Payload' can exist in three states in the queue, 'Enqueued',
--   and 'Dequeued'. A 'Payload' starts in the 'Enqueued' state and is locked
--   so some sort of process can occur with it, usually something in 'IO'.
--   Once the processing is complete, the `Payload' is moved the 'Dequeued'
--   state, which is the terminal state.
data State = Enqueued | Failed
  deriving (Show, Eq, Ord, Enum, Bounded)

state :: E.Params a -> D.Result b -> ByteString -> Statement a b
state enc dec theSql = Statement theSql enc dec True

stateDecoder :: D.Value State
stateDecoder = D.enum $ \txt ->
  if txt == "enqueued" then
    pure Enqueued
  else if txt == "failed" then
    pure Failed
  else Nothing

stateEncoder :: E.Value State
stateEncoder = E.enum $ \case
  Enqueued -> "enqueued"
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
        DELETE FROM payloads
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
        DELETE FROM payloads
        WHERE id =
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

data NoRows = NoRows
  deriving (Show, Eq, Typeable)

instance Exception NoRows

withNotifyWith :: WithNotifyHandlers
               -> ByteString
               -> Connection
               -> Session a
               -> IO a
withNotifyWith WithNotifyHandlers {..} channel conn action = bracket_
  (execute conn $ "LISTEN " <> channel)
  (execute conn $ "UNLISTEN " <> channel)
  $ fix $ \restart -> do
    x <- try $ runThrow action conn
    withNotifyHandlersAfterAction
    case x of
      Left NoRows  -> do
        -- TODO record the time here
        withNotifyHandlersBeforeNotification
        notifyPayload channel conn
        restart
      Right xs -> pure xs

fst3 :: (a, b, c) -> a
fst3 (x, _, _) = x

snd3 :: (a, b, c) -> b
snd3 (_, x, _) = x

trd3 :: (a, b, c) -> c
trd3 (_, _, x) = x

listState :: State -> D.Value a -> Maybe PayloadId -> Int -> Session [(PayloadId, a)]
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

  statement (theState, defaultPayloadId, fromIntegral count) theStatement
{-|
Retrieve the payloads that have entered a failed state. See 'withDequeue' for how that
occurs. The function returns a list of values and an id. The id is used the starting
place for the next batch of values. If 'Nothing' is passed the list starts at the
beginning.
-}
failures :: D.Value a
         -- ^ Payload decoder
         -> Maybe PayloadId
         -- ^ Starting position of payloads. Pass 'Nothing' to
         --   start at the beginning
         -> Int
         -- ^ Count
         -> Session [(PayloadId, a)]
failures = listState Failed

-- Move to Internal
-- This should use bracketOnError
withDequeue :: D.Value a -> Int -> Int -> ([a] -> IO b) -> Session (Maybe b)
withDequeue decoder retryCount count f = do
  -- TODO turn to a save point
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

delete :: [PayloadId] -> Session ()
delete xs = do
  let theQuery = [here|
        DELETE FROM payloads
        WHERE id = ANY($1)
        |]

      encoder = E.param
              $ E.nonNullable
              $ E.foldableArray
              $ E.nonNullable payloadIdEncoder

  statement xs $ Statement theQuery encoder D.noResult True

withSchema :: Text -> Session a -> Session a
withSchema schemaName action = do
  -- TODO get the current search path
  let decoder = D.singleRow $ D.column $ D.nonNullable D.text
  oldSchemaName <- statement ()
                 $ Statement "show search_path" mempty decoder True

  sql (TE.encodeUtf8 $ "set search_path to '" <> schemaName <> "'")
  r <- action
  sql (TE.encodeUtf8 $ "set search_path to '" <> oldSchemaName <> "'")
  pure r
