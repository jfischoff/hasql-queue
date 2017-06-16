{-# LANGUAGE QuasiQuotes #-}
module Database.Queue.Migrate where
import           Control.Monad
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Simple.SqlQQ

migrate :: Connection -> IO ()
migrate conn = void $ execute_ conn [sql|
    CREATE TYPE state_t AS ENUM ('enqueued', 'locked', 'dequeued');

    CREATE TABLE payloads
    ( id uuid PRIMARY KEY
    , value jsonb NOT NULL
    , state state_t NOT NULL DEFAULT 'enqueued'
    , created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
    );

    CREATE INDEX state_idx ON payloads (state);
  |]
