{-# LANGUAGE QuasiQuotes #-}
module Database.Queue.Migrate where
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Simple.SqlQQ
import Control.Monad

migrate :: Connection -> IO ()
migrate conn = void $ execute_ conn [sql|
    CREATE TYPE state AS ENUM ('enqueued', 'locked', 'dequeued');

    CREATE TABLE payloads
    ( id uuid PRIMARY KEY
    , value jsonb NOT NULL
    , status state NOT NULL DEFAULT 'enqueued'
    , created   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
    );

    CREATE INDEX status_idx ON payloads (status);
  |]

