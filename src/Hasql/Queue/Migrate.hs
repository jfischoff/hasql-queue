{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE QuasiQuotes, OverloadedStrings #-}
module Hasql.Queue.Migrate where
import           Control.Monad
import           Data.String
import           Data.String.Here.Uninterpolated
import           Hasql.Connection
import           Hasql.Session

migrationQueryString :: String
migrationQueryString = [here|
    DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'state_t') THEN
      CREATE TYPE state_t AS ENUM ('enqueued', 'dequeued', 'failed');
    END IF;
  END$$;

  CREATE SEQUENCE IF NOT EXISTS modified_index START 1;

  CREATE TABLE IF NOT EXISTS payloads
  ( id BIGSERIAL PRIMARY KEY
  , attempts int NOT NULL DEFAULT 0
  , state state_t NOT NULL DEFAULT 'enqueued'
  , modified_at int8 NOT NULL DEFAULT nextval('modified_index')
  ) WITH (fillfactor = 50);

  CREATE INDEX IF NOT EXISTS active_modified_at_idx ON payloads USING btree (modified_at)
    WHERE (state = 'enqueued');

|]

intPayloadMigration :: String
intPayloadMigration = [here|
    ALTER TABLE payloads ADD COLUMN  IF NOT EXISTS value int4 NOT NULL;
  |]

{-| This function creates a table and enumeration type that is
    appriopiate for the queue. The following sql is used.

 @
 CREATE TYPE state_t AS ENUM ('enqueued', 'locked', 'dequeued');

 CREATE TABLE payloads
 ( id uuid PRIMARY KEY
 , value jsonb NOT NULL
 , state state_t NOT NULL DEFAULT 'enqueued'
 , created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
 , modified_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
 );

 CREATE INDEX state_idx ON payloads (state);

 CREATE OR REPLACE FUNCTION update_row_modified_function_()
 RETURNS TRIGGER
 AS
 $$
 BEGIN
     -- ASSUMES the table has a column named exactly "modified_at".
     -- Fetch date-time of actual current moment from clock,
     -- rather than start of statement or start of transaction.
     NEW.modified_at = clock_timestamp();
     RETURN NEW;
 END;
 $$
 language 'plpgsql';
 @

-}

migrate :: Connection -> String -> IO ()
migrate conn alter = void $
  run (sql $ fromString $ migrationQueryString <> alter) conn
