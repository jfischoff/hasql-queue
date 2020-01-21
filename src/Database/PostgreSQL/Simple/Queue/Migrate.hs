{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE QuasiQuotes, OverloadedStrings #-}
module Database.PostgreSQL.Simple.Queue.Migrate where
import           Control.Monad
import           Database.PostgreSQL.Simple
import           Data.String
import           Data.String.Here.Uninterpolated

migrationQueryString :: String
migrationQueryString = [here|
    DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'state_t') THEN
      CREATE TYPE state_t AS ENUM ('enqueued', 'dequeued');
    END IF;
  END$$;

  CREATE OR REPLACE FUNCTION update_row_modified_function()
  RETURNS TRIGGER AS
  $$
  BEGIN
      NEW.modified_at = clock_timestamp();
      RETURN NEW;
  END;
  $$
  language 'plpgsql';

  CREATE TABLE IF NOT EXISTS payloads
  ( id BIGSERIAL PRIMARY KEY
  , value jsonb NOT NULL
  , attempts int NOT NULL DEFAULT 0
  , state state_t NOT NULL DEFAULT 'enqueued'
  , created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
  , modified_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT clock_timestamp()
  ) WITH (fillfactor = 50);

  CREATE INDEX IF NOT EXISTS active_modified_at_idx ON payloads USING btree (modified_at)
    WHERE (state = 'enqueued');

  CREATE INDEX IF NOT EXISTS active_created_at_idx ON payloads (created_at)
    WHERE (state = 'enqueued');

  DROP TRIGGER IF EXISTS payloads_modified ON payloads;
  CREATE TRIGGER payloads_modified
  BEFORE UPDATE ON payloads
  FOR EACH ROW EXECUTE PROCEDURE update_row_modified_function();


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
migrate :: Connection -> IO ()
migrate conn = void $ execute_ conn $
  fromString migrationQueryString
