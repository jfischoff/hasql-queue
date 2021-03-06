{-|
Functions for migrating the database to create the necessary
functions for the package.

Users can use these functions or copy and paste the tables
to create these tables through a standalone migration
system.
-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE QuasiQuotes, OverloadedStrings #-}
module Hasql.Queue.Migrate where
import qualified Hasql.Queue.Internal as I
import           Data.String
import           Data.String.Here.Interpolated
import           Hasql.Connection
import           Hasql.Session


{-|
The DDL statements to create the schema given a value type.
-}
migrationQueryString :: String
                     -- ^ @value@ column type, e.g. @int4@ or
                     -- @jsonb@.
                     -> String
migrationQueryString valueType = [i|
  CREATE OR REPLACE FUNCTION notify_on(channel text) RETURNs VOID AS $$
    BEGIN
      EXECUTE (format(E'NOTIFY %I', channel));
    END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION listen_on(channel text) RETURNS VOID AS $$
    BEGIN
      EXECUTE (format(E'LISTEN %I', channel));
    END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION unlisten_on(channel text) RETURNS VOID AS $$
    BEGIN
      EXECUTE (format(E'UNLISTEN %I', channel));
    END;
  $$ LANGUAGE plpgsql;

    DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'state_t') THEN
      CREATE TYPE state_t AS ENUM ('enqueued', 'failed');
    END IF;
  END$$;

  CREATE SEQUENCE IF NOT EXISTS modified_index START 1;

  CREATE TABLE IF NOT EXISTS payloads
  ( id BIGSERIAL PRIMARY KEY
  , attempts int NOT NULL DEFAULT 0
  , state state_t NOT NULL DEFAULT 'enqueued'
  , modified_at int8 NOT NULL DEFAULT nextval('modified_index')
  , value ${valueType} NOT NULL
  );

  CREATE INDEX IF NOT EXISTS active_modified_at_idx ON payloads USING btree (modified_at)
    WHERE (state = 'enqueued');

|]

{-| This function creates a table and enumeration type that is
    appriopiate for the queue. The following sql is used.

 @
 DO $$
  CREATE OR REPLACE FUNCTION notify_on(channel text) RETURNs VOID AS $$
    BEGIN
      EXECUTE (format(E'NOTIFY %I', channel));
    END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION listen_on(channel text) RETURNS VOID AS $$
    BEGIN
      EXECUTE (format(E'LISTEN %I', channel));
    END;
  $$ LANGUAGE plpgsql;

  CREATE OR REPLACE FUNCTION unlisten_on(channel text) RETURNS VOID AS $$
    BEGIN
      EXECUTE (format(E'UNLISTEN %I', channel));
    END;
  $$ LANGUAGE plpgsql;

    DO $$
  BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'state_t') THEN
      CREATE TYPE state_t AS ENUM ('enqueued', 'failed');
    END IF;
  END$$;

  CREATE SEQUENCE IF NOT EXISTS modified_index START 1;

  CREATE TABLE IF NOT EXISTS payloads
  ( id BIGSERIAL PRIMARY KEY
  , attempts int NOT NULL DEFAULT 0
  , state state_t NOT NULL DEFAULT 'enqueued'
  , modified_at int8 NOT NULL DEFAULT nextval('modified_index')
  , value ${VALUE_TYPE} NOT NULL
  );

  CREATE INDEX IF NOT EXISTS active_modified_at_idx ON payloads USING btree (modified_at, state)
    WHERE (state = 'enqueued');
 @

The @VALUE_TYPE@ needs to passed in through the second argument.
-}
migrate :: Connection
        -> String
        -- ^ The type of the @value@ column
        -> IO ()
migrate conn valueType =
  I.runThrow (sql $ fromString $ migrationQueryString valueType) conn

{-|
Drop everything created by 'migrate'
-}
teardown :: Connection -> IO ()
teardown conn = do
  let theQuery = [i|
        DROP TABLE IF EXISTS payloads;
        DROP TYPE IF EXISTS state_t;
        DROP SEQUENCE IF EXISTS modified_index;
        DROP FUNCTION IF EXISTS notify_on;
        DROP FUNCTION IF EXISTS listen_on;
        DROP FUNCTION IF EXISTS unlisten_on;
      |]

  I.runThrow (sql theQuery) conn
