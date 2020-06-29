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
import           Control.Monad
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

  CREATE INDEX IF NOT EXISTS active_modified_at_idx ON payloads USING btree (modified_at, state)
    WHERE (state = 'enqueued');

|]

{-| This function creates a table and enumeration type that is
    appriopiate for the queue. The following sql is used.

 @
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
  , value VALUE_TYPE NOT NULL
  ) WITH (fillfactor = 50);

  CREATE INDEX IF NOT EXISTS active_modified_at_idx ON payloads USING btree (modified_at)
    WHERE (state = 'enqueued');
 @

The @VALUE_TYPE@ needs to passed in through the second argument.
-}
migrate :: Connection
        -> String
        -- ^ The type of the @value@ column
        -> IO ()
migrate conn valueType = void $
  run (sql $ fromString $ migrationQueryString valueType) conn
