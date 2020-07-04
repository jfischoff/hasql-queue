module Hasql.Queue.MigrateSpec where
import Hasql.Queue.Migrate
import           Hasql.Queue.TestUtils
import           Test.Hspec                     (Spec, describe, parallel, it, shouldReturn)
import           Test.Hspec.Core.Spec (sequential)
import           Control.Monad.IO.Class
import qualified Hasql.Decoders as D
import           Hasql.Session
import           Hasql.Statement
import           Data.String.Here.Uninterpolated
import qualified Hasql.Queue.Internal as I


spec :: Spec
spec = describe "Hasql.Queue.High.ExactlyOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "basic" $ do
    it "is okay to migrate multiple times" $ withConnection $ \conn ->
      liftIO $ migrate conn "int4"

    it "drops all items" $ withConnection $ \conn -> do
      teardown conn

      let theQuery = [here|
            SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE  table_schema = 'public'
            AND    table_name   = 'payloads'
            );
          |]

          decoder = D.singleRow $ D.column $ D.nonNullable D.bool
      I.runThrow (statement () $ Statement theQuery mempty decoder True) conn `shouldReturn` False

      let theQuery' = [here|
             SELECT EXISTS (
             SELECT FROM pg_catalog.pg_class c
             JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace
             WHERE  n.nspname = 'public'
             AND    c.relname = 'modified_index'
          )
          |]

          decoder' = D.singleRow $ D.column $ D.nonNullable D.bool
      I.runThrow (statement () $ Statement theQuery' mempty decoder' True) conn `shouldReturn` False

      let theQuery'' = [here|
             SELECT EXISTS (
             SELECT 1
             FROM pg_type t
             JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
             WHERE t.typname = 'state_t'
             AND n.nspname = 'public'
          )
          |]

          decoder'' = D.singleRow $ D.column $ D.nonNullable D.bool
      I.runThrow (statement () $ Statement theQuery'' mempty decoder'' True) conn `shouldReturn` False
