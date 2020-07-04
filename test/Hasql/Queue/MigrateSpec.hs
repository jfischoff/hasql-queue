module Hasql.Queue.MigrateSpec where
import Hasql.Queue.Migrate
import           Hasql.Queue.TestUtils
import           Test.Hspec                     (Spec, describe, parallel, it)
import           Test.Hspec.Core.Spec (sequential)
import           Control.Monad.IO.Class

spec :: Spec
spec = describe "Hasql.Queue.High.ExactlyOnce" $ parallel $ do
  sequential $ aroundAll withSetup $ describe "basic" $ do
    it "is okay to migrate multiple times" $ withConnection $ \conn ->
      liftIO $ migrate conn "int4"
