{-# LANGUAGE RecordWildCards #-}
module Test.Setup where
import           Control.Exception
import           Control.Monad
import qualified Data.ByteString.Char8        as BSC
import           Data.Pool
import qualified Database.Postgres.Temp       as Temp
import           Database.PostgreSQL.Simple
import           Database.PostgreSQL.Transact
import           Database.Queue.Migrate
import           Test.Hspec

data TestDB = TestDB
  { tempDB     :: Temp.DB
  , connection :: Pool Connection
  }

setupDB :: IO TestDB
setupDB = do
  tempDB     <- either throwIO return =<< Temp.startAndLogToTmp []
  putStrLn $ Temp.connectionString tempDB
  connection <- createPool
    (connectPostgreSQL (BSC.pack $ Temp.connectionString tempDB))
    close
    1
    100000000
    50
  withResource connection migrate
  return TestDB {..}

teardownDB :: TestDB -> IO ()
teardownDB TestDB {..} = do
  destroyAllResources connection
  void $ Temp.stop tempDB

withPool :: TestDB -> (Connection -> IO a) -> IO a
withPool testDB = withResource (connection testDB)

withDB :: DB a -> TestDB -> IO a
withDB action testDB =
  withResource (connection testDB) (runDBTSerializable action)

runDB :: TestDB -> DB a -> IO a
runDB = flip withDB

itDB :: String -> DB a -> SpecWith TestDB
itDB msg action = it msg $ void . withDB action

describeDB :: String -> SpecWith TestDB -> Spec
describeDB str =
  beforeAll setupDB . afterAll teardownDB . describe str
