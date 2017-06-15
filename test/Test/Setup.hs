{-# LANGUAGE RecordWildCards #-}
module Test.Setup where
import Test.Hspec
import Database.Queue.Migrate
import qualified Database.Postgres.Temp as Temp
import Database.PostgreSQL.Simple
import Database.PostgreSQL.Transact
import qualified Data.ByteString.Char8 as BSC
import Control.Exception
import Control.Monad
import Data.Pool

data TestDB = TestDB
  { tempDB     :: Temp.DB
  , connection :: Pool Connection
  }

setupDB :: IO TestDB
setupDB = do
  tempDB     <- either throwIO return =<< Temp.startAndLogToTmp
-- tempDB     <- either throwIO return =<< Temp.start
  putStrLn $ Temp.connectionString tempDB
  connection <- createPool
                  (connectPostgreSQL (BSC.pack $ Temp.connectionString tempDB))
                  (close)
                  1
                  100000000
                  50
  withResource connection migrate
  return TestDB {..}

teardownDB :: TestDB -> IO ()
teardownDB TestDB {..} = do
  putStrLn "Shutting Down"
  destroyAllResources connection
  void $ Temp.stop tempDB

withConnection :: TestDB -> (Connection -> IO a) -> IO a
withConnection testDB = withResource (connection testDB)

withDB :: DB a -> TestDB -> IO a
withDB action testDB = withResource (connection testDB) (runDBTSerializable action)

runDB :: TestDB -> DB a -> IO a
runDB = flip withDB

itDB :: String -> DB a -> SpecWith TestDB
itDB msg action = it msg $ void . withDB action

describeDB :: String -> SpecWith TestDB -> Spec
describeDB str = beforeAll setupDB . afterAll teardownDB . describe str

