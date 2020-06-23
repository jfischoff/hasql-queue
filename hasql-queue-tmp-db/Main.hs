import Database.Postgres.Temp
import Control.Concurrent
import Hasql.Queue.Migrate
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Base64.URL as Base64
import           Control.Exception
import           Control.Monad
import           Crypto.Hash.SHA1 (hash)
import           Hasql.Connection

withConn :: DB -> (Connection -> IO a) -> IO a
withConn db f = do
  let connStr = toConnectionString db
  bracket (either (throwIO . userError . show) pure =<< acquire connStr) release f


main :: IO ()
main = either throwIO pure <=< withDbCache $ \dbCache -> do
  migratedConfig <- either throwIO pure =<<
      cacheAction
        (("~/.tmp-postgres/" <>) . BSC.unpack . Base64.encode . hash
          $ BSC.pack $ migrationQueryString "int4")
        (flip withConn $ flip migrate "int4")
        (autoExplainConfig 1  <> cacheConfig dbCache)
  withConfig migratedConfig $ \db -> do
    print $ toConnectionString db
    forever $ threadDelay 100000000
