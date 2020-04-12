import           Test.Hspec
import           Database.Hasql.Queue.SessionSpec as S
import           Database.Hasql.Queue.IOSpec as I


main :: IO ()
main = hspec $ do
  S.spec
  I.spec
