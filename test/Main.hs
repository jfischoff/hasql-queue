import           Test.Hspec
import           Hasql.Queue.High.ExactlyOnceSpec as S
import           Hasql.Queue.Low.AtLeastOnceSpec as I


main :: IO ()
main = hspec $ do
  S.spec
  I.spec
