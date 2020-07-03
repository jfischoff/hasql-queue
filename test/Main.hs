import           Test.Hspec
import           Hasql.Queue.High.ExactlyOnceSpec as HE
import           Hasql.Queue.High.AtMostOnceSpec as HM
import           Hasql.Queue.Low.AtLeastOnceSpec as LL


main :: IO ()
main = hspec $ do
  HE.spec
  HM.spec
  LL.spec
