import           Test.Hspec
import           Hasql.Queue.High.ExactlyOnceSpec as HE
import           Hasql.Queue.High.AtLeastOnceSpec as HL
import           Hasql.Queue.High.AtMostOnceSpec as HM
import           Hasql.Queue.Low.AtLeastOnceSpec as LL
import           Hasql.Queue.Low.AtMostOnceSpec as LM
import           Hasql.Queue.Low.AtMostOnceSpec as LE
import           Hasql.Queue.MigrateSpec as M


main :: IO ()
main = hspec $ do
  HE.spec
  HM.spec
  HL.spec
  LL.spec
  LM.spec
  LE.spec
  M.spec
