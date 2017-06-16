{-# LANGUAGE OverloadedStrings #-}
import           Control.Exception.Lifted
import           Control.Lens
import           Control.Monad
import           Control.Monad.IO.Class
import           Data.Aeson
import           Data.Text
import           Database.Queue
import           Database.Queue.Main
import           Network.AWS               as AWS
import           Network.AWS.SES.SendEmail as AWS
import           Network.AWS.SES.Types     as AWS

logFailedRequest :: MonadIO m => SendEmailResponse -> m ()
logFailedRequest resp = do
    let stat = view sersResponseStatus resp

    unless (stat >= 200 && stat < 300) $
      liftIO $ putStrLn $ "SES failed with status: " ++ show stat

makeEmail :: Text -> SendEmail
makeEmail email
  = sendEmail "async@example.com"
              (set dToAddresses [email] destination)
  $ message (content "Welcome!")
  $ set bText (Just $ content "You signed up!") AWS.body

main :: IO ()
main = do
  env <- newEnv Discover
  runResourceT $ runAWS env $ defaultMain "aws-email-queue-consumer" $ \payload _ -> do
    case pValue payload of
      String msg -> do
        resp <- AWS.send $ makeEmail msg
        logFailedRequest resp
      x -> throwIO $ userError $ "Type error. Wrong type of payload. Expected String got " ++
             show x
