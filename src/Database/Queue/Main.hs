module Database.Queue.Main where
import Database.Queue
-- Make a defaultMain that takes in a processing functionx

defaultMain :: (Payload -> Int -> IO ()) -> IO ()
defaultMain = undefined