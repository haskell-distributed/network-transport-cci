 {-# LANGUAGE DeriveDataTypeable #-}

module Main where

import Control.Concurrent ( threadDelay )
import Control.Concurrent.MVar
import Data.Binary
import Data.Typeable

import Control.Distributed.Process hiding (catch)
import Control.Distributed.Process.Node
import Network.Transport.CCI
import Control.Monad (replicateM_)
import Prelude hiding (catch)
import Control.Exception (catch, SomeException)

client1 :: Process ()
client1  = 
  do liftIO $ threadDelay 5000000 
     liftIO $ putStrLn "Client 1 ending"

client2 :: ProcessId -> Process ()
client2 pid =
  do  liftIO $ threadDelay 1000000
      link pid
      liftIO $ catch (liftIO $ threadDelay 10000000) handler
   where handler :: SomeException -> IO ()
         handler e = putStrLn $ show e

ignition :: Process ()
ignition = do
 pid <- spawnLocal client1
 spawnLocal $ client2 pid
 receiveWait []

main :: IO ()
main = do
 Right transport <- createTransport defaultCCIParameters
 node1 <- newLocalNode transport initRemoteTable
 node2 <- newLocalNode transport initRemoteTable
 mv <- newEmptyMVar 
 runProcess node1 $ do pid <- spawnLocal client1
                       say "Client 1 launched"
                       liftIO $ putMVar mv pid
 runProcess node2 $ do pid <- liftIO $ takeMVar mv
                       spawnLocal (client2 pid)
                       say "Client 2 launched"
                       receiveWait []
