 {-# LANGUAGE DeriveDataTypeable #-}

module Main where

import Control.Concurrent ( threadDelay )
import Data.Binary
import Data.Typeable

import Control.Distributed.Process
import Control.Distributed.Process.Node
import Network.Transport.CCI
import Control.Monad (replicateM_)

-- Serializable (= Binary + Typeable)
data Ping = Ping deriving (Typeable)

count = 10000

instance Binary Ping where
 put Ping = putWord8 0
 get= do { getWord8; return Ping }

server :: ReceivePort Ping -> Process ()
server rPing = 
 replicateM_ count $ 
   do Ping <- receiveChan rPing
      liftIO $ putStrLn "Got a ping!"

client :: SendPort Ping -> Process ()
client sPing =
 replicateM_ count $ sendChan sPing Ping

ignition :: Process ()
ignition = do
 -- start the server
 sPing <- spawnChannelLocal server
 -- start the client
 spawnLocal $ client sPing
 liftIO $ threadDelay 1000000 -- wait a while

main :: IO ()
main = do
 Right transport <- createTransport defaultCCIParameters
 node <- newLocalNode transport initRemoteTable
 runProcess node ignition
