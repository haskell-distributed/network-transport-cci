---- Copyright (C) 2012 Parallel Scientific. All rights reserved.
---- See the accompanying COPYING file for license information.

module Main where

import Network.CCI
import Control.Exception
import Prelude hiding ( catch )
import qualified Data.ByteString.Char8 as BS
import qualified Data.Map as M
import Control.Concurrent
import Control.Monad
import System.Environment


main = do
         args <- getArgs
         case  args of
             [] -> do
                  serverDone <- newEmptyMVar
                  putStrLn $ "EchoServer started"
                  initCCI
                  endpoint <- createPollingEndpoint Nothing
                  getEndpt_URI endpoint >>= putStrLn
                  forkIO $ echoServer endpoint serverDone
                  readMVar serverDone

             [ addr ] -> do
                  clientDone <- newEmptyMVar
                  putStrLn "Client started"
                  initCCI
                  endpoint <- createPollingEndpoint Nothing
                  forkIO $ client endpoint addr clientDone
                  readMVar clientDone




client :: Endpoint -> String -> MVar () -> IO ()
client endpoint addr clientDone = catch  loop  handler where

           handler :: SomeException -> IO ()
           handler e = putStrLn ( "Exception in Client " ++ show e ) >> putMVar clientDone () >> throw e

           loop  =  do
                connect endpoint  addr  BS.empty CONN_ATTR_RO ( 0::WordPtr ) Nothing
                newconnMVar <- newEmptyMVar
                forever $ do
                  evnt <- getEvent endpoint
                  case evnt of
                      Nothing -> return ()
                      Just s -> do
                          ev <- getEventData s
                          case ev of
                              EvSend wordPtr status connection -> do
                                      putStrLn $ "I am in EvSend " ++ show wordPtr ++ " " ++ show status ++ " " ++ show connection

                              EvRecv eventbytes  connection -> do
                                      putStrLn $  "I am in EvRecv " ++ show connection
                                      msg <- packEventBytes eventbytes
                                      BS.putStrLn msg

                              EvConnect wordPtr ( Left status ) -> do
                                      putStrLn $ "I am in EvConnect with status " ++ show wordPtr++ " " ++ show status

                              EvConnect wordPtr ( Right connection ) -> do
                                      putStrLn $ "I am in EvConnect with connection " ++  show wordPtr ++ " " ++ show connection
                                      putMVar newconnMVar connection

                              EvAccept wordPtr ( Right connection ) -> do
                                      putStrLn $ "I am in EvAccept " ++ show wordPtr ++ " " ++ show connection

                              EvConnectRequest sev  eb  attr -> do
                                      putStrLn $ "I am in EvConnectRequest " ++ show sev ++ " " ++ show eb ++ " " ++ show attr
                                      accept sev ( 0 :: WordPtr )

                              EvKeepAliveTimedOut connection -> do
                                      putStrLn $ "I am in EvKeepAliceTimedOut "  ++ show connection

                              EvEndpointDeviceFailed endpt -> do
                                      putStrLn $ "I am in EvEndPointDeviceFailed " ++ show endpt

                          conn <- readMVar newconnMVar
                          send conn  ( BS.pack "Hi Server. How are you " ) ( 0 :: WordPtr )
                          --disconnect  conn
                          threadDelay 50000000
                putMVar clientDone ()


echoServer :: Endpoint -> MVar ()  -> IO ()
echoServer endpoint serverDone = catch  loop  handler where
           handler :: SomeException -> IO ()
           handler e = putStrLn ( "Exception in echoServer " ++ show e ) >> putMVar serverDone () >> throw e

           loop  = do
               evnt <- getEvent endpoint
               case evnt of
                     Nothing -> loop
                     Just s -> do
                          ev <- getEventData s
                          case ev of
                              EvSend wordPtr status connection -> do
                                      putStrLn $ "I am in EvSend " ++ show wordPtr ++ " " ++ show status ++ " " ++ show connection
                                      loop

                              EvRecv eventbytes  connection -> do
                                     forkIO $ do
                                        putStrLn $ "I am in EvRecv "  ++ show connection
                                        msg <- packEventBytes eventbytes
                                        BS.putStrLn msg
                                        send connection ( BS.pack "Hi Client. How are you ? " ) ( 0::WordPtr )
                                     loop

                              EvConnect wordPtr ( Left status ) -> do
                                      putStrLn $ "I am in EvConnect with status " ++ show wordPtr ++ " " ++ show status
                                      loop

                              EvConnect wordPtr ( Right connection ) -> do
                                      putStrLn $ "I am in EvConnect with connection "  ++ show wordPtr ++ " " ++ show connection
                                      loop

                              EvAccept wordPtr ( Right connection ) -> do
                                        putStrLn $ "I am in EvAccept "  ++ show wordPtr ++ " " ++ show connection
                                        loop

                              EvConnectRequest sev  eb  attr -> do
                                      forkIO $ do
                                        putStrLn $  "I am in EvConnectRequest " ++ show sev ++ " " ++ show eb ++ " " ++ show attr
                                        accept sev ( 0 :: WordPtr )
                                      loop

                              EvKeepAliveTimedOut connection -> do
                                      putStrLn $ "I am in EvKeepAliveTimedOut " ++ show connection
                                      loop

                              EvEndpointDeviceFailed endpt -> do
                                      putStrLn $ "I am in EvEndPointDeviceFailed " ++ show endpt
                                      loop
                          threadDelay 50000000
                          putMVar serverDone ()
