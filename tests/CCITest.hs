
import Network.CCI
import qualified Network.Transport as N
import Network.Transport.CCI ( createTransport , defaultCCIParameters )
import Control.Exception
import Prelude hiding ( catch )
import qualified Data.ByteString.Char8 as BS
import qualified Data.Map as M
import Control.Concurrent
import Control.Monad
import System.Environment
import Debug.Trace

main = do
     testCase_1 <- do 
        putStrLn "1-->\nTest that the server gets a ConnectionClosed message when the client closes the socket without sending an explicit control message to the server first."
        putStrLn "Test case specific to TCP. No ConnectionClosed message in CCI. If client has closed the connection then server will come to know based on keep alive time out." 
        [ clientDone , serverDone ] <- replicateM 2 newEmptyMVar
        testEarlyDisconnect clientDone serverDone
        mapM_ readMVar [ clientDone , serverDone ]
    
     testCase_2 <- do 
        putStrLn "\n2-->\nThe behaviour of a premature CloseSocket request.This test case is specific to TCP"
        putStr "Connection can not be established until the server grants the permission. When server accepts the connection and after that  client "
        putStr " has closed the connection then server send status will be ETIMEDOUT if server wants to communicate with client. Based on keep alive timeout application will have "
        putStrLn " to take care of disconnection. Almost similart to 1"
        --testEarlyCloseSocket
     {--
     testCase_3 <- do 
        putStrLn "\n3-->\nTest the creation of a transport with an invalid address."
        --ToDo
        --testInvalidAddress
     --}
     testCase_4 <- do 
        putStrLn "\n4-->\nTest connecting to invalid or non-existing endpoints. This can be done by passing invalid address of  server to client trying to connect to server."
        putStrLn "See the CCIServer.hs file.Run the server and pass the its wrong address ( wrong IP + correct port ) or ( correct IP + wrong port )"
        --testInvalidConnect
     {--
     testCase_5 <- do 
        putStrLn "\n5-->\nTest that an endpoint can ignore CloseSocket requests (in reality this would happen when the endpoint sends a new connection request before receiving an (already underway) CloseSocket request)."
        putStr "We don't have any such request in CCI ( CloseSocket ). When client will close the connection , server will not aware until keep alive timeout event occur."
        putStrLn "We don't have a close request in CCI, but we do have one at the transport layer. This is implemented as ControlMessageCloseConnection.Thanks Jeff!"
        --ToDo see the Transport layer coder 
        --testIgnoreCloseSocket

     testCase_6 <- do 
       putStrLn "\n6-->\nLike test case 5 , but now the server requests a connection after the client closed their connection. In the meantime, the server will have sent a CloseSocket request to the client, and must block until the client responds."
       --ToDo 
       --testBlockAfterCloseSocket
     --}  
     testCase_7 <- do 
       putStrLn "\n7-->\nTest what happens when a remote endpoint sends a connection request to our transport for an endpoint it already has a connection to."
       putStrLn "Client will send 10 concurrent request and only one will be accepted."
       [ clientDone , serverDone ] <- replicateM 2 newEmptyMVar
       testUnnecessaryConnectConcurrent clientDone serverDone
       mapM_ takeMVar [ clientDone , serverDone ]       
       putStrLn "\nClient will send 10 sequential connection"
       testUnnecessaryConnectSequential  clientDone serverDone
       mapM_ readMVar [ clientDone , serverDone ] 


     testCase_8 <- do 
       putStrLn "\n8-->\nTest that we can create \"many\" transport instances."
       testMany
       
     {--
     testCase_9 <- do 
      putStrLn "\n9-->\nTest what happens when the transport breaks completely."
      --ToDO
      --testBreakTransport
     --}  
     testCase_10 <- do 
       putStr "\n10-->\nTest that a second call to 'connect' might succeed even if the first failed. This is a TCP specific test rather than an endpoint specific test"
       putStr " because we must manually create the endpoint address to match an endpoint we have yet to set up.  Then test that we get a connection lost message after the remote "
       putStrLn " endpoint suddenly closes the socket, and that a subsequent 'connect' allows us to re-establish a connection to the same endpoint."
       putStrLn "Again TCP specific. Call the connect function twice. Reject first request and accept second."
       [ clientDone , serverDone ] <- replicateM 2 newEmptyMVar
       testReconnect clientDone serverDone
       mapM_ readMVar [ clientDone , serverDone ] 
       
      
     testCase_11 <- do 
       putStrLn "\n11-->\nTest what happens if we close the socket one way only. This means that the 'recv' in 'handleIncomingMessages' will not fail, but a 'send' or 'connect' *will* fail. We are testing that error handling everywhere does the right  thing."
       putStrLn "Actually, CCI connections are bi-directional, but the Transport layer uses (mostly) only one direction. It would be interesting to see what happens if one side of the connection closes but the other keeps it open."

       [ clientDone , serverDone ] <- replicateM 2 newEmptyMVar
       testUnidirectionalError clientDone serverDone
       mapM_ readMVar [  clientDone , serverDone ]
  

   
     return ()
  


--This test is bound to fail because of lack of implementaion of KeepAliveTimeOut in CCI. The server closes the connection after client disconnects.
testEarlyDisconnect :: MVar () -> MVar () -> IO ()
testEarlyDisconnect clientDone serverDone  = catch go handler where 
       handler :: SomeException -> IO ()
       handler e = putStrLn ( show e ) >> putMVar clientDone () >> putMVar serverDone () >> throw e  

       go = do 
          initCCI
          serverAddress <- newEmptyMVar 

          --start server
          forkIO ( (  do 
             endpoint  <- createPollingEndpoint Nothing 
             getEndpt_URI endpoint >>= \addr -> putMVar serverAddress addr >> putStrLn addr 

             --Connection request from Client. Accept this connection.
             pollWithEventData endpoint $ \ev -> 
               case ev  of 
                  EvConnectRequest sev eb attr  -> accept sev ( 0 :: WordPtr ) 
                  _ -> fail "Some thing wrong with connection"

             pollWithEventData endpoint $ \ev ->
                   case ev of 
                        EvAccept _ _ -> return ()
                        _ -> fail "Error in connection acception sequence"

             -- client send message. receive it and send it but client has closed its connection so sever will not be able to send the reply. It will show 
             -- Timeout in EvSend status. Currently keepalivetimeout is not implement otherwise application has to take care of connection either reconnect or close 
             -- connection.             
             pollWithEventData endpoint $ \ev ->
                   case ev of 
                        EvRecv eventbytes  connection  -> do
                           msg <- packEventBytes eventbytes
                           BS.putStrLn msg
                           send connection ( BS.pack "Hi Client. I am not able to send you reply :(" ) ( 0 :: WordPtr )  
                        _ -> fail "Something wrong with this connection"

             -- send will fail to deliver the message 
             pollWithEventData endpoint $ \ev ->
                   case ev of 
                        EvSend _ st  _ -> putStrLn . show $ st
                        _ -> fail "Something wrong with this connection"

             --after fair amount of time KeepAliveTimeOut will occur. Currently not implemented in CCI.
             {--pollWithEventData endpoint $ \ev ->
                   case ev of 
                        EvKeepAliveTimedOut connection -> do 
                           disconnect connection
                        _ -> fail "Something wrong with this connection"
             --} 
             --threadDelay 100000000
             ) `finally` putMVar serverDone ()  )       

          --start client
          forkIO ( (  do 
             endpoint  <- createPollingEndpoint Nothing
             --connect  to server
             addr <- readMVar serverAddress 
             connect endpoint addr BS.empty CONN_ATTR_RO ( 0 :: WordPtr ) Nothing 
             --server accepted the connection
             newconnMVar  <- newEmptyMVar 
             pollWithEventData endpoint $ \ev ->
                   case ev of 
                        EvConnect cid ( Right conn ) ->  putMVar newconnMVar conn 
                        _ -> fail "Something wrong with client"

             -- send something over this connection 
             conn <- readMVar newconnMVar  
             send conn ( BS.pack "Hi Server. You won't be able to send me reply :)" ) ( 0 :: WordPtr ) 
             -- close this connection
             disconnect conn
             --destroy endpoint
             destroyEndpoint endpoint 
             --threadDelay 100000000
             ) `finally`  putMVar clientDone () )

          return ()

--  Test what happens when a remote endpoint sends a connection request to our transport for an endpoint it already has a connection to. TCP acceptes connection from one 
--  client and if same client sends the request then it issue ConnectionRequestInvalid and other error message. TCP server accepts any connection while CCI server grants the
--  permission to client to get accepted. If same client is sending 10 request then accept one and reject other ( I am bit skeptical about how does the internals of CCI behave 
--  on multiple connection.) 

testUnnecessaryConnectConcurrent :: MVar () -> MVar () -> IO ()
testUnnecessaryConnectConcurrent clientDone serverDone = catch go handler where

       handler :: SomeException -> IO ()
       handler e = putStrLn ( show e ) >> throw e

       go = do
           initCCI
           serverAddress <-  newEmptyMVar 
       

           forkIO  ( ( do 
              endpoint  <- createPollingEndpoint Nothing
              getEndpt_URI endpoint >>= \addr -> putMVar serverAddress addr >> putStrLn addr 
              connAccepted <- newEmptyMVar              

             {--            
              --pollWithEventData will wait until events becomes available. It will always looking for events and block until events available            
              let loop = do 
                      pollWithEventData endpoint $ \ev -> do 
                           case ev of
                               EvConnectRequest sev eb attr -> do 
                                         mv <- tryTakeMVar connAccepted 
                                         case mv of 
                                            Nothing -> do 
                                                   accept sev ( 0 :: WordPtr ) 
                                                   --putStrLn "Accepting connection"                                   
                                            Just _ -> do 
                                                   reject sev 
                                                   --putStrLn "Rejecting Connection"
                                         putMVar connAccepted ()
                               EvRecv eventbytes  connection  -> do
                                          msg <- packEventBytes eventbytes
                                          BS.putStrLn msg
                               _ -> return () 
                   
              --}
              --for multiple clients throwing connection to server use getEvent
              let loop = do 
                    evnt <- getEvent endpoint 
                    case evnt of 
                          Nothing -> return () 
                          Just s -> do 
                               ev <- getEventData s
                               case ev of 
                                 EvConnectRequest sev eb attr -> do 
                                         mv <- tryTakeMVar connAccepted 
                                         case mv of 
                                            Nothing -> do 
                                                   accept sev ( 0 :: WordPtr ) 
                                                   --putStrLn "Accepting Connection"                  
                                            Just _ -> do 
                                                   reject sev 
                                                   --putStrLn "Rejecting Connection"
                                         putMVar connAccepted ()
                                 EvRecv eventbytes  connection  -> do
                                          msg <- packEventBytes eventbytes
                                          BS.putStrLn msg
                                 _  -> return () 
                    

              replicateM_ 9000000 loop
              return () 

                      ) `finally` putMVar serverDone () )
           
           forkIO ( ( do
                  
                    tot <- replicateM 10 newEmptyMVar
                    addr <- readMVar serverAddress
                    --endpoint <- createPollingEndpoint Nothing --uncomment this 
                    forM_ tot ( forkIO . \var ->  do 
                                     
                                    endpoint <- createPollingEndpoint Nothing -- comment this
                                    connect endpoint addr BS.empty CONN_ATTR_RO ( 0 :: WordPtr ) Nothing
                                    newconnMVar  <- newEmptyMVar 
                                    pollWithEventData endpoint $ \ev -> do 
                                           case ev of
                                              EvConnect cid ( Right conn ) ->  putMVar newconnMVar conn
                                              _ -> return () 
                                    mv <- tryTakeMVar newconnMVar 
                                    case mv of 
                                             Just conn -> do 
                                               id <- myThreadId
                                               send  conn ( BS.pack $ "Hi Server. My thread id is " ++ show id ) ( 0 :: WordPtr )
                                               disconnect conn 
                                             Nothing -> return ()          
                                           
                                    destroyEndpoint endpoint
                                    putMVar var () 
                               ) 
                    mapM_ readMVar   tot 
                    --forM_ tot $ \var -> ( traceIO . show =<< isEmptyMVar var )
                       ) `finally` putMVar clientDone () )
           return () 


testUnnecessaryConnectSequential  :: MVar () -> MVar () -> IO () 
testUnnecessaryConnectSequential  clientDone serverDone = catch go handler where 
       handler :: SomeException -> IO ()
       handler e = putStrLn ( show e ) >> throw e

       go = do 
               initCCI 
               serverAddress <- newEmptyMVar
               --start server
               
               forkIO ( ( do 
                    endpoint  <- createPollingEndpoint Nothing
                    getEndpt_URI endpoint >>= \addr -> putMVar serverAddress addr >> putStrLn addr
                    connAccepted <- newEmptyMVar    
                    {--
                    let loop = do
                         pollWithEventData endpoint $ \ev -> do
                           case ev of
                               EvConnectRequest sev eb attr -> do
                                         mv <- tryTakeMVar connAccepted
                                         case mv of
                                            Nothing -> do
                                                   accept sev ( 0 :: WordPtr )
                                                   --putStrLn "Accepting connection"                                    
                                            Just _ -> do
                                                   reject sev
                                                   --putStrLn "Rejecting Connection"
                                         putMVar connAccepted ()
                               EvRecv eventbytes  connection  -> do
                                          msg <- packEventBytes eventbytes
                                          BS.putStrLn msg
                               _ -> return ()
                    --}
                    let loop = do
                          evnt <- getEvent endpoint
                          case evnt of
                            Nothing -> return ()
                            Just s -> do
                               ev <- getEventData s
                               case ev of
                                 EvConnectRequest sev eb attr -> do
                                         mv <- tryTakeMVar connAccepted
                                         case mv of
                                            Nothing -> do
                                                   accept sev ( 0 :: WordPtr )
                                                   --putStrLn "Accepting Connection"                   
                                            Just _ -> do
                                                   reject sev
                                                   --putStrLn "Rejecting Connection"
                                         putMVar connAccepted ()
                                 EvRecv eventbytes  connection  -> do
                                          msg <- packEventBytes eventbytes
                                          BS.putStrLn msg
                                 _  -> return ()

                    replicateM_ 9000000 loop
                    return ()
        
                        ) `finally` putMVar serverDone () )

               --start the client
               forkIO ( ( do 
       
                    
                    addr <- readMVar serverAddress
                    connAccepted <- newEmptyMVar 
                    replicateM_ 10 $ do
                                    done <- newEmptyMVar 
                                    forkIO $ do 
                                       endpoint <- createPollingEndpoint Nothing 
                                       connect endpoint addr BS.empty CONN_ATTR_RO ( 0 :: WordPtr ) Nothing
                 
                                       pollWithEventData endpoint $ \ev -> do
                                           case ev of
                                              EvConnect cid ( Right conn ) ->  putMVar connAccepted  conn
                                              _ -> return ()
                                       mv <- tryTakeMVar connAccepted
                                       case mv of
                                             Just conn -> do
                                               id <- myThreadId
                                               send  conn ( BS.pack $ "Hi Server. My thread id is " ++ show id ) ( 0 :: WordPtr )
                                               disconnect conn
                                             Nothing -> return ()
                       
                                       destroyEndpoint endpoint
                                       putMVar done ()
                                    readMVar  done 
                              


                        ) `finally` putMVar clientDone () )
               return () 
   

--Test that we can create "many" transport instances.  It will throw error if not created.
testMany :: IO () 
testMany  = catch go handler  where 

       handler :: SomeException -> IO ()
       handler e = putStrLn ( show e ) >> throw e


       go = do 
          initCCI
          Right masterTransport <- createTransport  defaultCCIParameters 
          Right masterEndPoint <- N.newEndPoint masterTransport 

          replicateM_ 10 $  do
                 done <- newEmptyMVar 
                 forkIO $ do 
                   mTransport <- createTransport defaultCCIParameters
                   case mTransport of
                       Left ex -> do
                            putStrLn $ "Error in transport creation " ++ show ex
                            putMVar done () 
                            throw ex
                       Right transport ->  do 
                            Right endpoint <- N.newEndPoint transport
                            Right _        <- N.connect endpoint (N.address masterEndPoint) N.ReliableOrdered N.defaultConnectHints
                            return ()
                   putMVar done () 
                 readMVar done


--This test case is specific to TCP.Send 
testReconnect :: MVar () -> MVar () -> IO ()  
testReconnect clientDone serverDone = catch go handler where 
   
       handler :: SomeException -> IO ()
       handler e = putStrLn ( show e ) >> throw e

       go = do 
             initCCI
             serverAddress <- newEmptyMVar 
             ---start server 
             forkIO $ ( ( do 
                endpoint <- createPollingEndpoint Nothing 
                getEndpt_URI endpoint >>= \addr -> putMVar serverAddress addr >> putStrLn addr
 
                --connection request from client. reject this. 
                pollWithEventData endpoint $ \ev ->
                             case ev  of
                                 EvConnectRequest sev eb attr  ->   reject  sev
                                 _ -> fail "Some thing wrong with connection"
 
                --client send again the request. Accept this time
                pollWithEventData endpoint $ \ev ->
                             case ev  of
                                 EvConnectRequest sev eb attr  ->   accept sev ( 0 :: WordPtr )
                                 _ -> fail "Some thing wrong with connection"

                pollWithEventData endpoint $ \ev ->
                             case ev of
                                 EvAccept cid  _ -> return ()
                                 _ -> fail "Error in connection acception sequence"

                pollWithEventData endpoint $ \ev ->
                             case ev of
                                 EvRecv eventbytes  connection  -> do
                                        msg <- packEventBytes eventbytes
                                        BS.putStrLn msg
                                 _ -> fail "Something wrong with this connection"

                          ) `finally` putMVar serverDone () ) 

             --start client 
             forkIO $ ( ( do 
                  
                addr <- readMVar serverAddress
                endpoint <- createPollingEndpoint Nothing
                --server rejected this connection 
                connect endpoint addr BS.empty CONN_ATTR_RO ( 0 :: WordPtr ) Nothing                  
                pollWithEventData endpoint $ \ev -> putStrLn .  show $ ev
                --send again and this time it accepts again
                connect endpoint addr BS.empty CONN_ATTR_RO ( 0 :: WordPtr ) Nothing
                newconnMVar  <- newEmptyMVar
                pollWithEventData endpoint $ \ev ->
                       case ev of
                          EvConnect cid ( Right conn ) ->  putMVar newconnMVar conn
                          _ -> fail "Something wrong with client"

                id <- myThreadId
                conn <- readMVar newconnMVar
                send  conn ( BS.pack $ "Hi Server. My thread id is " ++ show id ) ( 0 :: WordPtr )
                disconnect conn
                destroyEndpoint endpoint

                        ) `finally` putMVar clientDone () )
             return () 


--Closing the network layer in both client and server thread and they are still able to send and receive message. Currently a known bug in a the CCI/CH layer.
testUnidirectionalError :: MVar () -> MVar () -> IO () 
testUnidirectionalError clientDone serverDone = catch go handler where 

       handler :: SomeException -> IO ()
       handler e = putStrLn ( show e ) >> throw e

       go = do 
          initCCI
          Right transport <- createTransport defaultCCIParameters
          [ serverAddress , clientAddress ] <- replicateM 2 newEmptyMVar 
           
          --start server 
          forkIO ( ( do 
                 Right endpoint <- N.newEndPoint transport 
                 putMVar  serverAddress ( N.address endpoint )
                 putStrLn $ "server address " ++ show ( N.address endpoint )  
                 --client has sent a connection request 
                 N.ConnectionOpened cid _ addr <- N.receive endpoint
                 --receive message  from client
                 N.Received cid' msg  <- N.receive endpoint 
                 putStrLn . show $ msg 
                 --close transport and see if client is still able to send the message
                 --N.closeTransport transport 
                 N.Received _ msg' <- N.receive endpoint
                 putStrLn . show $ msg'
                 True <- return $  msg == msg'
                 return ()
                   ) `finally` putMVar serverDone () )


          --start client
          forkIO ( ( do 
                 Right endpoint <- N.newEndPoint transport 
                 putMVar clientAddress  ( N.address endpoint ) 
                 putStrLn  $ "Client address " ++ show ( N.address endpoint ) 
                 -- connect to server 
                 Right conn <- do addr <- readMVar serverAddress
                                  N.connect endpoint addr N.ReliableOrdered N.defaultConnectHints
                 --send message to server 
                 N.send conn [ BS.pack "Hi Server!" ]
                 --close transport and see if client is able to send the message. 
                 --N.closeTransport transport
                 N.send conn [ BS.pack "Hi Server!" ]
                 return ()
                   ) `finally` putMVar clientDone () )

          return ()
