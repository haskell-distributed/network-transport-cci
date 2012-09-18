
import Network.CCI
import Control.Exception
import Prelude hiding ( catch )
import qualified Data.ByteString.Char8 as BS
import qualified Data.Map as M
import Control.Concurrent
import Control.Monad
import System.Environment



main = do
     firstTestCase <- do 
        putStrLn "First Test case" 
        [ clientDone , serverDone ] <- replicateM 2 newEmptyMVar
        testEarlyDisconnect clientDone serverDone
        readMVar clientDone
        readMVar serverDone
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
                          forkIO $ do
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
