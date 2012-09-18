
import Network.CCI
import Control.Exception
import Prelude hiding ( catch )
import qualified Data.ByteString.Char8 as BS
import qualified Data.Map as M
import Control.Concurrent
import Control.Monad
import System.Environment



main = do 
     testEarlyDisconnect 
     return ()



--This test is bound to fail because of lack of implementaion of KeepAliveTimeOut in CCI. The server closes the connection after client disconnects.
testEarlyDisconnect :: IO ()
testEarlyDisconnect = do 
          initCCI
          [ clientDone , serverDone ] <- replicateM 2 newEmptyMVar 
          serverAddress <- newEmptyMVar 
          --start server
          forkIO $  do 
               
             ( endpoint , _ ) <- createBlockingEndpoint Nothing 
             getEndpt_URI endpoint >>= \addr -> putMVar serverAddress addr >> putStrLn addr 
             ev_1  <-  getEvent endpoint >>= \( Just s ) -> getEventData s 
             case ev_1  of 
                  EvConnectRequest sev eb attr  -> accept sev ( 0 :: WordPtr ) 
                  _ -> fail "Some thing wrong with connection"

             EvAccept _ _  <- getEvent endpoint >>= \( Just s ) -> getEventData s 

             -- client send message. receive it and send it but client has closed its connection so sever will not be able to send the reply. It will show 
             -- Timeout in EvSend status. Currently keepalivetimeout is not implement otherwise application has to take care of connection either reconnect or close 
             -- connection.             
             ev_2 <- getEvent endpoint >>= \( Just s ) -> getEventData s 
             case ev_2 of 
                EvRecv eventbytes  connection  -> do 
                       msg <- packEventBytes eventbytes
                       BS.putStrLn msg
                       send connection ( BS.pack "Hi Client. I am not able to send you reply :(" ) ( 0 :: WordPtr )  
                _ -> fail "Something wrong with this connection"

             -- send will fail to deliver the message 
             EvSend _ _ _ <- getEvent endpoint >>= \( Just s ) -> getEventData s

             --after fair amount of time KeepAliveTimeOut will occur. Currently not implemented in CCI.
             {--ev_3 <- getEvent endpoint >>= \( Just s ) -> getEventData s
             case ev_3 of 
                    EvKeepAliveTimedOut connection -> do 
                           disconnect connection
                    _ -> fail "Something wrong with this connection"
             --} 
             putMVar serverDone ()         

          --start client
          forkIO $  do 
             ( endpoint , _ ) <- createBlockingEndpoint Nothing
             --connect  to server
             addr <- readMVar serverAddress 
             connect endpoint addr BS.empty CONN_ATTR_RO ( 0 :: WordPtr ) Nothing 
             --server accepted the connection
             newconnMVar  <- newEmptyMVar 
             ev_1  <-  getEvent endpoint >>= \( Just s ) -> getEventData s
             case ev_1 of 
                     EvConnect cid ( Right conn ) ->  putMVar newconnMVar conn 
                     _ -> fail "Something wrong with client"
             -- send something over this connection 
             conn <- readMVar newconnMVar  
             send conn ( BS.pack "Hi Server. Yo won't be able to send me reply :)" ) ( 0 :: WordPtr ) 
             -- close this connection
             disconnect conn
             --destroy endpoint
             destroyEndpoint endpoint 
             putMVar clientDone ()

          readMVar serverDone 
          readMVar clientDone 
