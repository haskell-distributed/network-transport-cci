module Main where

import Network.CCI
import Control.Exception
import qualified Data.ByteString.Char8 as BS
import Data.ByteString (ByteString)
import qualified Data.Map as Map
import Control.Concurrent
import Prelude hiding (catch)
import Control.Monad (replicateM_)
import System.Environment (getArgs)

import Debug.Trace

msgLoop :: MyEndpoint -> IO ()
msgLoop ep = catch (loop Map.empty 0) handler
  where handler :: SomeException -> IO ()
        handler e = putStrLn ("msgLoop exception: "++show e) >> throw e
        loop conns nextid = do
           mv <- tryTakeMVar (meDone ep)
           case mv of
             Just _ -> putMVar (meDone1 ep) ()
             Nothing -> do
               (newconns,newnextid) <- tryWithEventData (meEndpoint ep) (return (conns,nextid))$ \ev ->
                 case ev of
                   EvSend _ctx _status _cnn -> 
                      return (conns,nextid)
                   EvRecv eb _conn ->
                     do msg <- packEventBytes eb
                        BS.putStrLn $ BS.concat [ BS.pack "Recv: ", msg ]
                        return (conns,nextid)
                   EvConnect cid (Left conn) ->
                     do putMVar (meConnection ep) Nothing
                        return (conns,nextid)
                   EvConnect cid (Right conn) ->
                     do trace "Connected" $ putMVar (meConnection ep) (Just conn)
                        return (conns,nextid)
                   EvAccept cid (Right conn) ->
                       let newmap = Map.insert conn cid conns 
                        in return (newmap,nextid)
                   EvConnectRequest sev eb attr ->
                     do accept sev (toEnum nextid)
                        return (conns,nextid+1)
                   EvKeepAliveTimedOut _conn ->
                        trace "Got keepalive" $ return (conns,nextid)
                   EvEndpointDeviceFailed _endp ->
                        return (conns,nextid)
                   _ -> do
                           putStrLn $ "Horrible error: " ++ show ev
                           return (conns,nextid)
               loop newconns newnextid 

data MyEndpoint = MyEndpoint 
    { meEndpoint :: Endpoint
    , meConnection :: MVar (Maybe Connection)
    , meDone :: MVar ()
    , meDone1 :: MVar ()
    }

makeEndpoint :: IO MyEndpoint
makeEndpoint =
  do endp <- createPollingEndpoint Nothing
     mcon <- newEmptyMVar
     mdone <- newEmptyMVar
     mdone1 <- newEmptyMVar
     let me = MyEndpoint endp mcon mdone mdone1
     forkIO $ msgLoop me
     return me

connectTo :: MyEndpoint -> String -> IO Connection
connectTo ep1 ep2 =
  do 
     connect (meEndpoint ep1) ep2 BS.empty CONN_ATTR_RO (0::WordPtr) Nothing
     takeMVar (meConnection ep1) >>= maybe (error "Connection rejected") return

sendStuff :: Connection -> ByteString -> IO ()
sendStuff conn bs = send conn bs (0::WordPtr)

main :: IO ()
main = withCCI go
   where go = do args <- getArgs
                 case args of
                   [] -> do ep <- makeEndpoint
                            getEndpt_URI (meEndpoint ep) >>= putStrLn
                            threadDelay 500000000
                   [addr] -> do ep <- makeEndpoint
                                conn <- connectTo ep addr
                                replicateM_ 500 $ (sendStuff conn (BS.pack "Hi there"))
                                threadDelay 5000000
                                putMVar (meDone ep) ()
                                takeMVar (meDone1 ep)
                                putStrLn "Disconnecting..."
                                disconnect conn
                                putStrLn "Destroying endpoint..."
                                destroyEndpoint (meEndpoint ep)
         handler :: SomeException -> IO ()
         handler e = putStrLn ("main exception: "++show e) >> throw e

