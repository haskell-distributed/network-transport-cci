-- |CCI.hs
--
-- Copyright (C) 2012 Parallel Scientific, Inc
-- Please see the accompanying LICENSE file or contact Parallel Scientific, Inc for licensing details.
--
-- Cloud Haskell backend supporting CCI, via CCI bindings
module Network.Transport.CCI 
  (
     createTransport,
     CCIParameters(..),
     defaultCCIParameters,
     ReceiveStrategy(..)
  ) where

import Control.Monad (liftM)
import Control.Concurrent.Chan
import Control.Concurrent (forkIO, ThreadId, killThread)
import Control.Concurrent.MVar
import Control.Exception (catch, try, SomeException, throwIO, Exception)
import Data.Binary (Binary,put,get,getWord8, putWord8, encode,decode)
import Data.ByteString (ByteString)
import Data.Char (chr,ord)
import qualified Data.List as List (lookup)
import Data.Typeable (Typeable)
import Data.Word (Word32, Word64)
import Network.CCI (WordPtr)
import Network.Transport.Internal (timeoutMaybe)
import Network.Transport (EventErrorCode(..), Transport(..), TransportError(..), NewEndPointErrorCode(..), EndPointAddress(..), Event(..), TransportError, ConnectErrorCode(..), EndPoint(..), SendErrorCode(..), NewMulticastGroupErrorCode(..), ResolveMulticastGroupErrorCode(..), Reliability(..), ConnectHints(..), Connection(..), ConnectionId)
import Prelude hiding (catch)
import qualified Data.ByteString.Char8 as BSC (head, tail, singleton,pack, unpack, empty, length)
import qualified Data.ByteString.Lazy as BSL (toChunks,fromChunks)
import qualified Data.Map as Map
import qualified Network.CCI as CCI (accept, reject, createPollingEndpoint, createBlockingEndpoint, Endpoint, Device, initCCI, finalizeCCI, CCIException(..), endpointURI, destroyEndpoint, connect, ConnectionAttributes(..), EventData(..), packEventBytes, Connection, pollWithEventData, withEventData, connectionMaxSendSize, sendv, SEND_FLAG(..), disconnect, EndpointOption(..), setEndpointOpt)
import System.Posix.Types (Fd)

import Debug.Trace

-- TODO: carefully read tests from distributed-process and network-transport-tcp, implement here
-- TODO: play with keepalive timeout, extend CCIParameters to support other endpoint options
-- TODO: use CCI.strError to display better exceptions
-- TODO: use Transport.ErrorEvent (esp. ConnectionLost) in response to bogus requests; throw $ userError when shutting down transport or endpoint
-- TODO: handle unexpected termination eventloop by closing transport and reporting error to CH

data CCIParameters = CCIParameters {
        -- | How long to wait, in microseconds, before deciding that an attempt
        -- to connect to a nonresponsive foreign host has failed
        cciConnectionTimeout :: Maybe Word64,

        -- | The mechanism by which messages are retrieved from the CCI queue
        -- Polling (the default) will probably result in better performance but
        -- may be wasteful
        cciReceiveStrategy :: ReceiveStrategy, 

        -- | Which device to use. Nothing (the default) means use the default device. 
        -- Other values can be retrieved from Network.CCI.getDevcices
        cciDevice :: Maybe CCI.Device
     } --TODO add an authorization key to be sent during connecting

data ReceiveStrategy = 
        AlwaysPoll
      | AlwaysBlock

data CCITransportState = 
     CCITransportStateValid |
     CCITransportStateClosed

data CCITransport = CCITransport {
        cciParameters :: CCIParameters,
        cciTransportState :: MVar CCITransportState
     }


-- | Default values to use with 'createTransport'
defaultCCIParameters :: CCIParameters 
defaultCCIParameters = CCIParameters {
        cciConnectionTimeout = Just 10000000, -- ^ 10 second connection timeout
        cciReceiveStrategy =  AlwaysPoll,
        cciDevice = Nothing -- ^ default device (as determined by driver)
     }

data CCIEndpoint = CCIEndpoint
   {
      cciFileDescriptor :: Fd,
      cciEndpoint :: CCI.Endpoint,
      cciChannel :: Chan Event,
      cciUri :: String,
      cciTransportEndpoint :: EndPoint,
      cciEndpointState :: MVar CCIEndpointState,
      cciEndpointFinalized :: MVar ()
   }

data CCIEndpointState = CCIEndpointValid {
                          cciNextConnectionId :: !ConnectionId,
                          cciEndpointThread :: ThreadId,
                          cciConnectionsById :: Map.Map ConnectionId CCIConnection -- TODO change to IntMap. why not?
                        } | CCIEndpointClosed

data CCIConnection = CCIConnection
   {
      cciConnectionState :: MVar CCIConnectionState,
      cciReady :: MVar (Maybe (TransportError ConnectErrorCode)),
      cciConnectionId :: ConnectionId
   }

data CCIConnectionState = 
      CCIConnectionInit |
      CCIConnectionConnected
          { cciConnection :: CCI.Connection,
            cciMaxSendSize :: Word32
          } |
      CCIConnectionClosed
          deriving (Show)

-- | This is used as an argument to Network.Transport.TransportError
data CCIErrorCode = 
       CCICreateTransportFailed |
       CCIDisconnectFailed 
       deriving (Show, Typeable, Eq)

-- | In addition to regular message (which will be passed on to CH and
-- eventually to a user process), we can send control messages, which
-- will be handled by the CCI event handler.
data ControlMessage = 
          ControlMessageCloseConnection -- ^ Request that the given connection terminate (and notify CH)
        | ControlMessageInitConnection Reliability EndPointAddress -- ^ Finish initializaion of the connection (and notify CH)
          deriving (Typeable)

-- This really belongs in Network.Transport
instance Binary Reliability where
   put ReliableOrdered = putWord8 0
   put ReliableUnordered  = putWord8 1
   put Unreliable = putWord8 2
   get = do hdr <- getWord8
            case hdr of
               0 -> return ReliableOrdered
               1 -> return ReliableUnordered
               2 -> return Unreliable
               _ -> fail "Reliability.get: invalid"

instance Binary ControlMessage where
  put ControlMessageCloseConnection = putWord8 0
  put (ControlMessageInitConnection r ep) = putWord8 1 >> put r >> put ep
  get = do hdr <- getWord8
           case hdr of
             0 -> return ControlMessageCloseConnection
             1 -> do r <- get
                     ep <- get
                     return $ ControlMessageInitConnection r ep
             _ -> fail "ControlMessage.get: invalid" 

getEndpointAddress :: CCIEndpoint -> EndPointAddress
getEndpointAddress ep = EndPointAddress $ BSC.pack (cciUri ep)

waitReady :: CCIConnection -> IO ()
waitReady conn = 
  takeMVar (cciReady conn)
       >>= \rdy -> 
         case rdy of
            Nothing -> return ()
            Just err -> throwIO err

-- | Create a transport object suitable for using with CH.
createTransport :: CCIParameters -> IO (Either (TransportError CCIErrorCode) Transport)
createTransport params = 
  try $ mapCCIException (translateException CCICreateTransportFailed) $
        do CCI.initCCI
           state <- newMVar $ CCITransportStateValid
           return (mkTransport (mkInternalTransport state))
    where 
         mkInternalTransport st = CCITransport {
             cciParameters = params,
             cciTransportState = st
         }
         mkTransport inter = 
             Transport {
                newEndPoint = apiNewEndPoint inter,
                closeTransport = apiCloseTransport inter
             }

apiCloseTransport :: CCITransport -> IO ()
apiCloseTransport transport = 
   modifyMVar_ (cciTransportState transport) $ \st ->
        case st of
          CCITransportStateClosed ->
                  return CCITransportStateClosed
          _ -> do CCI.finalizeCCI
                  return CCITransportStateClosed

apiNewEndPoint :: CCITransport -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint transport = 
  try $ mapCCIException (translateException NewEndPointFailed) $
    do (endpoint, fd) <- makeEndpoint  (cciDevice (cciParameters transport))
       CCI.setEndpointOpt endpoint CCI.OPT_ENDPT_KEEPALIVE_TIMEOUT 5000000 -- TODO WTF?
       uri <- CCI.endpointURI endpoint
       chan <- newChan
       thrdsem <- newEmptyMVar
       thrd <- forkIO (endpointHandler thrdsem)
       finalized <- newEmptyMVar
       withMVar (cciTransportState transport) $ \st ->
          case st of
            CCITransportStateValid ->
               do endpstate <- newMVar $ 
                        CCIEndpointValid {
                            cciNextConnectionId = 0,
                            cciEndpointThread = thrd,
                            cciConnectionsById = Map.empty }
                  let myEndpoint = CCIEndpoint {
                        cciFileDescriptor = fd,
                        cciEndpoint = endpoint,
                        cciChannel = chan,
                        cciUri = uri,
                        cciTransportEndpoint = transportEndPoint,
                        cciEndpointState = endpstate,
                        cciEndpointFinalized = finalized }
                      transportEndPoint = EndPoint {
                        receive = readChan chan,
                        address = EndPointAddress $ BSC.pack uri,
                        connect = apiConnect transport myEndpoint,
                        closeEndPoint = apiCloseEndPoint transport myEndpoint,
                        newMulticastGroup = return . Left $ newMulticastGroupError,
                        resolveMulticastGroup = return . Left . const resolveMulticastGroupError }
                  putMVar thrdsem (transport,myEndpoint)
                  return transportEndPoint
            _ -> throwIO (TransportError NewEndPointFailed "Transport closed")
  where
    makeEndpoint dv =
       case cciReceiveStrategy (cciParameters transport) of
         AlwaysPoll -> 
           liftM (flip(,)undefined) (CCI.createPollingEndpoint dv)
         AlwaysBlock ->
           CCI.createBlockingEndpoint dv
    newMulticastGroupError = 
      TransportError NewMulticastGroupUnsupported "Multicast not supported" 
    resolveMulticastGroupError = 
      TransportError ResolveMulticastGroupUnsupported "Multicast not supported" 

dbgEither :: Show a => Either a b -> IO (Either a b)
dbgEither ret@(Left err) = dbg err >> return ret
dbgEither ret = return ret

dbg :: Show a => a -> IO ()
dbg = print

endpointHandler :: MVar (CCITransport, CCIEndpoint) -> IO ()
endpointHandler mv = 
   do (transport, endpoint) <- takeMVar mv
      catch (go transport endpoint) exceptionHandler
  where exceptionHandler :: SomeException -> IO ()
        exceptionHandler e = dbg $ "Exception in endpointHandler: "++show e
        go transport endpoint = endpointLoop transport endpoint

endpointLoop :: CCITransport -> CCIEndpoint -> IO ()
endpointLoop transport endpoint =
 let loop :: Map.Map CCI.Connection ConnectionId -> ConnectionId -> IO ()
     loop !connectionsByConnection !nextConnectionId = 
        do ret <- receiveEvent transport endpoint $ \ev -> 
                case ev of

-- | The connection packet will be blank, in which case this is a real
-- connection request, orit can contain magicEndpointShutdown,
-- which we interpret as an attempt to shutdown the endpoint.
-- in other cases the connection is rejected.
                  CCI.EvConnectRequest sev eb _attr ->
                      do msg <- CCI.packEventBytes eb
                         case BSC.unpack msg of
                            [] -> do CCI.accept sev (toEnum nextConnectionId)
                                     return $ Just (connectionsByConnection,nextConnectionId+1)
                            shutdown | shutdown == magicEndpointShutdown ->
                                  do CCI.reject sev
                                     return Nothing 
                            _ ->  do dbg "Unknown connection magic word"
                                     CCI.reject sev
                                     return $ Just (connectionsByConnection,nextConnectionId)
                  CCI.EvSend _ctx _status _conn ->
                      return $ Just (connectionsByConnection, nextConnectionId)
                  CCI.EvRecv eb conn -> 
                      let mconnid = Map.lookup conn connectionsByConnection
                       in do case mconnid of
                               Nothing -> do dbg $ "Unknown connection in EvRecv: " ++ show conn
                                             return $ Just (connectionsByConnection,nextConnectionId)
                               Just connid -> do msg <- CCI.packEventBytes eb
                                                 case ord $ BSC.head msg of
                                                    0 -> do putEvent endpoint (Received connid [BSC.tail msg]) -- normal message
                                                            return $ Just (connectionsByConnection,nextConnectionId)
                                                    _ -> case decode $ BSL.fromChunks [BSC.tail msg] of
                                                            ControlMessageInitConnection rel epa -> 
                                                               do putEvent endpoint (ConnectionOpened connid rel epa)
                                                                  return $ Just (connectionsByConnection, nextConnectionId)
                                                            ControlMessageCloseConnection ->
                                                               do CCI.disconnect conn
                                                                  putEvent endpoint (ConnectionClosed connid)
                                                                  return $ Just (Map.delete conn connectionsByConnection, nextConnectionId)
                  CCI.EvConnect connectionId (Left failure) ->
-- TODO: change connection state to error/invalid/closed
                      do modifyMVar_ (cciEndpointState endpoint) $ \st ->
                           case st of
                             CCIEndpointValid {cciConnectionsById = connections} ->
                               let theconn = Map.lookup (fromEnum connectionId) connections 
                                   newmap = Map.delete (fromEnum connectionId) connections 
                                in maybe (return ()) (\tc -> putMVar (cciReady tc) 
                                       (Just $ TransportError ConnectNotFound (show failure))) theconn >>
                                   return (st {cciConnectionsById = newmap})
                             _ -> return (st)
                         dbg $ "Connection " ++ show connectionId ++ " failed with " ++ show failure
                         return $ Just (connectionsByConnection, nextConnectionId)
                  CCI.EvConnect connectionId (Right conn) ->
                      do withMVar (cciEndpointState endpoint) $ \st ->
                           case st of
                             CCIEndpointValid {cciConnectionsById = connectionsById} ->
                               do case Map.lookup (fromEnum connectionId) connectionsById of
                                    Nothing -> dbg $ "Unknown connection ID: "++show connectionId
                                    Just cciconn ->
                                      modifyMVar (cciConnectionState cciconn) $ \connstate ->
                                        case connstate of
                                          CCIConnectionInit ->
                                            do maxmsg <- CCI.connectionMaxSendSize conn
                                               let newconnstate = CCIConnectionConnected 
                                                     {cciConnection = conn, 
                                                      cciMaxSendSize = maxmsg}
                                               putMVar (cciReady cciconn) Nothing
                                               return (newconnstate, ())
                                          _ -> do dbg $ "Unexpected EvConnect for connection " ++ 
                                                    show connectionId ++ " in state " ++ show connstate
                                                  return (connstate, ())
                             _ -> dbg "Can't handle EvConnect when endpoint is closed"
                         return $ Just (connectionsByConnection, nextConnectionId)
                  CCI.EvAccept connectionId (Left status) ->
                      do dbg ("Failed EvAccept on connection " ++ show connectionId ++ " because " ++ show status)
                         return $ Just (connectionsByConnection,nextConnectionId)
                  CCI.EvAccept connectionId (Right conn) -> 
                      -- The connection is not fully ready until it gets an EvAccept. Therefore,
                      -- there is a possible race condition if the other (originating) side receives
                      -- its EvConnect and tries to send a message before this (target) side is ready.
                      let newmap = Map.insert conn (fromEnum connectionId) connectionsByConnection
                       in return $ Just (newmap, nextConnectionId)
                  CCI.EvKeepAliveTimedOut _conn ->
                     do dbg $ show ev
                        return $ Just (connectionsByConnection, nextConnectionId)
                  CCI.EvEndpointDeviceFailed _endp ->
                     do dbg $ show ev
                        return $ Just (connectionsByConnection, nextConnectionId)
           case ret of
              Nothing -> do -- TODO destroyEndpoint seems to lock up
                            --CCI.destroyEndpoint (cciEndpoint endpoint) 
                             putMVar (cciEndpointFinalized endpoint) ()
              Just (conns,next) -> loop conns next
  in loop Map.empty 0 
        
-- | Notify CH that something happened. Usually, a connection was opened or closed or a message was received.
putEvent :: CCIEndpoint -> Event -> IO ()
putEvent endp ev = writeChan (cciChannel endp) ev

putErrorEvent :: CCIEndpoint -> Maybe EndPointAddress -> [ConnectionId] -> String -> IO ()
putErrorEvent endpoint mepa lcid why =
   let err = EventConnectionLost mepa lcid
    in putEvent endpoint (ErrorEvent (TransportError err why))

magicEndpointShutdown :: String
magicEndpointShutdown = "shutdown"

-- | First we shutdown individual connections. Then we send a message to the
-- endpoint's event handler by embedding a shutdown
-- request in a connection request. The request is formally rejected, but is interpreted as a kill.
-- The event handler passes on the request to CH. This is all done synchronously, as we hold a lock
-- on the endpoint for the duration.
apiCloseEndPoint :: CCITransport -> CCIEndpoint -> IO ()
apiCloseEndPoint transport endpoint = 
   (catch closeit handler) 
      where handler :: CCI.CCIException -> IO ()
            handler _ = return ()
            helloPacket = BSC.pack magicEndpointShutdown
            closeit = 
               do modifyMVar_ (cciEndpointState endpoint) $ \st -> 
                     case st of
                       CCIEndpointValid {cciConnectionsById = connections} -> 
                         do putEvent endpoint EndPointClosed
                            mapM_ (swallowException . closeIndividualConnection transport endpoint) (Map.elems connections)
                            swallowException $ 
                               do sendSignalByConnect transport endpoint 
                                    (getEndpointAddress endpoint) helloPacket
                                  takeMVar (cciEndpointFinalized endpoint)
                            return CCIEndpointClosed
                       _ -> dbg "Endpoint already closed" >> return st 


sendSignalByConnect :: CCITransport -> CCIEndpoint -> EndPointAddress -> ByteString -> IO ()
sendSignalByConnect transport endpoint epaddr bs =
      timeoutMaybe someTimeout theerror go
  where go = CCI.connect (cciEndpoint endpoint)
              (BSC.unpack $ endPointAddressToByteString epaddr)
              bs (translateReliability ReliableOrdered) 
              (0::WordPtr) someTimeout
        someTimeout :: Integral a => Maybe a
        someTimeout = fmap fromIntegral $ cciConnectionTimeout (cciParameters transport)
        theerror = (TransportError ConnectTimeout "Connection reply timeout")

-- | The procedure for creating a new connection is:
--   1. apiConnect(Local) creates new connection ID
--   2. apiConnect(Local) calls CCI.connect, which sends an EvConnectRequest message to the remote side
--   3. EvConnectRequest is received by remote, which creates new remote ID and calls accept
--   4. accept(Remote) sends EvAccept to remote (target) side and EvConnect to local (originating) side
--   5. When the remote side gets EvAccept, it registers the new connection with its ID
--   6. apiConnect waits for EvConnect message, after which is sends a ControlMessageInitConnection message on newly-created connection; this message contains reliability and endpoint information that is needed for the remote side to send on to CH
--   7. Future EvReceives on the Remote side (caused by receiving a message) will result in event being placed on the remote endpoint queue
-- TODO: create shortcut connection of sending to self endpoint?
apiConnect :: CCITransport -> CCIEndpoint -> 
              EndPointAddress -> Reliability -> 
              ConnectHints -> 
              IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect transport endpoint remoteaddress reliability hints = 
  try $ mapCCIException (translateException ConnectFailed) $ 
      do (cciconn, transportconn, _cid) <- createConn
         timeoutMaybe (maybeTimeout) (TransportError ConnectTimeout "Connection reply timeout") (waitReady cciconn)
         sendControlMessage transport endpoint cciconn (ControlMessageInitConnection reliability (getEndpointAddress endpoint))
         -- putEvent endpoint (ConnectionOpened cid reliability remoteaddress) --I am pretty sure this line is not necessary
         return transportconn
   where
        maybeTimeout :: Integral a => Maybe a
        maybeTimeout = -- OR, alternatively: fmap fromIntegral $ connectTimeout hints
                       fmap fromIntegral $ cciConnectionTimeout (cciParameters transport)
        createConn =
               modifyMVar (cciEndpointState endpoint) $ \st ->
                   case st of 
                     CCIEndpointValid {cciNextConnectionId = cid, cciConnectionsById = connById} ->
                             do
                                connstate <- newMVar CCIConnectionInit
                                ready <- newEmptyMVar
                                let newmapById = Map.insert cid theconn connById
                                    newst = st {cciNextConnectionId = cid+1,
                                                cciConnectionsById = newmapById}
                                    theconn = CCIConnection {cciConnectionId = cid, 
                                                             cciConnectionState = connstate,
                                                             cciReady = ready}
                                    transportconn = Connection {
                                      send  = apiSend transport endpoint theconn,
                                      close = apiCloseConnection transport endpoint theconn
                                    }
                                requestConnection (fromIntegral cid)
                                return (newst,((theconn,transportconn, cid)))
                     _ -> 
                         throwIO (TransportError ConnectFailed "Endpoint invalid")
        requestConnection :: WordPtr -> IO ()
        requestConnection contextId =
         let localEndpoint = cciEndpoint endpoint
             remoteAddress = (BSC.unpack $ endPointAddressToByteString remoteaddress)
             helloPacket = BSC.empty
          in CCI.connect localEndpoint remoteAddress
                     helloPacket (translateReliability reliability) 
                     contextId maybeTimeout

-- | Sending is silent (viz. CCI.SEND_SILENT). This corresponds to the Unified's preference.
-- We distinguish control messages from regular messages by the first byte: 0 for the latter, 1 for the former.
-- Since we will also use control messages to provide RMA admin, we can't send them on RMA 
apiSend :: CCITransport -> CCIEndpoint -> CCIConnection -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
apiSend transport endpoint conn bs = 
  sendCore transport endpoint conn (0::WordPtr) True (BSC.singleton (chr 0):bs)

sendControlMessage :: CCITransport -> CCIEndpoint -> CCIConnection -> ControlMessage -> IO (Either (TransportError SendErrorCode) ())
sendControlMessage transport endpoint conn bs = 
  sendCore transport endpoint conn (0::WordPtr) False (BSC.singleton (chr 1):BSL.toChunks (encode bs)) >>= dbgEither

-- | CCI has a maximum message size of maxMessageLength. For short messages, we will send them the
-- normal way, but for longer messages we are obligated to use CCI's RMA mechanism
sendCore :: CCITransport -> CCIEndpoint -> CCIConnection -> WordPtr -> Bool -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
sendCore _ _ conn context allowRMA bs = 
  try $ mapCCIException (translateException SendFailed) $ 
     withMVar (cciConnectionState conn) $ \st ->
        case st of
           CCIConnectionConnected {cciMaxSendSize = maxMessageLength,
                                   cciConnection = realconnection} ->
             if messageLength > fromIntegral maxMessageLength && allowRMA
                then sendRMA realconnection bs context
                else sendSimple realconnection bs context
           CCIConnectionInit -> dbg "Connection not initialized" >>
                    throwIO (TransportError SendClosed "Connection not initialized")
           CCIConnectionClosed -> dbg "Connection already closed" >> 
                    throwIO (TransportError SendClosed "Connection already closed")
    where messageLength = sum (map BSC.length bs)

sendRMA :: CCI.Connection -> [ByteString] -> WordPtr -> IO ()
sendRMA = error "Unsupported" --TODO

sendSimple :: CCI.Connection -> [ByteString] -> WordPtr -> IO ()
sendSimple conn bs wp = 
     CCI.sendv conn bs wp [CCI.SEND_SILENT]

-- | CCI.disconnect is a local operation; it doesn't notify the other side, so we have to. We send a control
-- message to the other side, which unregisters and disconnects the remote endpoint, then do the same here
apiCloseConnection :: CCITransport -> CCIEndpoint -> CCIConnection -> IO ()
apiCloseConnection transport endpoint conn = 
   mapCCIException (translateException CCIDisconnectFailed) $ 
      modifyMVar_ (cciEndpointState endpoint) $ \st ->
          case st of
            CCIEndpointValid {cciConnectionsById = connectionsById} ->
              do closeIndividualConnection transport endpoint conn
                 let newState = st {cciConnectionsById = newConnectionsById}
                     newConnectionsById = Map.delete (cciConnectionId conn) connectionsById
                 return newState
            _ -> dbg "Endpoint closed" >> return st

-- | Takes care of the work of shutting down the connection (from the originator side), including
-- sending a shutdown message to the server (target). Does not, however, update the connection
-- table in the endpoint -- use apiCloseConnection for most purposes.
closeIndividualConnection :: CCITransport -> CCIEndpoint -> CCIConnection -> IO ()
closeIndividualConnection transport endpoint conn = 
              do sendControlMessage transport endpoint conn (ControlMessageCloseConnection) 
                 transportconn <- modifyMVar (cciConnectionState conn) $ \connst ->
                    case connst of
                      CCIConnectionConnected {cciConnection=realconn} ->
                         return $ (CCIConnectionClosed,Just realconn)
                      CCIConnectionClosed -> dbg "Connection is already closed" >> 
                         return (CCIConnectionClosed,Nothing)
                      CCIConnectionInit -> dbg "Connection still initializing"  >> 
                         return (CCIConnectionClosed,Nothing)                       
                 -- putEvent endpoint (ConnectionClosed $ cciConnectionId conn) -- I am also pretty sure this line is not necessary
                 maybe (return ()) CCI.disconnect transportconn

translateReliability :: Reliability -> CCI.ConnectionAttributes       
translateReliability ReliableOrdered = CCI.CONN_ATTR_RO
translateReliability ReliableUnordered = CCI.CONN_ATTR_RU
translateReliability Unreliable = CCI.CONN_ATTR_UU

{-
getEventConnect :: CCI.EventData s -> IO CCI.Connection
getEventConnect evd = 
  case evd of -- TODO more precise translation of CCI.Status to real messages
    CCI.EvConnect _ctx (Right conn) -> return conn
    CCI.EvConnect _ctx (Left err) -> 
         throwIO (TransportError ConnectFailed $ "Transport closed " ++ show err)
    _ -> throwIO (TransportError ConnectFailed "Transport closed")
-}

receiveEvent :: CCITransport -> CCIEndpoint -> 
                (forall s. CCI.EventData s -> IO a) -> 
                IO a
receiveEvent transport endpoint f =
   case cciReceiveStrategy (cciParameters transport) of
     AlwaysPoll -> receivePoll
     AlwaysBlock -> receiveBlock
   where receivePoll = CCI.pollWithEventData (cciEndpoint endpoint) f
         receiveBlock = CCI.withEventData (cciEndpoint endpoint) 
                                          (cciFileDescriptor endpoint) f

translateException :: a -> CCI.CCIException -> TransportError a
translateException a ex = TransportError a (show ex)
-- TODO more precise translation of CCI.CCIException to TransportError

mapCCIException :: Exception e => (CCI.CCIException -> e) -> IO a -> IO a
mapCCIException f p = catch p (throwIO . f)

swallowException :: IO () -> IO ()
swallowException a = a `catch` handler
  where handler :: SomeException -> IO ()
        handler _ = return ()

