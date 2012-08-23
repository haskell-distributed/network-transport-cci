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

import Control.Monad (liftM, forM_, when)
import Control.Concurrent.Chan
import Control.Concurrent (forkIO, ThreadId)
import Control.Concurrent.MVar
import Control.Exception (catch, bracketOnError, try, SomeException, throw, throwIO, Exception, finally)
import Data.Binary (Binary,put,get,getWord8, putWord8, encode,decode)
import Data.ByteString (ByteString)
import Data.Char (chr,ord)
import qualified Foreign.Marshal.Alloc as Alloc
import Data.Typeable (Typeable)
import Data.Word (Word32, Word64)
import Network.CCI (WordPtr)
import Foreign.C.String (CStringLen)
import Network.Transport.Internal (timeoutMaybe)
import Network.Transport (EventErrorCode(..), Transport(..), TransportError(..), NewEndPointErrorCode(..), EndPointAddress(..), Event(..), TransportError, ConnectErrorCode(..), EndPoint(..), SendErrorCode(..), NewMulticastGroupErrorCode(..), ResolveMulticastGroupErrorCode(..), Reliability(..), ConnectHints(..), Connection(..), ConnectionId)
import Prelude hiding (catch)
import qualified Data.ByteString.Char8 as BSC (packCStringLen, concat, useAsCStringLen, head, tail, singleton,pack, unpack, empty, length)
import qualified Data.ByteString.Lazy as BSL (toChunks,fromChunks)
import qualified Data.Map as Map
import qualified Network.CCI as CCI (strError, rmaHandle2ByteString,createRMARemoteHandle,withRMALocalHandle, rmaRegister, rmaDeregister, rmaWrite, RMA_MODE(..), RMA_FLAG(..), RMARemoteHandle, RMALocalHandle, accept, reject, createPollingEndpoint, createBlockingEndpoint, Endpoint, Device, initCCI, finalizeCCI, CCIException(..), getEndpt_URI, destroyEndpoint, connect, ConnectionAttributes(..), EventData(..), packEventBytes, Connection, pollWithEventData, withEventData, connMaxSendSize, sendv, SEND_FLAG(..), disconnect,setEndpt_KeepAliveTimeout,getEndpt_RMAAlign,RMAAlignments(..),Status(..))
import System.Posix.Types (Fd)

-- TODO: carefully read tests from distributed-process and network-transport-tcp, implement here
-- TODO: wait for ONL to implement keepalive timeout, extend CCIParameters to support other endpoint options
-- TODO: use CCI.strError to display better exceptions
-- TODO: use Transport.ErrorEvent (esp. ConnectionLost) in response to bogus requests; throw $ userError when shutting down transport or endpoint
-- TODO: handle unexpected termination eventloop by closing transport and reporting error to CH
-- TODO reduce excessive copying when transmitting RMA buffers
-- TODO: test UU/RU mode
-- CCI explodes after about 1000 connections; should I use virtual connections, as the TCP version does?
-- TODO add exception handling todo CCI's eventHandler, make sure that thrown exceptions kill the endpoint cleanly, and notify CH


-- | Arguments given to createTransport
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
        cciDevice :: Maybe CCI.Device,

        cciMaxRMABuffer :: Word64
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
        cciReceiveStrategy = AlwaysPoll,
        cciDevice = Nothing, -- ^ default device (as determined by driver)
        cciMaxRMABuffer = 4194304
     }

data CCIEndpoint = CCIEndpoint
   {
      cciFileDescriptor :: Fd,
      cciEndpoint :: CCI.Endpoint,
      cciChannel :: Chan Event,
      cciUri :: String,
      cciTransportEndpoint :: EndPoint,
      cciRMAAlignments :: CCI.RMAAlignments,
      cciEndpointState :: MVar CCIEndpointState,
      cciEndpointFinalized :: MVar ()
   }

type RMATransferId = Int

data CCIEndpointState = CCIEndpointValid {
                          cciNextConnectionId :: !ConnectionId,
                          cciEndpointThread :: ThreadId,
                          cciRMAState :: Map.Map RMATransferId CCIRMAState, -- TODO IntMap
                          cciRMANextTransferId :: !RMATransferId,
                          cciConnectionsById :: Map.Map ConnectionId CCIConnection -- TODO IntMap
                        } | CCIEndpointClosed

data CCIRMAState = CCIRMAState {
        cciRMARemoteHandle :: MVar (CCI.RMARemoteHandle,RMATransferId),
        cciOutstandingChunks :: Int,
        cciRMAComplete :: MVar CCI.Status
     }

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
        | ControlMessageInitRMA {rmaSize :: Int, rmaId :: RMATransferId, rmaEndpointAddress :: String}
        | ControlMessageAckInitRMA {rmaAckOrginatingId :: RMATransferId, rmaAckRemoteId :: RMATransferId, rmaAckRemoteHandle :: ByteString}
        | ControlMessageFinalizeRMA {rmaOk :: Bool, rmaRemoteFinalizingId :: RMATransferId}
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
  put (ControlMessageInitRMA rs rid repa) = putWord8 2 >> put rs >> put rid >> put repa
  put (ControlMessageAckInitRMA roid rrid bs) = putWord8 3 >> put roid >> put rrid >> put bs
  put (ControlMessageFinalizeRMA rok rfin) = putWord8 4 >> put rok >> put rfin
  get = do hdr <- getWord8
           case hdr of
             0 -> return ControlMessageCloseConnection
             1 -> do r <- get
                     ep <- get
                     return $ ControlMessageInitConnection r ep
             2 -> do rs <- get
                     rid <- get
                     repa <- get
                     return $ ControlMessageInitRMA rs rid repa
             3 -> do roid <- get
                     rrid <- get
                     bs <- get
                     return $ ControlMessageAckInitRMA roid rrid bs
             4 -> do rok <- get
                     rfin <- get
                     return $ ControlMessageFinalizeRMA rok rfin
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
       CCI.setEndpt_KeepAliveTimeout endpoint 5000000 -- TODO broken
       uri <- CCI.getEndpt_URI endpoint
       align <- CCI.getEndpt_RMAAlign endpoint -- TODO broken
       chan <- newChan
       thrdsem <- newEmptyMVar
       thrd <- forkIO (endpointHandler thrdsem)
       finalized <- newEmptyMVar
       withMVar (cciTransportState transport) $ \st ->
          case st of
            CCITransportStateValid ->
               do endpstate <- newMVar $ 
                        CCIEndpointValid {
                            cciRMAState = Map.empty,
                            cciRMANextTransferId = 1,
                            cciNextConnectionId = 0,
                            cciEndpointThread = thrd,
                            cciConnectionsById = Map.empty }
                  let myEndpoint = CCIEndpoint {
                        cciFileDescriptor = fd,
                        cciEndpoint = endpoint,
                        cciChannel = chan,
                        cciUri = uri,
                        cciRMAAlignments = align,
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

allocCStringLen :: Int -> IO CStringLen
allocCStringLen size = 
   do buf <- Alloc.mallocBytes size -- TODO this should be aligned (to something)
      return (buf, size)

freeCStringLen :: CStringLen -> IO ()
freeCStringLen (buf,_) = Alloc.free buf

data EndpointLoopState = EndpointLoopState
    {
       eplsConnectionsByConnection :: Map.Map CCI.Connection ConnectionId,
       eplsNextConnectionId :: !ConnectionId,
       eplsNextTransferId :: !RMATransferId,
       eplsTransfers :: Map.Map RMATransferId (CCI.RMALocalHandle, CStringLen) -- TODO IntMap
    }

endpointLoop :: CCITransport -> CCIEndpoint -> IO ()
endpointLoop transport endpoint =
 let newEpls = EndpointLoopState {eplsNextConnectionId = 0, 
                                  eplsConnectionsByConnection = Map.empty,
                                  eplsNextTransferId = 0,
                                  eplsTransfers = Map.empty}
     loop :: EndpointLoopState -> IO ()
     loop epls = 
       case epls of 
         EndpointLoopState {eplsNextConnectionId = nextConnectionId,
                            eplsConnectionsByConnection = connectionsByConnection,
                            eplsNextTransferId = nextTransferId,
                            eplsTransfers = transfers} ->
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
                                     return $ Just epls {eplsNextConnectionId = nextConnectionId+1}
                            shutdown | shutdown == magicEndpointShutdown ->
                                  do CCI.reject sev -- TODO deallocate outstanding RMA buffers
                                     return Nothing 
                            _ ->  do dbg "Unknown connection magic word"
                                     CCI.reject sev
                                     return $ Just epls
                  CCI.EvSend ctx status _conn ->
                      case ctx of
                         0 -> return () -- ordinary send confirmation
                         rmaid -> withMVar (cciEndpointState endpoint) $ \st -> -- TODO check status flag, possibly return error value
                                    case st of
                                       CCIEndpointValid {cciRMAState = rmaState} ->
                                          case Map.lookup (fromEnum rmaid) rmaState of
                                             Nothing -> dbg "Bogus RMA ID"
                                             Just rmas -> putMVar (cciRMAComplete rmas) status
                                       _ -> dbg "Endpoint already dead" 
                        >> (return $ Just epls)
                  CCI.EvRecv eb conn -> 
                      let connid = case Map.lookup conn connectionsByConnection of
                                      Nothing -> throw (userError $ "Unknown connection in EvRecv: " ++ show conn)
                                      Just val ->  val
                       in do msg <- CCI.packEventBytes eb
                             case ord $ BSC.head msg of
                                  0 -> do putEvent endpoint (Received connid [BSC.tail msg]) -- normal message
                                          return $ Just epls
                                  _ -> case decode $ BSL.fromChunks [BSC.tail msg] of
                                          ControlMessageInitConnection rel epa -> 
                                             do putEvent endpoint (ConnectionOpened connid rel epa)
                                                return $ Just epls
                                          ControlMessageCloseConnection ->
                                             do CCI.disconnect conn
                                                putEvent endpoint (ConnectionClosed connid)
                                                return $ Just epls {eplsConnectionsByConnection = Map.delete conn connectionsByConnection}
                                          ControlMessageInitRMA {rmaSize=rmasize, 
                                                                 rmaId=orginatingId} -> -- TODO error handling, if allocaiton fails sends a NACK
                                             do (cstr, localh) <- makeRMABuffer rmasize endpoint CCI.RMA_WRITE
                                                let notify = ControlMessageAckInitRMA
                                                           {rmaAckOrginatingId=orginatingId,
                                                            rmaAckRemoteId = nextTransferId, 
                                                            rmaAckRemoteHandle = CCI.rmaHandle2ByteString localh}
                                                sendControlMessageInside transport endpoint conn notify
                                                return $ Just epls {eplsNextTransferId = nextTransferId+1,
                                                                    eplsTransfers = Map.insert nextTransferId (localh, cstr) transfers}
                                          ControlMessageAckInitRMA {rmaAckOrginatingId=originatingId,
                                                            rmaAckRemoteId = remoteId, 
                                                            rmaAckRemoteHandle = bs} -> -- TODO error handling
                                             let Just remoteHandle = CCI.createRMARemoteHandle bs
                                              in withMVar (cciEndpointState endpoint) $ \st ->
                                                   case st of
                                                      CCIEndpointValid {cciRMAState = rmastate} ->
                                                         case Map.lookup originatingId rmastate of
                                                           Nothing -> dbg "Bogus originating id"
                                                           Just myrma -> putMVar (cciRMARemoteHandle myrma)
                                                                 (remoteHandle, remoteId)
                                                      _ -> dbg "Unexpected endpoint state"
                                                  >> (return $ Just epls)
                                          ControlMessageFinalizeRMA {rmaOk = ok, rmaRemoteFinalizingId = remoteid} ->
                                             case Map.lookup remoteid transfers of
                                               Nothing -> dbg "Bogus transfer id"
                                               Just (lhandle, cstr) -> 
                                                   do when ok $ 
                                                         do buffermsg <- BSC.packCStringLen cstr
                                                            putEvent endpoint (Received connid [buffermsg])
                                                      freeRMABuffer endpoint (cstr,lhandle) 
                                              >> (return $ Just epls {eplsTransfers = Map.delete remoteid transfers})
                  CCI.EvConnect connectionId (Left status) ->
-- TODO: change connection state to error/invalid/closed
                      do statusMsg <- CCI.strError (Just $ cciEndpoint endpoint) status
                         let errmsg = TransportError ConnectNotFound ("Connection "++show connectionId++" failed because "++statusMsg)
                         modifyMVar_ (cciEndpointState endpoint) $ \st ->
                           case st of
                             CCIEndpointValid {cciConnectionsById = connections} ->
                               let theconn = Map.lookup (fromEnum connectionId) connections -- this conversion (WordPtr to ConnectionId) is probably okay
                                   newmap = Map.delete (fromEnum connectionId) connections 
                                in maybe (dbg $ "Connection " ++ show connectionId ++ " failed because " ++ statusMsg) 
                                         (\tc -> putMVar (cciReady tc) (Just $ errmsg)) 
                                         theconn >>
                                   return (st {cciConnectionsById = newmap})
                             _ -> return (st)
                         return $ Just epls
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
                                            do let maxmsg = CCI.connMaxSendSize conn
                                               let newconnstate = CCIConnectionConnected 
                                                     {cciConnection = conn, 
                                                      cciMaxSendSize = maxmsg}
                                               putMVar (cciReady cciconn) Nothing
                                               return (newconnstate, ())
                                          _ -> do dbg $ "Unexpected EvConnect for connection " ++ 
                                                    show connectionId ++ " in state " ++ show connstate
                                                  return (connstate, ())
                             _ -> dbg "Can't handle EvConnect when endpoint is closed"
                         return $ Just epls
                  CCI.EvAccept connectionId (Left status) ->
                      do statusMsg <- CCI.strError (Just $ cciEndpoint endpoint) status
                         dbg ("Failed EvAccept on connection " ++ show connectionId ++ " because " ++ statusMsg)
                         return $ Just epls
                  CCI.EvAccept connectionId (Right conn) -> 
                      -- The connection is not fully ready until it gets an EvAccept. Therefore,
                      -- there is a possible race condition if the other (originating) side receives
                      -- its EvConnect and tries to send a message before this (target) side is ready.
                      let newmap = Map.insert conn (fromEnum connectionId) connectionsByConnection
                       in return $ Just epls {eplsConnectionsByConnection = newmap}
                  CCI.EvKeepAliveTimedOut _conn ->
                     do dbg $ show ev -- TODO another endpoint has died: tell CH; to do this we need to learn the endpointaddress for the given conn
                        return $ Just epls
                  CCI.EvEndpointDeviceFailed _endp ->
                     do dbg $ show ev -- TODO this endpoint has died: tell CH
                        -- putErrorEvent endpoint (Just $ getEndpointAddress endpoint) (Map.elems connectionsByConnection) "Device failed"
                        -- Should probably kill Endpoint and move EndpointState to closed
                        return $ Just epls
              case ret of
                 Nothing -> do --CCI.destroyEndpoint (cciEndpoint endpoint)  -- TODO this hangs/segfaults
                                putMVar (cciEndpointFinalized endpoint) ()
                 Just newstate -> loop newstate
  in loop newEpls 
        
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
apiConnect transport endpoint remoteaddress reliability _hints = 
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
                                requestConnection (fromIntegral cid) -- TODO this conversion (Int to WordPtr) is probablay okay
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
  sendCore transport endpoint conn (0::WordPtr) False bs

sendControlMessage :: CCITransport -> CCIEndpoint -> CCIConnection -> ControlMessage -> IO (Either (TransportError SendErrorCode) ())
sendControlMessage transport endpoint conn bs = 
  sendCore transport endpoint conn (0::WordPtr) True (BSL.toChunks (encode bs)) >>= dbgEither

sendControlMessageInside :: CCITransport -> CCIEndpoint -> CCI.Connection -> ControlMessage -> IO ()
sendControlMessageInside _transport _endpoint conn bs = 
   sendSimple conn (BSC.singleton (chr 1):BSL.toChunks (encode bs)) (0::WordPtr)

-- | CCI has a maximum message size of maxMessageLength. For short messages, we will send them the
-- normal way, but for longer messages we are obligated to use CCI's RMA mechanism
sendCore :: CCITransport -> CCIEndpoint -> CCIConnection -> WordPtr -> Bool -> [ByteString] -> IO (Either (TransportError SendErrorCode) ())
sendCore transport endpoint conn context isCtrlMsg bs = 
  try $ mapCCIException (translateException SendFailed) $ 
     withMVar (cciConnectionState conn) $ \st ->
        case st of
           CCIConnectionConnected {cciMaxSendSize = maxMessageLength,
                                   cciConnection = realconnection} ->
             if (messageLength > fromIntegral maxMessageLength && not isCtrlMsg)
                then sendRMA transport endpoint conn realconnection bs context 
                else sendSimple realconnection augmentedbs context
           CCIConnectionInit -> dbg "Connection not initialized" >>
                    throwIO (TransportError SendClosed "Connection not initialized")
           CCIConnectionClosed -> dbg "Connection already closed" >> 
                    throwIO (TransportError SendClosed "Connection already closed")
   
    where messageLength = sum (map BSC.length bs)
          augmentedbs = msgprefix:bs
          msgprefix = case isCtrlMsg of
                        True -> BSC.singleton (chr 1)
                        False -> BSC.singleton (chr 0)

-- | For RMA transmissions, we do this:
-- 1. Allocate a local transfer ID on this endpoint
-- 2. Send an InitRMA message to the other side, along with the size of the buffer we want to send
-- 3. Remote side allocates buffer, send back an acknowledgement with its remote buffer handle and its remote transfer ID
-- 4. We move the message to a local RMA buffer, and call rmaWrite to send the data to the remote buffer
-- 5. We wait for transmission confirmation via an EvSend with our local transfer ID
-- 6. After we get confirmation, we can release the local buffer and send a FinalizeRMA message to the other side
-- 7. The other side copies its buffers to a message, sends it to CH, and frees its buffers
sendRMA :: CCITransport -> CCIEndpoint -> CCIConnection -> CCI.Connection -> [ByteString] -> WordPtr -> IO ()
sendRMA transport endpoint _conn realconn bs _ctx = 
  do (rmatid,rmastate) <- modifyMVar (cciEndpointState endpoint) $ \st ->
       case st of
        CCIEndpointValid 
          {cciRMANextTransferId=nextTransferId,
           cciRMAState=rmaState} -> 
             let tid = case nextTransferId of
                         0 -> nextTransferId + 1 -- Skip over ID zero, since context 0 is used for regular messages
                         _ -> nextTransferId
              in do rmaRemoteHandle <- newEmptyMVar
                    rmaComplete <- newEmptyMVar
                    let myRmaState = CCIRMAState
                                       {
                                          cciRMARemoteHandle = rmaRemoteHandle,
                                          cciRMAComplete = rmaComplete,
                                          cciOutstandingChunks = length chunks
                                       }
                    return (st {cciRMANextTransferId=tid+1,
                             cciRMAState=Map.insert tid myRmaState rmaState},(tid,myRmaState))
        _ -> throwIO (TransportError SendFailed "Endpoint invalid")
     let initMsg = 
           ControlMessageInitRMA {rmaSize = msgLength, rmaId = rmatid, rmaEndpointAddress = cciUri endpoint} -- remote side allocates buffer and sends back RemoteHandle, which local event handler puts into cciRMARemoteHandle
         freeRemoteBuffer remoteid ok = swallowException $ sendControlMessageInside transport endpoint realconn
               ControlMessageFinalizeRMA {rmaOk = ok, rmaRemoteFinalizingId = remoteid}
         throwStatus status = case status of
                                CCI.SUCCESS -> return ()
                                _ -> throwIO $ TransportError SendFailed ("EvSend reported error " ++ show status)
         sendInitMsg = sendControlMessageInside transport endpoint realconn initMsg
         eraseRmaState = modifyMVar_ (cciEndpointState endpoint) (\st -> return st {cciRMAState = Map.delete rmatid (cciRMAState st)})
         maybeTimeout = fmap fromIntegral $ cciConnectionTimeout (cciParameters transport)
         err = TransportError SendFailed "RMA partner failed to respond"
         getRemoteHandle = timeoutMaybe maybeTimeout err $
                                do sendInitMsg
                                   takeMVar (cciRMARemoteHandle rmastate)
         doRMA = bracketOnError 
                    getRemoteHandle
                    (\(_, remoteid) -> freeRemoteBuffer remoteid False)
                    (\(remoteh, remoteid) ->
                       do withRMABuffer endpoint allbs CCI.RMA_READ $ \localh ->
                            let chunksAndFlags = reverse (zip (reverse chunks) ([CCI.RMA_FENCE] : repeat [CCI.RMA_SILENT]))
                             in do forM_ chunksAndFlags $ \((buffOffset,buffLength),opts) -> 
                                     CCI.rmaWrite realconn Nothing remoteh (toEnum buffOffset) localh (toEnum buffOffset) (toEnum buffLength) 
                                     (toEnum rmatid::WordPtr) opts
                                   takeMVar (cciRMAComplete rmastate) >>= throwStatus
                          freeRemoteBuffer remoteid True) -- TODO if the CCI people fix send confirmations, this won't be necessary: the framework will send a receipt notice to the receiving side
     doRMA `finally` eraseRmaState -- TODO probably shold mask exceptions here
         where allbs = BSC.concat bs
               msgLength = BSC.length allbs
               chunkSize = msgLength -- cciMaxRMABuffer (cciParameters transport) -- TODO test this: I believe that the destination offset parameter of cciWrite is ignored, so transfers over this limit (by default 4MB) will break; furthermore, the chunkSize should not exceed the maximum size returned by CCI's CCI_OPT_ENDPT_RMA_ALIGN option
               (fullchunks,partialchunk) = msgLength `divMod` chunkSize
               chunks = take fullchunks ([ (offsets,chunkSize) | offsets<-[0,chunkSize..]]) 
                           ++ maybePartialChunk
               maybePartialChunk = if partialchunk==0
                                      then []
                                      else [(fullchunks*chunkSize,partialchunk)]
               
-- TODO: draw buffers from a pool of locally stored buffers, rather than registering them each time
withRMABuffer :: CCIEndpoint -> ByteString -> CCI.RMA_MODE -> (CCI.RMALocalHandle -> IO a) -> IO a
withRMABuffer endpoint bs mode f =
   BSC.useAsCStringLen bs $ \cstr -> -- TODO this buffer should be aligned (to something)
       CCI.withRMALocalHandle (cciEndpoint endpoint) cstr mode f

makeRMABuffer :: Int -> CCIEndpoint -> CCI.RMA_MODE -> IO (CStringLen, CCI.RMALocalHandle)
makeRMABuffer rmasize endpoint mode = 
  do cstr <- allocCStringLen rmasize -- TODO cache registered buffers      TODO this buffer should be aligned on a 4096 boundary
     localh <- CCI.rmaRegister (cciEndpoint endpoint) cstr mode
     return (cstr,localh)

freeRMABuffer :: CCIEndpoint -> (CStringLen,CCI.RMALocalHandle) -> IO ()
freeRMABuffer _ (cstr, lhandle) =
   do -- CCI.rmaDeregister (cciEndpoint endpoint) lhandle -- TODO segfaults
      freeCStringLen cstr

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

