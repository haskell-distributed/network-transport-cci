-- |
-- Copyright : (C) 2012-2013 Parallel Scientific Labs, LLC.
-- License   : BSD3
--
-- Cloud Haskell backend supporting CCI, via CCI bindings.
--
-- A 'Network.Transport.EndPoint' is implemented with a 'Network.CCI.Endpoint'.
--
-- A 'Network.Transport.Connection' is implemented with a
-- 'Network.CCI.Connection'.
--
-- Small messages are sent with CCI active messages and the larger messages
-- are sent with RMA operations.
--
-- The buffers holding large messages are registered for RMA in the sender
-- side. If the application reuses these buffers for multiple messages, the
-- cost of subsequent registrations diminishes. In the future we expect to
-- provide a transport specific call to pre-register buffers.
--
-- The receiver side registers buffers for RMA when requested by a sender.
-- A pool of registered buffers is maintained, from which registered buffers
-- are grabbed on demand.
--
-- When the receiver side gets a large message in a registered buffer, it
-- handles the buffer to the application as a ByteString. When the ByteString
-- is garbage collected the buffer is returned to the pool. In the future we
-- expect to unregister buffers before they are returned to the pool if
-- too many buffers are registered. We also expect to offer transport specific
-- calls to promptly unregister or return buffers to the pool.
--
{-# LANGUAGE ForeignFunctionInterface #-}
module Network.Transport.CCI
  ( createTransport
  , createCCITransport
  , CCIParameters(..)
  , CCITransport
  , defaultCCIParameters
  , networkTransport
  , ReceiveStrategy(..)
  , requestBuffer
  , returnBuffer
  , unregisterBuffer
  ) where

import qualified Network.Transport.CCI.Pool as Pool

import qualified Network.CCI as CCI
import Network.Transport.Internal (timeoutMaybe)
import Network.Transport
    ( Transport(..)
    , TransportError(..)
    , NewEndPointErrorCode(..)
    , EndPointAddress(..)
    , Event(..)
    , TransportError
    , ConnectErrorCode(..)
    , EndPoint(..)
    , SendErrorCode(..)
    , NewMulticastGroupErrorCode(..)
    , ResolveMulticastGroupErrorCode(..)
    , Reliability(..)
    , ConnectHints(..)
    , Connection(..)
    , ConnectionId
    )

import qualified Data.Map as Map

import Data.Binary
    ( Binary
    , decode
    , encode
    )

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as BSL (toChunks, fromChunks)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen, unsafeUseAsCString)

import Control.Applicative ((<*>), (<$>), pure)
import Control.Monad (liftM, forM_, void, when)
import Control.Concurrent.Chan
import Control.Concurrent (forkIO, ThreadId, threadDelay)
import Control.Concurrent.MVar
import Control.Exception
    ( catch
    , bracketOnError
    , try
    , SomeException
    , throw
    , throwIO
    , Exception
    , finally
    )
import Data.Char (chr, ord)
import Data.Foldable (mapM_)
import Data.IORef (newIORef)
import Data.List (genericTake)
import Data.Maybe (catMaybes, fromMaybe)
import Data.Typeable (Typeable)
import Data.Word (Word32, Word64)
import Foreign.C.Types (CInt(..))
import Foreign.Ptr (WordPtr)
import GHC.Generics (Generic)

import System.Posix.Types (Fd)

import Prelude hiding (mapM_)

-- TODO: wait for ORNL to implement keepalive timeout, extend CCIParameters to
-- support other endpoint options
--
-- TODO: use CCI.strError to display better exceptions
--
-- TODO: use Transport.ErrorEvent (esp. ConnectionLost) in response to bogus
-- requests; throw $ userError when shutting down transport or endpoint
--
-- TODO: test UU/RU mode
--
-- CCI explodes after about 1000 connections; should I use virtual connections,
-- as the TCP version does?
--
-- TODO add exception handling todo CCI's eventHandler, make sure that thrown
-- exceptions kill the endpoint cleanly, and notify CH; handle out of memory
-- errors from getBuffer elegantly
--
-- TODO avoid copying RMA buffers by using sendNoCopy and unsafeUseAsCStringLen

-- | Arguments given to 'createTransport'.
data CCIParameters = CCIParameters
    { -- | How long to wait, in microseconds, before deciding that an attempt to
      -- connect to a nonresponsive foreign host has failed.
      cciConnectionTimeout :: Maybe Word64

      -- | The mechanism by which messages are retrieved from the CCI queue
      -- Polling (the default) will probably result in better performance but
      -- may be wasteful.
    , cciReceiveStrategy :: ReceiveStrategy

      -- | Which device to use. 'Nothing' (the default) means use the default
      -- device. Other values can be retrieved from 'Network.CCI.getDevcices'.
    , cciDevice :: Maybe CCI.Device

      -- | The maximum number of available RMA buffers that the layer permits
      -- before it starts releasing them.
    , cciOutstandingRMABuffers  :: Int

      -- | The largest quantity of data to be sent in a single RMA chunk.
    , cciMaxRMABuffer :: Word32
     } -- TODO add an authorization key to be sent during connecting

data ReceiveStrategy
    = AlwaysPoll
    | AlwaysBlock

data CCITransportState
    = CCITransportStateValid
    | CCITransportStateClosed

data CCITransport = CCITransport
    { cciParameters     :: CCIParameters
    , cciTransportState :: MVar CCITransportState
    , cciLocalEndpoints :: MVar (Map.Map EndPointAddress CCIEndpoint)
    }

-- | Default values to use with 'createTransport'.
defaultCCIParameters :: CCIParameters
defaultCCIParameters = CCIParameters
    { cciConnectionTimeout = Just 10000000       -- 10 second connection timeout
    , cciReceiveStrategy = AlwaysPoll
    , cciDevice = Nothing            -- default device (as determined by driver)
    , cciOutstandingRMABuffers = 10
    , cciMaxRMABuffer = 4194304
    }

data CCIEndpoint = CCIEndpoint
    { cciFileDescriptor :: Fd
    , cciEndpoint :: CCI.Endpoint
    , cciChannel :: Chan Event
    , cciUri :: String
    , cciTransportEndpoint :: EndPoint
    , cciRMAAlignments :: CCI.RMAAlignments
    , cciEndpointState :: MVar CCIEndpointState
    , cciEndpointFinalized :: MVar ()
    , cciPool :: Pool.PoolRef CCI.RMALocalHandle
    }

type RMATransferId = Int

data CCIEndpointState
    = CCIEndpointValid
      { cciNextConnectionId :: !ConnectionId
      , cciEndpointThread :: ThreadId
      , cciRMAState :: Map.Map RMATransferId CCIRMAState -- TODO IntMap
      , cciRMANextTransferId :: !RMATransferId
      , cciConnectionsById :: Map.Map ConnectionId CCIConnection -- TODO IntMap
      }
    | CCIEndpointClosed

data CCIRMAState = CCIRMAState
    { cciRMARemoteHandle :: MVar (Maybe (CCI.RMARemoteHandle,RMATransferId))
    , cciOutstandingChunks :: Int
    , cciRMAComplete :: MVar CCI.Status
    }

data CCIConnection = CCIConnection
    { cciConnectionState :: MVar CCIConnectionState
    , cciReady :: MVar (Maybe (TransportError ConnectErrorCode))
    , cciConnectionId :: ConnectionId
    }

data CCIConnectionState
    = CCIConnectionInit
    | CCIConnectionConnected
      { cciConnection :: CCI.Connection
      , cciMaxSendSize :: Word32
      }
    | CCIConnectionClosed
    deriving (Show)

-- | This is used as an argument to 'Network.Transport.TransportError'.
data CCIErrorCode
    = CCICreateTransportFailed
    | CCIDisconnectFailed
    deriving (Eq, Show, Generic, Typeable)

-- | In addition to regular message (which will be passed on to CH and
-- eventually to a user process), we can send control messages, which will be
-- handled by the CCI event handler.
data ControlMessage
    = ControlMessageCloseConnection
      -- ^ Request that the given connection terminate (and notify CH)
    | ControlMessageInitConnection Reliability EndPointAddress
      -- ^ Finish initializaion of the connection (and notify CH)
    | ControlMessageInitRMA
      { rmaSize :: Int
      , rmaId :: RMATransferId
      , rmaEndpointAddress :: String
      }
    | ControlMessageAckInitRMA
      { rmaAckOrginatingId :: RMATransferId
      , rmaAckRemote :: Maybe (RMATransferId, ByteString)
      }
    | ControlMessageFinalizeRMA
      { rmaOk :: Bool
      , rmaRemoteFinalizingId :: RMATransferId }
    deriving (Generic, Typeable)

-- This really belongs in Network.Transport
deriving instance Generic Reliability
deriving instance Typeable Reliability
instance Binary Reliability

instance Binary ControlMessage

getEndpointAddress :: CCIEndpoint -> EndPointAddress
getEndpointAddress ep = EndPointAddress $ BSC.pack (cciUri ep)

waitReady :: CCIConnection -> IO ()
waitReady conn = takeMVar (cciReady conn) >>= mapM_ throwIO

createCCITransport :: CCIParameters
                   -> IO (Either (TransportError CCIErrorCode) CCITransport)
createCCITransport params =
    try $ mapCCIException (translateException CCICreateTransportFailed) $ do
      CCI.initCCI
      state <- newMVar CCITransportStateValid
      mkInternalTransport state
  where
    mkInternalTransport st = do
      mvLE <- newMVar Map.empty
      return $ CCITransport
        { cciParameters = params
        , cciTransportState = st
        , cciLocalEndpoints = mvLE
        }

-- | Create a transport object suitable for using with CH.
createTransport :: CCIParameters
                -> IO (Either (TransportError CCIErrorCode) Transport)
createTransport params =
    fmap (fmap networkTransport) $ createCCITransport params

networkTransport :: CCITransport -> Transport
networkTransport inter = Transport
    { newEndPoint = apiNewEndPoint inter
    , closeTransport = apiCloseTransport inter
    }

-- | Requests a buffer from the endpoint pool of buffers.
--
-- The given buffer is already registered for RMA operations
-- so it will be faster to use when sending.
--
-- A requested buffer can be released with 'returnBuffer',
-- in which case it may be returned by another call to
-- 'requestBuffer'.
--
requestBuffer :: CCITransport -> EndPoint -> Int -> IO ByteString
requestBuffer t e sz = do
    les <- readMVar (cciLocalEndpoints t)
    case Map.lookup (address e) les of
      Just ep -> do
        mres <- Pool.newBuffer (cciPool ep) sz
        case mres of
          Just buffer -> Pool.convertBufferToByteString (cciPool ep) buffer
          Nothing     -> error "requestBuffer: pool was released"
      Nothing  -> error "requestBuffer: unknown endpoint"

-- | Returns a buffer to the pool of a given endpoint.
--
-- A subsequent call to 'requestBuffer' may return the given buffer.
--
-- It does nothing if the buffer does not belong to the pool of the endpoint.
--
returnBuffer :: CCITransport -> EndPoint -> ByteString -> IO ()
returnBuffer t e bs = do
   les <- readMVar (cciLocalEndpoints t)
   case Map.lookup (address e) les of
     Just ep -> do
       mbuf <- unsafeUseAsCString bs $ flip Pool.lookupBuffer (cciPool ep)
       case mbuf of
         Just buf -> Pool.freeBuffer (cciPool ep) buf
         Nothing  -> return () -- The buffer was not in the pool.
     Nothing  -> error "returnBuffer: unknown endpoint"

-- | Converts an endpoint-pool-buffer into a normal buffer.
--
-- Buffers from the pool of buffers are more expensive than normal buffers
-- because they are registered for RMA operations. This means that the
-- buffer cannot be swapped to disk and is registered with the hardware for
-- DMA.
--
-- This calls unregisters the buffer therefore making the physical memory
-- and the network hardware resources available for some other purpose.
-- Aftewards, using this buffer for sending messages is as expensive as sending
-- any other buffer.
--
-- It does nothing if the buffer does not belong to the pool of the endpoint.
--
unregisterBuffer :: CCITransport -> EndPoint -> ByteString -> IO ()
unregisterBuffer t e bs = do
   les <- readMVar (cciLocalEndpoints t)
   case Map.lookup (address e) les of
     Just ep -> do
       mbuf <- unsafeUseAsCString bs $ flip Pool.lookupBuffer (cciPool ep)
       case mbuf of
         Just buf -> Pool.unregisterBuffer (cciPool ep) buf
         Nothing  -> return () -- The buffer was not in the pool.
     Nothing  -> error "unregisterBuffer: unknown endpoint"

-- | Aligning buffers to the page size makes a difference in performance
-- when registering big buffers for RMA operations. Registering a 4 MB buffer
-- was measured to be 200 us (~ 20 %) faster for page aligned buffers.
--
-- Therefore, we employ pagesize to determine the alignment size.
--
foreign import ccall unsafe "unistd.h getpagesize" pagesize :: CInt

apiCloseTransport :: CCITransport -> IO ()
apiCloseTransport transport =
   modifyMVar_ (cciTransportState transport) $ \st ->
        case st of
          CCITransportStateClosed ->
                  return CCITransportStateClosed
          _ -> do eps <- swapMVar (cciLocalEndpoints transport) Map.empty
                  mapM_ (apiCloseEndPoint transport) eps
                  CCI.finalizeCCI
                  return CCITransportStateClosed

apiNewEndPoint :: CCITransport
               -> IO (Either (TransportError NewEndPointErrorCode) EndPoint)
apiNewEndPoint transport =
    try $ mapCCIException (translateException NewEndPointFailed) $ do
      (endpoint, fd) <- makeEndpoint $ cciDevice (cciParameters transport)
      -- TODO currently broken in CCI layer
      CCI.setEndpt_KeepAliveTimeout endpoint 5000000
      uri <- CCI.getEndpt_URI endpoint
      align <- CCI.getEndpt_RMAAlign endpoint
      chan <- newChan
      thrdsem <- newEmptyMVar
      thrd <- forkIO (endpointHandler thrdsem)
      finalized <- newEmptyMVar
      withMVar (cciTransportState transport) $ \st -> case st of
        CCITransportStateValid -> do
          endpstate <- newMVar CCIEndpointValid
                         { cciRMAState = Map.empty
                         , cciRMANextTransferId = 1
                         , cciNextConnectionId = 0
                         , cciEndpointThread = thrd
                         , cciConnectionsById = Map.empty }
          rPool <- newIORef $ Just $
            let mostRestrictiveAlignment = maximum $ (fromIntegral pagesize :) $
                  [ CCI.rmaWriteLocalAddr
                  , CCI.rmaWriteRemoteAddr
                  , CCI.rmaReadLocalAddr
                  , CCI.rmaReadRemoteAddr
                  ] <*> pure align
             in Pool.newPool
                  (fromEnum mostRestrictiveAlignment)
                  (cciOutstandingRMABuffers $ cciParameters transport)
                  (\cstr -> CCI.rmaRegister endpoint cstr CCI.RMA_WRITE)
                  (CCI.rmaDeregister endpoint)
          let myEndpoint = CCIEndpoint
                { cciFileDescriptor = fd
                , cciEndpoint = endpoint
                , cciChannel = chan
                , cciUri = uri
                , cciRMAAlignments = align
                , cciTransportEndpoint = transportEndPoint
                , cciEndpointState = endpstate
                , cciEndpointFinalized = finalized
                , cciPool = rPool
                }
              transportEndPoint = EndPoint
                { receive = readChan chan
                , address = EndPointAddress $ BSC.pack uri
                , connect = apiConnect transport myEndpoint
                , closeEndPoint = apiCloseEndPoint transport myEndpoint
                , newMulticastGroup = return . Left $ newMulticastGroupError
                , resolveMulticastGroup =
                    return . Left . const resolveMulticastGroupError }
          putMVar thrdsem (transport,myEndpoint)
          modifyMVar_ (cciLocalEndpoints transport)
            $ return . Map.insert (address transportEndPoint) myEndpoint
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

fromConnectionId :: ConnectionId -> WordPtr
fromConnectionId = toEnum . fromIntegral

toConnectionId :: WordPtr -> ConnectionId
toConnectionId = fromIntegral . fromEnum

endpointHandler :: MVar (CCITransport, CCIEndpoint) -> IO ()
endpointHandler mv =
   do (transport, endpoint) <- takeMVar mv
      catch (endpointLoop transport endpoint) exceptionHandler
  where exceptionHandler :: SomeException -> IO ()
        exceptionHandler e =
           -- TODO shutdown endpoint here
          dbg $ "Exception in endpointHandler: "++ show e

data EndpointLoopState = EndpointLoopState
    { eplsConnectionsByConnection :: Map.Map CCI.Connection ConnectionId
    , eplsNextConnectionId :: !ConnectionId
    , eplsNextTransferId :: !RMATransferId
      -- TODO IntMap
    , eplsTransfers :: Map.Map RMATransferId (Pool.Buffer CCI.RMALocalHandle)
    }

endpointLoop :: CCITransport -> CCIEndpoint -> IO ()
endpointLoop transport endpoint =  loop newEpls
  where
    newEpls = EndpointLoopState
                { eplsNextConnectionId = 0
                , eplsConnectionsByConnection = Map.empty
                , eplsNextTransferId = 0
                , eplsTransfers = Map.empty
                }
    loop :: EndpointLoopState -> IO ()
    loop epls@EndpointLoopState { eplsNextConnectionId = nextConnectionId
                                , eplsConnectionsByConnection = connectionsByConnection
                                , eplsNextTransferId = nextTransferId
                                , eplsTransfers = transfers
                                }
         = do
      ret <- receiveEvent transport endpoint $ \ev -> case ev of
        -- The connection packet will be blank, in which case this is
        -- a real connection request, orit can contain
        -- magicEndpointShutdown, which we interpret as an attempt to
        -- shutdown the endpoint. in other cases the connection is
        -- rejected.
        CCI.EvConnectRequest sev eb _attr -> do
            msg <- CCI.packEventBytes eb
            case BSC.unpack msg of
               [] -> do CCI.accept sev $ fromConnectionId nextConnectionId
                        return $ Just epls {eplsNextConnectionId = nextConnectionId+1}
               shutdown | shutdown == magicEndpointShutdown ->
                     do CCI.reject sev
                        Pool.freePool (cciPool endpoint)
                        return Nothing
               _ ->  do dbg "Unknown connection magic word"
                        CCI.reject sev
                        return $ Just epls
        CCI.EvSend ctx status _conn -> do
          case ctx of
            0 -> dbg "Unexpected normal send confirmation"
            rmaid -> withMVar (cciEndpointState endpoint) $ \st -> case st of
                       CCIEndpointValid {cciRMAState = rmaState} ->
                         case Map.lookup (fromEnum rmaid) rmaState of
                            Nothing -> dbg "Bogus RMA ID"
                            -- Send the received transmit status to the blocking
                            -- thread in which sendRMA was called. We use
                            -- tryPutMVar here instead of putMVar just in case
                            -- CCI sends redundant send confirmations (as it did
                            -- until recently).
                            Just rmas -> void $ tryPutMVar (cciRMAComplete rmas) status
                       _ -> dbg "Endpoint already dead"
          return $ Just epls
        CCI.EvRecv eb conn -> do
          let connid =
                fromMaybe
                  (throw $ userError $ "Unknown connection in EvRecv: " ++ show conn)
                  (Map.lookup conn connectionsByConnection)
          msg <- CCI.packEventBytes eb
          case ord $ BSC.head msg of
            0 -> do
              putEvent endpoint $ Received connid [BSC.tail msg] -- normal message
              return $ Just epls
            1 -> case decode $ BSL.fromChunks [BSC.tail msg] of  -- control message
              ControlMessageInitConnection rel epa -> do
                 putEvent endpoint (ConnectionOpened connid rel epa)
                 return $ Just epls
              ControlMessageCloseConnection -> do
                 CCI.disconnect conn
                 putEvent endpoint (ConnectionClosed connid)
                 return $ Just epls { eplsConnectionsByConnection =
                                        Map.delete conn connectionsByConnection }
              ControlMessageInitRMA { rmaSize = rmasize
                                    , rmaId = orginatingId } -> do
                mres <- Pool.newBuffer (cciPool endpoint) rmasize
                case mres of
                  Just buffer -> do
                    localhb <- CCI.rmaHandle2ByteString (Pool.getBufferHandle buffer)
                    let notify = ControlMessageAckInitRMA
                                   { rmaAckOrginatingId=orginatingId
                                   , rmaAckRemote = Just (nextTransferId, localhb)
                                   }
                    sendControlMessageInside transport endpoint conn notify
                    return $ Just epls { eplsNextTransferId = nextTransferId + 1
                                       , eplsTransfers = Map.insert nextTransferId
                                                                    buffer
                                                                    transfers
                                       }
                  Nothing -> do
                    sendControlMessageInside
                      transport
                      endpoint
                      conn
                      ControlMessageAckInitRMA
                         { rmaAckOrginatingId=orginatingId
                         , rmaAckRemote = Nothing }
                    return$ Just epls
              ControlMessageAckInitRMA { rmaAckOrginatingId=originatingId
                                       , rmaAckRemote = Nothing} ->
                putRMARemoteHandle originatingId Nothing >> return (Just epls)
              ControlMessageAckInitRMA { rmaAckOrginatingId = originatingId
                                       , rmaAckRemote = Just (remoteId, bs) } ->
                case CCI.createRMARemoteHandle bs of
                  Just remoteHandle -> do
                    putRMARemoteHandle originatingId (Just (remoteHandle, remoteId))
                    return (Just epls)
                  Nothing -> do
                    putRMARemoteHandle originatingId Nothing
                    return (Just epls)
              ControlMessageFinalizeRMA { rmaOk = ok
                                        , rmaRemoteFinalizingId = remoteid } ->
                 case Map.lookup remoteid transfers of
                   Nothing -> do
                     dbg "Bogus transfer id"
                     return (Just epls)
                   Just buffer -> do
                     let pushEvent buffermsg =
                           putEvent endpoint $ Received connid [buffermsg]
                         closeBufferNoCopy :: IO EndpointLoopState
                         closeBufferNoCopy = do
                           if ok
                             then do
                               buffermsg <- Pool.convertBufferToByteString
                                              (cciPool endpoint) buffer
                               pushEvent buffermsg
                             else Pool.freeBuffer (cciPool endpoint) buffer
                           return $ epls { eplsTransfers =
                                             Map.delete remoteid transfers
                                         }
                         _closeBufferReUse :: IO EndpointLoopState
                         _closeBufferReUse = do
                           when ok $ do
                             buffermsg <- Pool.getBufferByteString buffer
                             pushEvent buffermsg
                           Pool.freeBuffer (cciPool endpoint) buffer
                           return $ epls { eplsTransfers =
                                             Map.delete remoteid transfers
                                         }
                     Just <$> closeBufferNoCopy
                     -- TODO there is some ideal tradeoff in message size;
                     -- smaller messages should be have their buffers reuused,
                     -- larger buffers should be handed off to CH without
                     -- copying. (a) At what point do we make this change? (b)
                     -- When do we call Pool.spares to allocate excess buffer
                     -- capacity in advance?
            _ -> do
              dbg "Unknown message type"
              return $ Just epls
        CCI.EvConnect connectionId (Left status) -> do
          -- TODOconnection state to error/invalid/closed
          statusMsg <- CCI.strError (Just $ cciEndpoint endpoint) status
          let errmsg = TransportError ConnectNotFound ("Connection "++show connectionId++" failed because "++statusMsg)
          modifyMVar_ (cciEndpointState endpoint) $ \st -> case st of
            CCIEndpointValid {cciConnectionsById = connections} -> do
              let theconn =
                    -- This conversion (WordPtr to ConnectionId) is probably okay.
                    Map.lookup (toConnectionId connectionId) connections
                  newmap = Map.delete (toConnectionId connectionId) connections
              maybe (dbg $ "Connection " ++
                           show connectionId ++
                           " failed because " ++ statusMsg)
                     (\tc -> putMVar (cciReady tc) (Just errmsg))
                    theconn
              return st { cciConnectionsById = newmap }
            _ -> return st
          return $ Just epls
        CCI.EvConnect connectionId (Right conn) -> do
          withMVar (cciEndpointState endpoint) $ \st -> case st of
            CCIEndpointValid {cciConnectionsById = connectionsById} ->
              case Map.lookup (toConnectionId connectionId) connectionsById of
                Nothing -> dbg $ "Unknown connection ID: " ++ show connectionId
                Just cciconn -> modifyMVar (cciConnectionState cciconn) $ \connstate ->
                  case connstate of
                    CCIConnectionInit -> do
                      let maxmsg = CCI.connMaxSendSize conn
                      let newconnstate = CCIConnectionConnected
                            {cciConnection = conn,
                             cciMaxSendSize = maxmsg}
                      putMVar (cciReady cciconn) Nothing
                      return (newconnstate, ())
                    _ -> do
                      dbg $ "Unexpected EvConnect for connection " ++ show connectionId ++
                            " in state " ++ show connstate
                      return (connstate, ())
            _ -> dbg "Can't handle EvConnect when endpoint is closed"
          return $ Just epls
        CCI.EvAccept connectionId (Left status) -> do
          statusMsg <- CCI.strError (Just $ cciEndpoint endpoint) status
          dbg $ "Failed EvAccept on connection " ++ show connectionId ++
                " because " ++ statusMsg
          return $ Just epls
        CCI.EvAccept connectionId (Right conn) -> do
          -- The connection is not fully ready until it gets an EvAccept.
          -- Therefore, there is a possible race condition if the other
          -- (originating) side receives its EvConnect and tries to send
          -- a message before this (target) side is ready.
          let newmap = Map.insert conn
                                  (toConnectionId connectionId)
                                  connectionsByConnection
          return $ Just epls { eplsConnectionsByConnection = newmap }
        CCI.EvKeepAliveTimedOut _conn -> do
          -- TODO another endpoint has died: tell CH; to do this we need to
          -- learn the endpointaddress for the given conn
          dbg $ show ev
          return $ Just epls
        CCI.EvEndpointDeviceFailed _endp -> do
          -- TODO this endpoint has died: tell CH putErrorEvent endpoint (Just $
          -- getEndpointAddress endpoint) (Map.elems connectionsByConnection)
          -- "Device failed" Should probably kill Endpoint and move
          -- EndpointState to closed
          dbg $ show ev
          return $ Just epls
      case ret of
        Nothing -> do
          CCI.destroyEndpoint (cciEndpoint endpoint)
          putMVar (cciEndpointFinalized endpoint) ()
        Just newstate -> loop newstate
    putRMARemoteHandle originatingId val =
      withMVar (cciEndpointState endpoint) $ \st -> case st of
        CCIEndpointValid {cciRMAState = rmastate} ->
          case Map.lookup originatingId rmastate of
            Nothing -> dbg "Bogus originating id"
            Just myrma -> putMVar (cciRMARemoteHandle myrma) val
        _ -> dbg "Unexpected endpoint state"

-- | Notify CH that something happened. Usually, a connection was opened or
-- closed or a message was received.
putEvent :: CCIEndpoint -> Event -> IO ()
putEvent endp ev = writeChan (cciChannel endp) ev

{-
-- TODO use this to indicate KeepaliveTimeout errors, exceptions thrown from message handling thread,
-- and probably also certain errors (e.g. ETIMEOUT) from send and friends
putErrorEvent :: CCIEndpoint -> Maybe EndPointAddress -> [ConnectionId] -> String -> IO ()
putErrorEvent endpoint mepa lcid why =
   let err = EventConnectionLost mepa lcid
    in putEvent endpoint (ErrorEvent (TransportError err why))
-}

magicEndpointShutdown :: String
magicEndpointShutdown = "shutdown"

-- | First we shutdown individual connections. Then we send a message to the
-- endpoint's event handler by embedding a shutdown request in a connection
-- request. The request is formally rejected, but is interpreted as a kill. The
-- event handler passes on the request to CH. This is all done synchronously, as
-- we hold a lock on the endpoint for the duration.
apiCloseEndPoint :: CCITransport -> CCIEndpoint -> IO ()
apiCloseEndPoint transport endpoint = catch closeit handler
  where
    handler :: CCI.CCIException -> IO ()
    handler _ = return ()
    helloPacket = BSC.pack magicEndpointShutdown
    closeit =
       modifyMVar_ (cciEndpointState endpoint) $ \st ->
         case st of
           CCIEndpointValid {cciConnectionsById = connections} -> do
             putEvent endpoint EndPointClosed
             mapM_ (swallowException . closeIndividualConnection transport endpoint)
                   (Map.elems connections)
             swallowException $ do
                sendSignalByConnect transport
                                    endpoint
                                    (getEndpointAddress endpoint)
                                    helloPacket
                takeMVar (cciEndpointFinalized endpoint)
             modifyMVar_ (cciLocalEndpoints transport) $ \eps ->
                  return $ Map.delete (address $ cciTransportEndpoint endpoint) eps
             return CCIEndpointClosed
           _ -> dbg "Endpoint already closed" >> return st


sendSignalByConnect :: CCITransport
                    -> CCIEndpoint
                    -> EndPointAddress
                    -> ByteString
                    -> IO ()
sendSignalByConnect transport endpoint epaddr bs =
      timeoutMaybe someTimeout theerror go
  where go = CCI.connect (cciEndpoint endpoint)
              (BSC.unpack $ endPointAddressToByteString epaddr)
              bs (translateReliability ReliableOrdered)
              (0::WordPtr) someTimeout
        someTimeout :: Integral a => Maybe a
        someTimeout = fmap fromIntegral $ cciConnectionTimeout $ cciParameters transport
        theerror = TransportError ConnectTimeout "Connection reply timeout"

-- | The procedure for creating a new connection is:
--
--   1. @apiConnect(Local)@ creates new connection ID
--
--   2. @apiConnect(Local)@ calls 'CCI.connect', which sends an 'EvConnectRequest'
--   message to the remote side.
--
--   3. 'EvConnectRequest' is received by remote, which creates new remote ID
--   and calls 'accept'
--
--   4. @accept(Remote)@ sends 'EvAccept' to remote (target) side and
--   'EvConnect' to local (originating) side
--
--   5. When the remote side gets 'EvAccept', it registers the new connection
--   with its ID
--
--   6. 'apiConnect' waits for 'EvConnect' message, after which is sends
--   a 'ControlMessageInitConnection' message on newly-created connection; this
--   message contains reliability and endpoint information that is needed for
--   the remote side to send on to CH
--
--   7. Future 'EvReceives' on the Remote side (caused by receiving a message)
--   will result in event being placed on the remote endpoint queue.
apiConnect :: CCITransport
           -> CCIEndpoint
           -> EndPointAddress
           -> Reliability
           -> ConnectHints
           -> IO (Either (TransportError ConnectErrorCode) Connection)
apiConnect transport endpoint remoteaddress reliability _hints =
    try $ mapCCIException (translateException ConnectFailed) $ do
      (cciconn, transportconn, _cid) <- createConn
      timeoutMaybe maybeTimeout
                   (TransportError ConnectTimeout "Connection reply timeout")
                   (waitReady cciconn)
      void $ sendControlMessage transport
                         endpoint
                         cciconn
                         (ControlMessageInitConnection reliability
                                                       (getEndpointAddress endpoint))
      -- putEvent endpoint (ConnectionOpened cid reliability remoteaddress) --I am pretty sure this line is not necessary
      return transportconn
   where
     maybeTimeout :: Integral a => Maybe a
     maybeTimeout =
       -- OR, alternatively: fmap fromIntegral $ connectTimeout hints
       fmap fromIntegral $ cciConnectionTimeout (cciParameters transport)
     createConn =
       modifyMVar (cciEndpointState endpoint) $ \st ->
         case st of
           CCIEndpointValid
             { cciNextConnectionId = cid
             , cciConnectionsById = connById } -> do
               connstate <- newMVar CCIConnectionInit
               ready <- newEmptyMVar
               let newmapById = Map.insert cid theconn connById
                   newst = st { cciNextConnectionId = cid+1
                              , cciConnectionsById = newmapById }
                   theconn = CCIConnection
                     { cciConnectionId = cid
                     , cciConnectionState = connstate
                     , cciReady = ready}
                   transportconn = Connection
                     { send  = apiSend transport endpoint theconn
                     , close = apiCloseConnection transport endpoint theconn
                     }
               requestConnection (fromIntegral cid) -- this conversion (Int to WordPtr) is probablay okay
               return (newst, (theconn,transportconn, cid))
           _ -> throwIO $ TransportError ConnectFailed "Endpoint invalid"
     requestConnection :: WordPtr -> IO ()
     requestConnection contextId =
       let localEndpoint = cciEndpoint endpoint
           remoteAddress = BSC.unpack $ endPointAddressToByteString remoteaddress
           helloPacket   = BSC.empty
       in CCI.connect localEndpoint
                      remoteAddress
                      helloPacket
                      (translateReliability reliability)
                      contextId
                      maybeTimeout

-- | Sending is silent (viz. 'CCI.SEND_SILENT'). This corresponds to the
-- Unified's preference. We distinguish control messages from regular messages
-- by the first byte: 0 for the latter, 1 for the former. Since we will also use
-- control messages to provide RMA admin, we can't send them on RMA.
apiSend :: CCITransport
        -> CCIEndpoint
        -> CCIConnection
        -> [ByteString]
        -> IO (Either (TransportError SendErrorCode) ())
apiSend transport endpoint conn bs =
  sendCore transport endpoint conn (0::WordPtr) False bs

sendControlMessage :: CCITransport
                   -> CCIEndpoint
                   -> CCIConnection
                   -> ControlMessage
                   -> IO (Either (TransportError SendErrorCode) ())
sendControlMessage transport endpoint conn bs =
  sendCore transport endpoint conn (0::WordPtr) True (BSL.toChunks (encode bs)) >>= dbgEither

sendControlMessageInside :: CCITransport
                         -> CCIEndpoint
                         -> CCI.Connection
                         -> ControlMessage
                         -> IO ()
sendControlMessageInside _transport _endpoint conn bs =
   sendSimple conn (BSC.singleton (chr 1):BSL.toChunks (encode bs)) (0::WordPtr)

-- | CCI has a maximum message size of 'maxMessageLength'. For short messages,
-- we will send them the normal way, but for longer messages we are obligated to
-- use CCI's RMA mechanism.
sendCore :: CCITransport
         -> CCIEndpoint
         -> CCIConnection
         -> WordPtr
         -> Bool
         -> [ByteString]
         -> IO (Either (TransportError SendErrorCode) ())
sendCore transport endpoint conn context isCtrlMsg bs =
    try $ mapCCIException (translateException SendFailed) $
       withMVar (cciConnectionState conn) $ \st ->
          case st of
            CCIConnectionConnected
              { cciMaxSendSize = maxMessageLength
              , cciConnection = realconnection } ->
              if messageLength > fromIntegral maxMessageLength && not isCtrlMsg
              then sendRMA transport endpoint conn realconnection bs context
              else sendSimple realconnection augmentedbs context
            CCIConnectionInit -> do
              dbg "Connection not initialized"
              throwIO $ TransportError SendClosed "Connection not initialized"
            CCIConnectionClosed -> do
              dbg "Connection already closed"
              throwIO $ TransportError SendClosed "Connection already closed"
  where
    messageLength = sum (map BSC.length augmentedbs)
    augmentedbs = msgprefix:bs
    msgprefix = BSC.singleton $ chr $ if isCtrlMsg then 1 else 0

-- | For RMA transmissions, we do this:
--
--   1. Allocate a local transfer ID on this endpoint.
--
--   2. Send an 'InitRMA' message to the other side, along with the size of the
--   buffer we want to send.
--
--   3. Remote side allocates buffer, send back an acknowledgement with its
--   remote buffer handle and its remote transfer ID.
--
--   4. We move the message to a local RMA buffer, and call 'rmaWrite' to send
--   the data to the remote buffer.
--
--   5. We wait for transmission confirmation via an 'EvSend' with our local
--   transfer ID.
--
--   6. After we get confirmation, we can release the local buffer.
--
--   7. The other side copies its buffers to a message, sends it to CH, and
--   frees its buffers.
sendRMA :: CCITransport
        -> CCIEndpoint
        -> CCIConnection
        -> CCI.Connection
        -> [ByteString]
        -> WordPtr
        -> IO ()
sendRMA transport endpoint _conn realconn bs _ctx = do
    (rmatid,rmastate) <- newRMA
    let -- Tell the remote side, if we can, to free the buffer corresponding to
        -- the given ID. If ok, then the buffer should be complete, create
        -- a message from it. Otherwise, throw it away.
        freeRemoteBuffer remoteid ok =
          swallowException $
          sendControlMessageInside transport endpoint realconn $
          finalizeMessage ok remoteid

        -- When we're sending the last chunk to the other side, we ask CCI to
        -- deliver this finalization message, which is interpreted as a control
        -- message, signalling the receiver to extract the data, send it to CH,
        -- and shut down the corresponding buffer. The leading '\1' identifies it
        -- as a control message.
        finalizeMessage ok remoteid =
          ControlMessageFinalizeRMA {rmaOk = ok, rmaRemoteFinalizingId = remoteid}
        encodedFinalization ok remoteid =
          BSC.concat (BSC.singleton '\1' : (BSL.toChunks $ encode $ finalizeMessage ok remoteid))

        eraseRmaState =
          modifyMVar_ (cciEndpointState endpoint) $ \st ->
            return st { cciRMAState = Map.delete rmatid (cciRMAState st) }

        -- Coordinate RMA handles with the other side. We tell receiver how big
        -- the complete message is. It allocates a buffer and sends us back
        -- a handle. We wait for it, throwing if we exceed the timeout.
        getRemoteHandle =
          let maybeTimeout =
                fmap fromIntegral $ cciConnectionTimeout $ cciParameters transport
              err = TransportError SendFailed "RMA partner failed to respond"
              sendInitMsg = sendControlMessageInside transport endpoint realconn initMsg
              initMsg = ControlMessageInitRMA
                { rmaSize = msgLength
                , rmaId = rmatid
                , rmaEndpointAddress = cciUri endpoint }
           in timeoutMaybe maybeTimeout err $ do
                sendInitMsg
                res <- takeMVar (cciRMARemoteHandle rmastate)
                case res of
                  Just n -> return n
                  Nothing -> throwIO $ TransportError SendFailed "Couldn't allocate remote buffer"
        doRMA = bracketOnError
                  getRemoteHandle
                  (\(_, remoteid) -> freeRemoteBuffer remoteid False)
                  (\(remoteh, remoteid) ->
                     withRMABuffer endpoint allbs CCI.RMA_READ $ \localh -> do
                        let chunksAndFlags = reverse (zip3 (reverse chunks)
                                                     ([CCI.RMA_FENCE] : repeat [CCI.RMA_SILENT])
                                                     ((Just $ encodedFinalization True remoteid) : repeat Nothing))

                        -- Break the original message to chunks of the allowed
                        -- send size. The chunks are sent with separate rmaWrite
                        -- calls. The flags are important: the last chunk is
                        -- marked RMA_FENCE, and all other messages are marked
                        -- RMA_SILENT. This means (a) we only get confirmation
                        -- message for the last chunk and (b) the confirmation
                        -- message for the last chunk carries a guarantee that all
                        -- preceeding chunks have been sent.
                        forM_ chunksAndFlags $ \((buffOffset,buffLength),opts,finalizer) ->
                          CCI.rmaWrite realconn
                                       finalizer
                                       remoteh
                                       (fromIntegral buffOffset)
                                       localh
                                       (fromIntegral buffOffset)
                                       (fromIntegral buffLength)
                                       (toEnum rmatid::WordPtr)
                                       opts

                        -- We need to wait until we get a send confirmation from
                        -- CCI about the lsat chunk before it's okay to release
                        -- the buffer. If we get an error from sending, throw.
                        takeMVar (cciRMAComplete rmastate) >>= throwStatus)
    doRMA `finally` eraseRmaState -- probably shold mask exceptions here
  where
    newRMA = modifyMVar (cciEndpointState endpoint) $ \st -> case st of
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
    throwStatus CCI.SUCCESS = return ()
    throwStatus status =
        throwIO $ TransportError SendFailed ("EvSend reported error " ++ show status)

    allbs = BSC.concat bs
    msgLength = BSC.length allbs

    chunkSize :: Word32
    chunkSize =
      let notZero 0 = Nothing
          notZero n = Just n
      in minimum $ catMaybes [ Just (maxBound :: Word32)
                             , notZero $ fromIntegral $ CCI.rmaWriteLength (cciRMAAlignments endpoint)
                             , notZero $ fromIntegral $ CCI.rmaReadLength (cciRMAAlignments endpoint) ]

    -- A list of chunks to send, indicated as the offset into the buffer and
    -- size. Chunksize should be limited by the maximum RMA buffer size (set in
    -- CCIParameters) and maximum read/write length (gotten from RMAAlignments).
    -- I'm not sure if we need to obey both rmaWriteLength and rmaReadLength,
    -- but playing it safe by taking the minimum seems wise.
    chunks =
        let (fullchunks,partialchunk) = fromIntegral msgLength `divMod` chunkSize
            maybePartialChunk =
              if partialchunk==0
              then []
              else [(fullchunks*chunkSize,partialchunk)]
        in genericTake fullchunks [ (offsets,chunkSize) | offsets<-[0,chunkSize..]] ++
           maybePartialChunk

-- TODO: draw buffers from a pool of locally stored buffers, rather than
-- registering them each time;
--
-- TODO: test performance of alloc-on-demand versus copying into buffer pool
withRMABuffer :: CCIEndpoint
              -> ByteString
              -> CCI.RMA_MODE
              -> (CCI.RMALocalHandle -> IO a) -> IO a
withRMABuffer endpoint bs mode f =
    unsafeUseAsCStringLen bs $ \cstr -> do
      mbuf <- Pool.lookupBuffer (fst cstr) (cciPool endpoint)
      case mbuf of
        Nothing ->
          -- TODO this buffer should be aligned (to something)
          CCI.withRMALocalHandle (cciEndpoint endpoint) cstr mode f
        Just buf ->
          f $ Pool.bHandle buf

sendSimple :: CCI.Connection
           -> [ByteString]
           -> WordPtr
           -> IO ()
-- TODO find out why tests don't pass if CCI.sendvSilent is used instead
sendSimple conn bs wp = retryCCI_ENOBUFS $ CCI.sendvBlocking conn bs wp

retryCCI_ENOBUFS :: IO a -> IO a
retryCCI_ENOBUFS action = catch action
     $ \e@(CCI.CCIException ret) ->
         if ret == CCI.ENOBUFS || ret == CCI.EAGAIN then do
           threadDelay 10000
           retryCCI_ENOBUFS action
          else
           throwIO e

-- | 'CCI.disconnect' is a local operation; it doesn't notify the other side, so
-- we have to. We send a control message to the other side, which unregisters
-- and disconnects the remote endpoint, then do the same here
apiCloseConnection :: CCITransport
                   -> CCIEndpoint
                   -> CCIConnection
                   -> IO ()
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

-- | Takes care of the work of shutting down the connection (from the originator
-- side), including sending a shutdown message to the server (target). Does not,
-- however, update the connection table in the endpoint - use
-- 'apiCloseConnection' for most purposes.
closeIndividualConnection :: CCITransport
                          -> CCIEndpoint
                          -> CCIConnection
                          -> IO ()
closeIndividualConnection _transport _endpoint conn =
              do -- sendControlMessage transport endpoint conn (ControlMessageCloseConnection)
                 transportconn <- modifyMVar (cciConnectionState conn) $ \connst ->
                    case connst of
                      CCIConnectionConnected {cciConnection=realconn} ->
                         return (CCIConnectionClosed,Just realconn)
                      CCIConnectionClosed -> dbg "Connection is already closed" >>
                         return (CCIConnectionClosed,Nothing)
                      CCIConnectionInit -> dbg "Connection still initializing"  >>
                         return (CCIConnectionClosed,Nothing)
                 -- putEvent endpoint (ConnectionClosed $ cciConnectionId conn) -- I am also pretty sure this line is not necessary
                 flip (maybe (return ())) transportconn $ \cciConn -> do
                   retryCCI_ENOBUFS $ CCI.sendvBlocking cciConn
                       ( BSC.singleton (chr 1)
                       : BSL.toChunks (encode ControlMessageCloseConnection)
                       )
                       0
                   CCI.disconnect cciConn

translateReliability :: Reliability -> CCI.ConnectionAttributes
translateReliability ReliableOrdered = CCI.CONN_ATTR_RO
translateReliability ReliableUnordered = CCI.CONN_ATTR_RU
translateReliability Unreliable = CCI.CONN_ATTR_UU

receiveEvent :: CCITransport
             -> CCIEndpoint
             -> (forall s. CCI.EventData s -> IO a)
             -> IO a
receiveEvent transport endpoint f =
   case cciReceiveStrategy (cciParameters transport) of
     AlwaysPoll -> receivePoll
     AlwaysBlock -> receiveBlock
   where receivePoll = CCI.pollWithEventData (cciEndpoint endpoint) f
         receiveBlock = CCI.withEventData (cciEndpoint endpoint)
                                          (cciFileDescriptor endpoint) f

-- TODO more precise translation of CCI.CCIException to TransportError
translateException :: a -> CCI.CCIException -> TransportError a
translateException a ex = TransportError a (show ex)

mapCCIException :: Exception e => (CCI.CCIException -> e) -> IO a -> IO a
mapCCIException f p = catch p (throwIO . f)

swallowException :: IO () -> IO ()
swallowException a = a `catch` handler
  where handler :: SomeException -> IO ()
        handler _ = return ()
