module Main where

-- #if ! MIN_VERSION_base(4,6,0)
import Prelude hiding (catch)
-- #endif

import Data.Binary (Binary(..))
import Data.Typeable (Typeable)
import Data.Foldable (forM_)
import Control.Concurrent (threadDelay,ThreadId,myThreadId)
import Control.Concurrent.MVar
  ( MVar
  , newEmptyMVar
  , putMVar
  , takeMVar
  , readMVar
  )
import Data.List (stripPrefix,findIndices)
import Control.Distributed.Process.Internal.Types
import Control.Monad (replicateM_, replicateM)
import Control.Exception (throwIO,SomeException, throwTo)
import Control.Applicative ((<$>), (<*>))
import qualified Network.Transport as NT (Transport, closeEndPoint,EndPointAddress(..))
import Network.Transport.CCI (createTransport, defaultCCIParameters)
import Control.Distributed.Process
import Control.Distributed.Process.Internal.Types (LocalNode(localEndPoint))
import qualified Data.ByteString.Char8 as BSC (pack)
import Control.Distributed.Process.Node
import Control.Distributed.Process.Serializable (Serializable)
import System.Environment (getArgs)
import TestAuxiliary

newtype Ping = Ping ProcessId
  deriving (Typeable, Binary, Show)

newtype Pong = Pong ProcessId
  deriving (Typeable, Binary, Show)

data BigPing = BigPing ProcessId String
  deriving (Typeable, Show)

data BigPong = BigPong ProcessId String
  deriving (Typeable, Show)

instance Binary BigPing where
  put (BigPing a b) = put a >> put b
  get = do a <- get
           b <- get
           return $ BigPing a b
instance Binary BigPong where
  put (BigPong a b) = put a >> put b
  get = do a <- get
           b <- get
           return $ BigPong a b


showProcessId :: ProcessId -> String
showProcessId (ProcessId (NodeId addr) (LocalProcessId unique lid))
    = "pid://" ++ show addr ++ ":" ++ show lid ++ "!" ++ show unique

readProcessId :: String -> ProcessId
readProcessId str =
 let Just processaddr = stripPrefix "pid://" str
     (mainpid,'!':unique) = splitAt (last (findIndices (=='!') processaddr)) processaddr
     (nid,':':lpid) = splitAt (last (findIndices (==':') mainpid)) mainpid
     remoteNode = NodeId (NT.EndPointAddress $ BSC.pack nid)
     remotePid = ProcessId remoteNode (LocalProcessId {lpidUnique=read unique, lpidCounter=read lpid})
  in remotePid


-- | The big ping server from the paper
bigPing :: Process ()
bigPing = do
  BigPong partner fill <- expect
  self <- getSelfPid
  send partner (BigPing self fill)
  bigPing

pingSize, numPings :: Int
pingSize = 50000
numPings = 1000

server :: NT.Transport -> MVar ProcessId -> IO ()
server transport serverAddr = do
  node <- newLocalNode transport initRemoteTable
  tryRunProcess node $ do
    addr <- spawnLocal bigPing
    liftIO $ putMVar serverAddr addr

client :: NT.Transport -> ProcessId -> IO ()
client transport pingServer =
  do node <- newLocalNode transport initRemoteTable
     tryRunProcess node $ replicateM_ numPings pinger
    where
          pinger =
              do pid <- getSelfPid
                 send pingServer (BigPong pid (replicate pingSize '!'))
                 BigPing _ _ <- expect
                 return ()

wait = getLine >> return ()

main :: IO ()
main = do
  Right transport <- createTransport defaultCCIParameters
  params <- getArgs
  mpid <- newEmptyMVar
  case params of
    [] -> do server transport mpid
             pid <- takeMVar mpid
             putStrLn $ showProcessId pid
             wait
    ["self"] -> do server transport mpid
                   pid <- takeMVar mpid
                   client transport pid
                   wait
    [addr] -> do let pid = readProcessId addr
                 client transport pid
                 wait
