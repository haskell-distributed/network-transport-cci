{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import Prelude hiding 
  ( catch )
import TestTransport (testTransport) 
import TestAuxiliary (forkTry, runTests)
import Network.Transport
import Network.Transport.CCI ( createTransport
                             , defaultCCIParameters
                             )
import Data.Int (Int32)
import Control.Concurrent (threadDelay, killThread)
import Control.Concurrent.MVar ( MVar
                               , newEmptyMVar
                               , putMVar
                               , takeMVar
                               , readMVar
                               , isEmptyMVar
                               , newMVar
                               , modifyMVar
                               )
import Control.Monad (replicateM, guard, forM_, replicateM_, when)
import Control.Applicative ((<$>))
import Control.Exception (throwIO, try, SomeException)
import Network.Transport.Internal ( encodeInt32
                                  , prependLength
                                  , tlog
                                  , tryIO
                                  , void
                                  )
import Data.String (fromString)
import GHC.IO.Exception (ioe_errno)
import Foreign.C.Error (Errno(..), eADDRNOTAVAIL)
import System.Timeout (timeout)

main :: IO ()
main = do
  -- TODO run CCI-specific tests
  -- Run the generic tests even if the TCP specific tests failed.. 
  Right transport <- createTransport defaultCCIParameters
  testTransport transport
  -- ..but if the generic tests pass, still fail if the specific tests did not

