-- |
-- Copyright: (C) Parallel Scientific Labs, LLC.
-- License: BSD3
--
-- Runner for given CH test suite. The test suite to run is given by the
-- expansion of the @TEST_SUITE_MODULE@ macro.

module Main where

import TEST_SUITE_MODULE (tests)

import Network.Transport.Test (TestTransport(..))
import Network.Transport.CCI
  ( createTransport
  , defaultCCIParameters
  )
import Test.Framework (defaultMain)

main :: IO ()
main = do
    Right transport <- createTransport defaultCCIParameters
    defaultMain =<< tests TestTransport
      { testTransport = transport
      , testBreakConnection = \_ _ -> do
          error "Unimplemented."
      }
