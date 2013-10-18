{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}
module Main where

import TestTransport (testTransport)
import Network.Transport.CCI
    ( createTransport
    , defaultCCIParameters
    )

main :: IO ()
main = do
  -- TODO run CCI-specific tests
  -- Run the generic tests even if the TCP specific tests failed..
  Right transport <- createTransport defaultCCIParameters
  testTransport transport
  -- ..but if the generic tests pass, still fail if the specific tests did not
