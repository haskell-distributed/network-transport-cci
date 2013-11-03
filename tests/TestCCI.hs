module Main where

import Control.Applicative ((<$>))

import Network.Transport
import Network.Transport.CCI
import Network.Transport.Tests
import Network.Transport.Tests.Auxiliary (runTests)

main :: IO ()
main = testTransport' (either (Left . show) (Right) <$> createTransport defaultCCIParameters)

testTransport' :: IO (Either String Transport) -> IO ()
testTransport' newTransport = do
  Right transport <- newTransport
  runTests
    [ ("PingPong",              testPingPong transport numPings)
    , ("EndPoints",             testEndPoints transport numPings)
    , ("Connections",           testConnections transport numPings)
    -- FIXME: Test stalls sometimes
    --, ("CloseOneConnection",    testCloseOneConnection transport numPings)
    , ("CloseOneConnection",      undefined)
    , ("CloseOneDirection",     testCloseOneDirection transport numPings)
    , ("CloseReopen",           testCloseReopen transport numPings)
    -- FIXME: Fails, but causes tests to stop executing
    -- , ("ParallelConnects",      testParallelConnects transport numPings)
    , ("ParallelConnects",      undefined)
    , ("SendAfterClose",        testSendAfterClose transport 100)
    , ("Crossing",              testCrossing transport 10)
    -- Fails sometimes, appears to be CCI bug?
    --, ("CloseTwice",            testCloseTwice transport 100)
    , ("ConnectToSelf",         testConnectToSelf transport numPings)
    , ("ConnectToSelfTwice",    testConnectToSelfTwice transport numPings)
    -- Waiting on parallel-haskell mailing list RE. semantics
    --, ("CloseSelf",             testCloseSelf newTransport)
    --, ("CloseEndPoint",         testCloseEndPoint transport numPings)
    -- FIXME: Test stalls
    --, ("CloseTransport",        testCloseTransport newTransport)
    , ("CloseTransport",      undefined)
    , ("ConnectClosedEndPoint", testConnectClosedEndPoint transport)
    -- FIXME: Test stalls
    --, ("ExceptionOnReceive",    testExceptionOnReceive newTransport)
    , ("ExceptionOnReceive",      undefined)
    , ("SendException",         testSendException newTransport)
    , ("Kill",                  testKill newTransport 1000)
    ]
  where
    numPings = 10000 :: Int
