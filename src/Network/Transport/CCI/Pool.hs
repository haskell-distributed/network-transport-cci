-- |Pool.hs
--
-- Copyright (C) 2012 Parallel Scientific, Inc
-- Please see the accompanying LICENSE file or contact Parallel Scientific, Inc for licensing details.
--
-- Support for managing pools of CCI transfer buffers
module Network.Transport.CCI.Pool
  (
    Pool,
    Buffer,
    newPool,
    freePool,
    newBuffer,
    freeBuffer,
    getBufferHandle,
    getBufferByteString
  ) where

import Prelude hiding (catch)
import Control.Exception (catch, IOException)
import Control.Applicative ((<$>))
import Data.Map (Map)
import qualified Data.Map as Map
import Network.CCI (RMALocalHandle, RMA_MODE, rmaRegister, rmaDeregister, Endpoint)
import Foreign.C.Types (CChar)
import Foreign.Ptr (Ptr,alignPtr)
import Foreign.Marshal.Alloc (mallocBytes, free)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.C.String (CStringLen)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)
import qualified Data.List as List (find, delete, deleteBy, insertBy)
import qualified Data.ByteString.Char8 as BSC
import Data.ByteString.Char8 (ByteString)
import Data.Ord (comparing)
import Data.Maybe (fromJust)

type BufferId = Int

data Buffer = Buffer
     {
        bId :: BufferId,
        bAllocStart :: Ptr CChar,
        bStart :: Ptr CChar,
        bSize :: Int,
        bHandle :: RMALocalHandle
     }

data Pool = Pool
     {
        pNextId :: !BufferId,
        pMaxBufferCount :: Int,
        pAlign :: Int,
        pMode :: RMA_MODE,
        pEndpoint :: Endpoint,

        pInUse :: Map BufferId Buffer,
        pAvailableBySize :: [Buffer],
        pAvailableLru :: [BufferId]
     }

getBufferHandle :: Buffer -> RMALocalHandle
getBufferHandle = bHandle

dbg :: String -> IO ()
dbg = putStrLn

freePool :: Pool -> IO ()
freePool pool =
  let inuse = Map.elems (pInUse pool)
      notinuse = pAvailableBySize pool
   in mapM_ (destroyBuffer pool) (inuse++notinuse)

newPool :: Endpoint -> Int -> Int -> RMA_MODE -> Pool
newPool endpoint alignment maxbuffercount mode =
  Pool {pNextId=0,
        pMaxBufferCount=maxbuffercount,
        pAlign=alignment,
        pMode = mode,
        pEndpoint = endpoint,
        pInUse=Map.empty,
        pAvailableBySize=[],
        pAvailableLru=[]}

freeBuffer :: Pool -> Buffer -> IO Pool
freeBuffer pool buffer = 
  case Map.lookup (bId buffer) (pInUse pool) of
    Just buf | bSize buf == bSize buffer -> 
       let newpool = pool {
                       pInUse = Map.delete (bId buffer) (pInUse pool),
                       pAvailableBySize = List.insertBy (comparing bSize) buffer (pAvailableBySize pool),
                       pAvailableLru = bId buf : pAvailableLru pool
                      }
        in return newpool
    _ -> dbg "Trying to free buffer that I don't know about" >> return pool

cleanup :: Pool -> IO Pool
cleanup pool =
  if (pMaxBufferCount pool < length (pAvailableLru pool)) 
     then
        let killme = let killmeId = last (pAvailableLru pool)
                      in fromJust $ List.find (\b -> bId b == killmeId) (pAvailableBySize pool)
            newpool = pool { pAvailableLru = init $ pAvailableLru pool,
                             pAvailableBySize = List.deleteBy byId killme (pAvailableBySize pool)}
         in do destroyBuffer pool killme
               return newpool
     else return pool 
    where byId a b = bId a == bId b

destroyBuffer :: Pool -> Buffer -> IO ()
destroyBuffer pool buffer =
 do rmaDeregister (pEndpoint pool) (bHandle buffer)
    freeAligned ((bStart buffer,bSize buffer),bAllocStart buffer)


newBuffer :: Pool -> Either Int ByteString -> IO (Maybe (Pool, Buffer))
newBuffer pool content = 
   case findAndRemove goodSize (pAvailableBySize pool) of
      (_newavailable,Nothing) ->
         do mres <- allocAligned' (pAlign pool) (neededSize content)
            case mres of
              Nothing -> return Nothing
              Just (cstr@(start,_),allocstart) -> do
                case content of
                  Right bs ->
                    copyTo bs start
                  Left _ -> return ()
                handle <- rmaRegister (pEndpoint pool) cstr (pMode pool)
                let newbuf = 
                       Buffer {
                         bId = pNextId pool,
                         bAllocStart = allocstart,
                         bStart = start,	
                         bSize = neededSize content,
                         bHandle = handle
                      }
                    newpool = pool {
                         pNextId = (pNextId pool)+1,
                         pInUse = Map.insert (bId newbuf) newbuf (pInUse pool)
                      }
                cleanpool <- cleanup newpool
                return $ Just (cleanpool, newbuf)
      (newavailable,Just buf) -> 
         let newpool = pool {pAvailableBySize = newavailable,
                             pInUse = Map.insert (bId buf) buf (pInUse pool),
                             pAvailableLru = List.delete (bId buf) (pAvailableLru pool)}
          in do case content of
                  Right bs ->
                    copyTo bs (bStart buf)
                  Left _ -> return ()
                cleanpool <- cleanup newpool
                return $ Just (cleanpool, buf)
    where goodSize b = bSize b >= (neededSize content)
          neededSize (Left n) = n
          neededSize (Right str) = BSC.length str

freeAligned :: (CStringLen, Ptr CChar) -> IO ()
freeAligned (_,actual) = free actual

allocAligned' :: Int -> Int -> IO (Maybe (CStringLen, Ptr CChar))
allocAligned' a s =
   catch (Just <$> allocAligned a s) (\x -> const (return Nothing) (x::IOException))

allocAligned :: Int -> Int -> IO (CStringLen, Ptr CChar)
allocAligned 0 size = mallocBytes size >>= \p -> return ((p,size),p)
allocAligned align size =
     do ptr <- mallocBytes maxSize
        return ((alignPtr ptr align, size), ptr)
  where maxSize = size + align - 1

copyTo :: ByteString -> Ptr CChar -> IO ()
copyTo bs pstr =
   unsafeUseAsCStringLen bs $ \(cs,_) -> 
       copyBytes pstr cs (BSC.length bs)

{-
moveToFront :: Eq a => a -> [a] -> [a]
moveToFront x xs = go [] xs
  where go before [] = x:reverse before
        go before (z:zs) | z==x = z:(reverse before) ++ zs
        go before (z:zs) = go (z:before) zs

-}

getBufferByteString :: Buffer -> IO ByteString
getBufferByteString buffer =
   BSC.packCStringLen (bStart buffer, bSize buffer)

findAndRemove :: (a -> Bool) -> [a] -> ([a], Maybe a)
findAndRemove f xs = go [] xs
   where go before [] = (reverse before,Nothing)
         go before (x:after) | f x = ((reverse before)++after,Just x)
         go before (x:after) = go (x:before) after

