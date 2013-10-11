-- |
-- Copyright : (C) 2012-2013 Parallel Scientific Labs, LLC.
-- License   : BSD3
--
-- Support for managing pools of CCI transfer buffers.

module Network.Transport.CCI.Pool
  ( Pool
  , Buffer
  , newPool
  , freePool
  , newBuffer
  , freeBuffer
  , getBufferHandle
  , getBufferByteString
  , convertBufferToByteString
  ) where

import Network.Transport.CCI.ByteString (unsafePackMallocCStringLen)

import Control.Applicative ((<$>))
import Control.Exception (catch, IOException)

import Data.Map (Map)
import qualified Data.Map as Map

import qualified Data.ByteString.Char8 as BSC
import Data.ByteString.Char8 (ByteString)
import Data.ByteString.Unsafe (unsafeUseAsCStringLen)

import qualified Data.List as List (find, delete, deleteBy, insertBy)
import Data.Maybe (fromJust)
import Data.Ord (comparing)
import Foreign.C.Types (CChar)
import Foreign.C.String (CStringLen)
import Foreign.Marshal.Alloc (mallocBytes, free)
import Foreign.Marshal.Utils (copyBytes)
import Foreign.Ptr (Ptr,alignPtr)
import Prelude

type BufferId = Int

-- TODO call spares (somewhwhere??) to allocate buffers in advance of their need

-- | A buffer, identified by a handle. With this handle,
-- we can deallocate with 'freeBuffer', we can get its
-- contents with 'getBufferByteString' and we can get
-- its handle value with 'getBufferHandle'
data Buffer handle = Buffer
     {
        bId :: BufferId,
        bAllocStart :: Ptr CChar,
        bStart :: Ptr CChar,
        bSize :: Int,
        bHandle :: handle
     }

-- | A collection of managed buffers, parameterized by the
-- type of the handle that is created when a buffer is
-- registered. In CCI's case, that is RMALocalHandle.
data Pool handle = Pool
     {
        pNextId :: !BufferId,
        pMaxBufferCount :: Int,
        pAlign :: Int,
        pRegister :: CStringLen -> IO handle,
        pUnregister :: handle -> IO (),

        pInUse :: Map BufferId (Buffer handle),
        pAvailableBySize :: [Buffer handle],
        pAvailableLru :: [BufferId]
     }

-- | Returns the handle of the given buffer
getBufferHandle :: Buffer handle -> handle
getBufferHandle = bHandle

dbg :: String -> IO ()
dbg = putStrLn

-- | Deallocates and unregisters all buffers managed
-- by the given pool.
freePool :: Pool handle -> IO ()
freePool pool =
  let inuse = Map.elems (pInUse pool)
      notinuse = pAvailableBySize pool
   in mapM_ (destroyBuffer pool) (inuse++notinuse)

-- | Create a new pool. All buffers will be aligned at the
-- given alignment (or 0 for any alignment). Allocated, but unused
-- buffers will be harvested after the given max count. All, the
-- user provides two functions for registering and registering
-- handles, which are called when buffers are allocated and deallocated.
newPool :: Int -> Int -> (CStringLen -> IO handle) -> (handle -> IO ()) -> Pool handle
newPool alignment maxbuffercount reg unreg =
  Pool {pNextId=0,
        pMaxBufferCount=maxbuffercount,
        pAlign=alignment,
        pRegister = reg,
        pUnregister = unreg,
        pInUse=Map.empty,
        pAvailableBySize=[],
        pAvailableLru=[]}

-- | Release the given buffer. It won't be unregistered
-- and deallocated immediately, but simply placed on the
-- available list.
freeBuffer :: Pool handle -> Buffer handle -> IO (Pool handle)
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

-- | Allocate excess buffers up to our limit
spares :: Pool handle -> Int -> IO (Pool handle)
spares pool defaultsize =
  if (pMaxBufferCount pool > length (pAvailableLru pool))
     then do res <- newBuffer pool (Left defaultsize)
             case res of
                Just (newpool, newbuf) ->
                    freeBuffer newpool newbuf
                Nothing -> return pool
     else return pool

-- | Remove and destroy excess buffers beyond our limit
cleanup :: Pool handle -> IO (Pool handle)
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

destroyBuffer :: Pool handle -> Buffer handle -> IO ()
destroyBuffer pool buffer =
 do (pUnregister pool) (bHandle buffer)
    freeAligned ((bStart buffer,bSize buffer),bAllocStart buffer)

-- | Find an available buffer of the appropriate size, or
-- allocate a new one if such a buffer is not already allocated.
-- You will get back an updated pool and the buffer object.
-- You may provide the size of the desired buffer either as an Int
-- or as a ByteString. In the latter case, the contents of the
-- ByteString will be copied into the buffer.
newBuffer :: Pool handle -> Either Int ByteString -> IO (Maybe (Pool handle, Buffer handle))
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
                handle <- (pRegister pool) cstr
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
                cleanpool <- cleanup newpool -- We remove at most unused buffer beyond the limit here.
                                             -- We don't want to constnatly alloc/dealloc a same-sized buffer, so we go gradual.
                return $ Just (cleanpool, newbuf)
      (newavailable,Just buf) ->
         let newpool = pool {pAvailableBySize = newavailable,
                             pInUse = Map.insert (bId buf) buf (pInUse pool),
                             pAvailableLru = List.delete (bId buf) (pAvailableLru pool)}
          in do case content of
                  Right bs ->
                    copyTo bs (bStart buf)
                  Left _ -> return ()
                cleanpool <- cleanup newpool -- We remove at most unused buffer beyond the limit here.
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

-- | View the contents of the buffer as a bytestring. Currently O(n).
getBufferByteString :: Buffer handle -> IO ByteString
getBufferByteString buffer =
   BSC.packCStringLen (bStart buffer, bSize buffer) -- TODO use unsafePackCStringLen, unsafePackMallocCString in Pool.getByteString? How to safely avoid copying?

findAndRemove :: (a -> Bool) -> [a] -> ([a], Maybe a)
findAndRemove f xs = go [] xs
   where go before [] = (reverse before,Nothing)
         go before (x:after) | f x = ((reverse before)++after,Just x)
         go before (x:after) = go (x:before) after

-- | A zero-copy alternative to getBufferByteString. The buffer is removed from
-- the pool; after this call, the Buffer object is invalid. The resulting ByteString
-- occupies the same space and will be handled normally by the gc.
convertBufferToByteString :: Pool handle -> Buffer handle -> IO (Pool handle, ByteString)
convertBufferToByteString pool buffer =
   let newpool = pool {pInUse = Map.delete (bId buffer) (pInUse pool)}
    in do (pUnregister pool) (bHandle buffer)
          bs <- unsafePackMallocCStringLen (bStart buffer, bSize buffer)
          -- TODO we should reinsert available buffer into the pool, so we're ready for the next msg
          return (newpool, bs)
