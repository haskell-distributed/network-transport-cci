-- |
-- Copyright : (C) 2012-2013 Parallel Scientific Labs, LLC.
-- License   : BSD3
--
-- Support for managing pools of CCI transfer buffers.

{-# LANGUAGE ForeignFunctionInterface #-}
module Network.Transport.CCI.Pool
  ( Pool
  , PoolRef
  , Buffer(bHandle)
  , newPool
  , freePool
  , newBuffer
  , freeBuffer
  , getBufferHandle
  , getBufferByteString
  , convertBufferToByteString
  , spares
  , cleanup
  , lookupBuffer
  , unregisterBuffer
  ) where

import Control.Applicative ((<$>))
import Control.Monad (when, join)
import Control.Exception (catch, IOException, mask_, bracketOnError)

import Data.Map (Map)
import qualified Data.Map as Map

import qualified Data.ByteString.Char8 as BSC
import Data.ByteString.Char8 (ByteString)
import Data.ByteString.Unsafe (unsafePackCStringFinalizer)

import Data.IORef
import qualified Data.List as List (find, delete, deleteBy, insertBy)
import Data.Maybe (fromJust)
import Data.Foldable ( forM_, mapM_ )
import Data.List (find)
import Data.Ord (comparing)
import Foreign.C.Types ( CChar, CSize(..), CInt(..) )
import Foreign.C.String (CStringLen)
import Foreign.Marshal.Alloc (mallocBytes, free, alloca)
import Foreign.Ptr (Ptr, castPtr)
import Foreign.Storable ( peek )
import Prelude hiding (mapM_)


foreign import ccall unsafe posix_memalign :: Ptr (Ptr a) -> CSize -> CSize -> IO CInt

type BufferId = Int

-- TODO call spares (somewhere??) to allocate buffers in advance of their need.

-- | A buffer, identified by a handle. With this handle, we can deallocate with
-- 'freeBuffer', we can get its contents with 'getBufferByteString' and we can
-- get its handle value with 'getBufferHandle'.
data Buffer handle = Buffer
     {
        bId :: BufferId,
        bStart :: Ptr CChar,
        bSize :: Int,
        bHandle :: handle
     }

-- | A collection of managed buffers, parameterized by the type of the handle
-- that is created when a buffer is registered. In CCI's case, that is
-- RMALocalHandle.
data Pool handle = Pool
     { pNextId :: !BufferId
       -- ^ The generator for buffer identifiers
     , pMaxBufferCount :: Int
       -- ^ The maximum amount of idle buffers that are kept
     , pAlign :: Int
       -- ^ Alignment required for buffers
     , pRegister :: CStringLen -> IO handle
       -- ^ Operation used to register buffers
     , pUnregister :: handle -> IO ()
       -- ^ Operation used to unregister buffers

     , pInUse :: Map BufferId (Buffer handle)
       -- ^ Buffers in use
     , pAvailableBySize :: [Buffer handle]
       -- ^ Idle buffers sorted by size
     , pAvailableLru :: [BufferId]
       -- ^ Idle buffers in the order they were created or returned
       --   (the most recent buffer appears first)
     }

type PoolRef handle = IORef (Maybe (Pool handle))

-- | Returns the handle of the given buffer
getBufferHandle :: Buffer handle -> handle
getBufferHandle = bHandle

_dbg :: String -> IO ()
_dbg = putStrLn

-- | Deallocates and unregisters all buffers managed
-- by the given pool.
freePool :: PoolRef handle -> IO ()
freePool rPool = mask_ $ do
    mPool <- atomicModifyIORef rPool $ \mPool -> (Nothing, mPool)
    forM_ mPool $ \pool -> do
      mapM_ (pUnregister pool) $ map bHandle $ Map.elems $ pInUse pool
      mapM_ (destroyBuffer pool) $ pAvailableBySize pool

-- | Create a new pool. All buffers will be aligned at the given alignment (or
-- 0 for any alignment). Allocated, but unused buffers will be harvested after
-- the given max count. All, the user provides two functions for registering and
-- registering handles, which are called when buffers are allocated and
-- deallocated.
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

-- | Lookups a buffer in a given pool.
lookupBuffer :: Ptr CChar -> PoolRef handle -> IO (Maybe (Buffer handle))
lookupBuffer ptr =
    fmap (join . fmap (find ((ptr ==) . bStart) . Map.elems . pInUse))
      . readIORef

-- | Unregisters a buffer.
unregisterBuffer :: PoolRef handle -> Buffer handle -> IO ()
unregisterBuffer rPool buf =
    atomicModifyIORef rPool (\mPool ->
      case mPool of
        Nothing   -> (mPool,Nothing)
        Just pool -> ( Just pool { pInUse = Map.delete (bId buf) (pInUse pool) }
                     , Just pool
                     )
      )
    >>= mapM_ (\pool -> pUnregister pool (bHandle buf))

-- | Release the given buffer. It won't be unregistered and deallocated
-- immediately, but simply placed on the available list.
freeBuffer :: PoolRef handle -> Buffer handle -> IO ()
freeBuffer rPool buffer = mask_ $ do
    mPoolOk <- atomicModifyIORef rPool $ \mPool -> case mPool of
      Nothing -> (mPool, Right False)
      Just pool -> do
        case Map.lookup (bId buffer) (pInUse pool) of
          Just buf | bSize buf == bSize buffer ->
            if pMaxBufferCount pool <= length (pAvailableLru pool)
              then let newpool = pool
                         { pInUse = Map.delete (bId buffer) (pInUse pool) }
                    in (Just newpool, Left pool)
              else let newpool = pool
                         { pInUse = Map.delete (bId buffer) (pInUse pool)
                         , pAvailableBySize =
                             List.insertBy (comparing bSize)
                                           buffer
                                           (pAvailableBySize pool)
                         , pAvailableLru = bId buf : pAvailableLru pool
                         }
                    in (Just newpool, Right True)
          _ -> (mPool, Right True)

    case mPoolOk of
      -- The pool is full of idle buffers, so unregister and release.
      Left pool   -> destroyBuffer pool buffer
      -- The pool has been released, so we release the buffer.
      Right False -> freeAligned (bStart buffer)
      -- The pool accepted the buffer.
      Right True  -> return ()

-- | Allocate excess buffers up to our limit
spares :: PoolRef handle -> Int -> IO ()
spares rPool defaultsize = mask_ $ do
    mPool <- readIORef rPool
    forM_ mPool $ \pool ->
      if (pMaxBufferCount pool > length (pAvailableLru pool))
        then do res <- newBuffer rPool defaultsize
                case res of
                  Just newbuf ->
                    freeBuffer rPool newbuf
                  Nothing -> return ()
        else return ()

-- | Remove and destroy excess buffers beyond our limit
cleanup :: PoolRef handle -> IO ()
cleanup rPool = mask_ $ do
    mPool <- readIORef rPool
    forM_ mPool $ \pool ->
      if (pMaxBufferCount pool < length (pAvailableLru pool)) then
        let killme = let killmeId = last (pAvailableLru pool)
                      in fromJust $ List.find (\b -> bId b == killmeId) (pAvailableBySize pool)
            newpool = pool { pAvailableLru = init $ pAvailableLru pool,
                             pAvailableBySize = List.deleteBy byId killme (pAvailableBySize pool)}
         in do destroyBuffer pool killme
               return newpool
      else return pool
  where
    byId a b = bId a == bId b

destroyBuffer :: Pool handle -> Buffer handle -> IO ()
destroyBuffer pool buffer =
 do (pUnregister pool) (bHandle buffer)
    freeAligned $ bStart buffer

-- | Find an available buffer of the appropriate size, or allocate a new one if
-- such a buffer is not already allocated. You will get back an updated pool and
-- the buffer object. You may provide the size of the desired buffer.
newBuffer :: PoolRef handle -> Int -> IO (Maybe (Buffer handle))
newBuffer rPool rmaSize = mask_ $ do
    mres <- atomicModifyIORef rPool $ \mPool -> case mPool of
      Nothing -> (mPool, Left Nothing)
      Just pool -> case findAndRemove goodSize (pAvailableBySize pool) of
        (_newavailable, Nothing) ->
          ( Just pool { pNextId = pNextId pool + 1 }
          , Left mPool
          )
        (newavailable, Just buf) ->
          -- We renew the identifier when recycling the buffer so there is no
          -- danger that the GC returns the buffer of a previous incarnation,
          -- which could happen if the buffer was returned explicitly.
          let newbuf = buf { bId = pNextId pool }
           in ( Just pool
                  { pNextId = pNextId pool + 1
                  , pAvailableBySize = newavailable
                  , pInUse = Map.insert (bId newbuf) newbuf (pInUse pool)
                  , pAvailableLru = List.delete (bId buf) (pAvailableLru pool)
                  }
              , Right newbuf
              )
    case mres of
      -- reusing a recycled buffer
      Right buf  -> return $ Just buf
      -- the pool has been released
      Left Nothing -> return Nothing
      -- there are no idle buffers, so we request one
      Left (Just pool0)  -> bracketOnError
        (allocAligned' (pAlign pool0) rmaSize)
        (maybe (return ()) $ freeAligned . fst)
        $ \mbuf -> case mbuf of
          Nothing -> return Nothing
          Just cstr@(start,_) -> do
            handle <- (pRegister pool0) cstr
            let newbuf = Buffer
                  { bId = pNextId pool0
                  , bStart = start
                  , bSize = rmaSize
                  , bHandle = handle
                  }
            poolOk <- atomicModifyIORef rPool $ maybe (Nothing,False) $ \pool ->
              ( Just pool { pInUse = Map.insert (bId newbuf) newbuf (pInUse pool) }
              , True
              )
            if poolOk then return $ Just newbuf
             else error "network-transport-cci: PANIC! Pool was released while still in use."

  where
    goodSize b = bSize b >= rmaSize

freeAligned :: Ptr CChar -> IO ()
freeAligned = free

allocAligned' :: Int -> Int -> IO (Maybe CStringLen)
allocAligned' a s =
   catch (Just <$> allocAligned a s) (\x -> const (return Nothing) (x::IOException))

allocAligned :: Int -> Int -> IO CStringLen
allocAligned 0 size = mallocBytes size >>= \p -> return (p,size)
allocAligned align size = alloca $ \ptr ->
     do ret <- posix_memalign ptr (fromIntegral align) (fromIntegral size)
        when (ret /= 0) $ error $ "allocAligned: " ++ show ret
        res <- peek ptr
        return (res, size)

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
-- the pool; after this call, the Buffer object is invalid. The resulting
-- ByteString occupies the same space and will be handled normally by the gc.
convertBufferToByteString :: PoolRef handle -> Buffer handle -> IO ByteString
convertBufferToByteString rPool buffer =
    unsafePackCStringFinalizer (castPtr $ bStart buffer) (bSize buffer)
      $ freeBuffer rPool buffer
