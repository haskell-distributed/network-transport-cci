module Util where

import Foreign.C.String (CStringLen, Ptr)
import Foreign.C.Types (CChar)

data Buffer = Buffer { bufferContent :: CStringLen, bufferAlloc :: Ptr CChar }
