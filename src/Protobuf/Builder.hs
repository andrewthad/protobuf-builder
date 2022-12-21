{-# language DataKinds #-}
{-# language RankNTypes #-}
{-# language MagicHash #-}
{-# language TypeApplications #-}
{-# language KindSignatures #-}
{-# language ScopedTypeVariables #-}
{-# language PatternSynonyms #-}
{-# language GADTs #-}

-- | Encode data with protobuf. Under the hood, this uses catenable byte
-- sequences, meaning that a large number of small @ByteArray@s are allocated
-- during encoding. That makes this a simple but low-performance solution.
-- To prevent incorrect use, @Builder@ is parameterized by @Value@. This
-- distinguishes encodings of primitives from encodings of sequences of
-- key-value pairs. The functions 'message' and 'pair' are used to move
-- between these two domains.
module Protobuf.Builder
  ( Builder(..)
  , WireType(..)
  , Value(..)
  , run
    -- * Variable Length
  , variableWord8
  , variableWord16
  , variableWord32
  , variableWord64
  , sint64
  , int64
  , sint32
  , int32
    -- * Fixed Length 32
  , fixed32
    -- * Fixed Length 64
  , fixed64
  , double
    -- * Messages
  , message
  , pair
    -- * Length-Delimited 
  , shortText
  , shortByteString
  ) where

import Control.Monad.ST.Run (runByteArrayST)
import Data.Word (Word8,Word16,Word32,Word64)
import Data.Int (Int32,Int64)
import GHC.Exts (Proxy#,proxy#)
import Data.Bits ((.|.),unsafeShiftL)
import Data.Builder.Catenable.Bytes (pattern (:<))
import Data.Bytes (Bytes)
import Data.Word.Zigzag (toZigzag32,toZigzag64)
import Data.Text.Short (ShortText)
import Data.ByteString.Short (ShortByteString)

import qualified Data.Bytes as Bytes
import qualified Data.Bytes.Chunks as Chunks
import qualified Data.Bytes.Builder.Bounded as Bounded
import qualified Data.Builder.Catenable.Bytes as Builder
import qualified Data.Primitive as PM
import qualified Data.Kind as GHC
import qualified Arithmetic.Nat as Nat
import qualified Data.ByteString.Short as SBS
import qualified Data.Text.Short as TS

-- | A protobuf object builder. The data constructor is exposed, but it is
-- unsafe to use it.
--
-- Note that @Builder 'Pairs@ has @Semigroup@ and @Monoid@ instances.
-- This is an important part of the interface.
data Builder :: Value -> GHC.Type where
  Builder ::
       !Int -- length of builder
    -> !Builder.Builder -- builder
    -> Builder v

run :: Builder v -> Bytes
run (Builder _ b) = Chunks.concat (Builder.run b)

-- | Protobuf\'s four wire types.
data WireType
  = BitsFixed32
  | BitsFixed64
  | BitsVariable
  | Bytes

-- | Either a primitive type (a wire type) or a collector of encoded
-- key-value pairs.
data Value
  = Primitive WireType
  | Pairs

sint32 ::Int32 -> Builder ('Primitive 'BitsVariable)
sint32 w = variableWord32 (toZigzag32 w)

int32 ::Int32 -> Builder ('Primitive 'BitsVariable)
int32 w = variableWord32 (fromIntegral w)

sint64 ::Int64 -> Builder ('Primitive 'BitsVariable)
sint64 w = variableWord64 (toZigzag64 w)

int64 ::Int64 -> Builder ('Primitive 'BitsVariable)
int64 w = variableWord64 (fromIntegral w)

variableWord8 :: Word8 -> Builder ('Primitive 'BitsVariable)
variableWord8 w = variableWord64 (fromIntegral w)

variableWord16 :: Word16 -> Builder ('Primitive 'BitsVariable)
variableWord16 w = variableWord64 (fromIntegral w)

variableWord32 :: Word32 -> Builder ('Primitive 'BitsVariable)
variableWord32 w = variableWord64 (fromIntegral w)

variableWord64 :: Word64 -> Builder ('Primitive 'BitsVariable)
variableWord64 w =
  let b = Bounded.run Nat.constant (Bounded.word64LEB128 w)
   in Builder (PM.sizeofByteArray b) (Bytes.fromByteArray b :< Builder.Empty)

fixed32 :: Word32 -> Builder ('Primitive 'BitsFixed32)
fixed32 w =
  let b = Bounded.run Nat.constant (Bounded.word32LE w)
   in Builder (PM.sizeofByteArray b) (Bytes.fromByteArray b :< Builder.Empty)

fixed64 :: Word64 -> Builder ('Primitive 'BitsFixed64)
fixed64 w =
  let b = Bounded.run Nat.constant (Bounded.word64LE w)
   in Builder (PM.sizeofByteArray b) (Bytes.fromByteArray b :< Builder.Empty)

double :: Double -> Builder ('Primitive 'BitsFixed64)
double w =
  let b = runByteArrayST $ do
            dst <- PM.newByteArray 8
            PM.writeByteArray dst 0 w
            PM.unsafeFreezeByteArray dst
   in Builder 8 (Bytes.fromByteArray b :< Builder.Empty)

pair :: forall (ty :: WireType). HasWireTypeNumber ty
  => Word32 -- ^ key
  -> Builder ('Primitive ty) -- ^ value
  -> Builder 'Pairs
{-# inline pair #-}
pair k (Builder valLen valBuilder) =
  let fullKey = unsafeShiftL (fromIntegral @Word32 @Word64 k) 3 .|. fromIntegral @Word8 @Word64 (wireTypeNumber (proxy# @ty))
      keyBytes = Bounded.run Nat.constant (Bounded.word64LEB128 fullKey) 
   in Builder (valLen + PM.sizeofByteArray keyBytes) (Builder.Cons (Bytes.fromByteArray keyBytes) valBuilder)

message :: Builder 'Pairs -> Builder ('Primitive 'Bytes)
message (Builder len b) =
  let lenBytes = Bounded.run Nat.constant (Bounded.word64LEB128 (fromIntegral len))
   in Builder (len + PM.sizeofByteArray lenBytes) (Builder.Cons (Bytes.fromByteArray lenBytes) b)

shortText :: ShortText -> Builder ('Primitive 'Bytes)
shortText t = shortByteString (TS.toShortByteString t)

shortByteString :: ShortByteString -> Builder ('Primitive 'Bytes)
shortByteString sbs =
  let len = SBS.length sbs
      lenBytes = Bounded.run Nat.constant (Bounded.word64LEB128 (fromIntegral len))
   in Builder (len + PM.sizeofByteArray lenBytes) (Bytes.fromByteArray lenBytes :< Bytes.fromShortByteString sbs :< Builder.Empty)

instance (v ~ 'Pairs) => Monoid (Builder v) where
  mempty = Builder 0 Builder.Empty

instance (v ~ 'Pairs) => Semigroup (Builder v) where
  Builder xlen x <> Builder ylen y = Builder (xlen + ylen) (x <> y)

class HasWireTypeNumber (t :: WireType) where
  wireTypeNumber :: Proxy# t -> Word8

instance HasWireTypeNumber 'BitsFixed32 where
  wireTypeNumber _ = 5

instance HasWireTypeNumber 'BitsFixed64 where
  wireTypeNumber _ = 1

instance HasWireTypeNumber 'BitsVariable where
  wireTypeNumber _ = 0

instance HasWireTypeNumber 'Bytes where
  wireTypeNumber _ = 2

