# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

import std/[tables, sequtils, strutils,
          unicode, intsets, marshal, options]

type
  AMF0Type* = enum
    ## AMF0 data types as defined in the RTMP specification.
    AMF0_Number = 0
    AMF0_Boolean = 1
    AMF0_String = 2
    AMF0_Object = 3
    AMF0_Null = 5
    AMF0_Undefined = 6
    AMF0_ECMAArray = 8
    AMF0_StrictArray = 10
    AMF0_LongString = 12

  AMF3Type* = enum
    ## AMF3 data types (marker values 0x00-0x1F).
    AMF3_Undefined = 0x00
    AMF3_Null = 0x01
    AMF3_False = 0x02
    AMF3_True = 0x03
    AMF3_Integer = 0x04
    AMF3_Double = 0x05
    AMF3_String = 0x06
    AMF3_Object = 0x08
    AMF3_Array = 0x09
    AMF3_ByteArray = 0x0C

  AMF0Value* = ref object
    ## Represents a value encoded in AMF0 format, used for RTMP action messages.
    case typ*: AMF0Type
      ## The type of the AMF0 value, determines which field is valid.
    of AMF0_Number:
      num*: float64
        ## For AMF0_Number type, holds the numeric value.
    of AMF0_Boolean:
      b*: bool
        ## For AMF0_Boolean type, holds the boolean value.
    of AMF0_String, AMF0_LongString:
      s*: string
        ## For AMF0_String and AMF0_LongString types, holds the string value.
    of AMF0_Object, AMF0_ECMAArray:
      obj*: Table[string, AMF0Value]
        ## For AMF0_Object and AMF0_ECMAArray types, holds a table of key-value pairs.
    of AMF0_StrictArray:
      arr*: seq[AMF0Value]
        ## For AMF0_StrictArray type, holds a sequence of AMF0 values.
    of AMF0_Null, AMF0_Undefined:
      discard

#
# constructors
#
proc newNumber*(v: float64): AMF0Value =
  result = AMF0Value(typ: AMF0_Number, num: v)

proc newBoolean*(v: bool): AMF0Value =
  result = AMF0Value(typ: AMF0_Boolean, b: v)

proc newString*(v: string): AMF0Value =
  result = AMF0Value(typ: AMF0_String, s: v)

proc newNull*(): AMF0Value =
  result = AMF0Value(typ: AMF0_Null)

proc newUndefined*(): AMF0Value =
  result = AMF0Value(typ: AMF0_Undefined)

proc newObject*(): AMF0Value =
  result = AMF0Value(typ: AMF0_Object, obj: initTable[string, AMF0Value]())

proc newEcmaArray*(): AMF0Value =
  result = AMF0Value(typ: AMF0_ECMAArray, obj: initTable[string, AMF0Value]())

proc newStrictArray*(items: seq[AMF0Value]): AMF0Value =
  result = AMF0Value(typ: AMF0_StrictArray, arr: items)

proc amfObj*(pairs: openArray[(string, AMF0Value)]): AMF0Value =
  result = newObject()
  for (k, v) in pairs:
    result.obj[k] = v

#
# internal helper procs for encoding/decoding big-endian primitives
#
proc readUint16BE(data: seq[byte], idx: var int): uint16 =
  var r: uint16 = 0
  r = (uint16(data[idx]) shl 8) or uint16(data[idx+1])
  idx += 2
  return r

proc readUint32BE(data: seq[byte], idx: var int): uint32 =
  var r: uint32 = 0
  r = (uint32(data[idx]) shl 24) or (uint32(data[idx+1]) shl 16) or (uint32(data[idx+2]) shl 8) or uint32(data[idx+3])
  idx += 4
  return r

proc readFloat64BE(data: seq[byte], idx: var int): float64 =
  var u: uint64 = 0
  for i in 0..7:
    u = (u shl 8) or uint64(data[idx + i])
  idx += 8
  var f: float64
  cast[ptr uint64](addr f)[] = u
  return f

# helper: convert slice of seq[byte] to string
proc seqSliceToStr(data: seq[byte], start, len: int): string =
  if len <= 0: return ""
  result = newString(len)
  for i in 0 ..< len:
    result[i] = char(data[start + i])

#
# AMF3 helpers
#
proc readU29(data: seq[byte], idx: var int): int =
  ## Read AMF3 variable-length U29 integer (1-4 bytes, 29 bits max).
  var result = int(data[idx] and 0x7F)
  idx.inc
  if (data[idx-1] and 0x80) != 0:
    result = (result shl 7) or int(data[idx] and 0x7F)
    idx.inc
    if (data[idx-1] and 0x80) != 0:
      result = (result shl 7) or int(data[idx] and 0x7F)
      idx.inc
      if (data[idx-1] and 0x80) != 0:
        result = (result shl 8) or int(data[idx])
        idx.inc
  result

proc readAmf3String(data: seq[byte], idx: var int): string =
  ## Read AMF3 string (length is U29-prefixed).
  let head = readU29(data, idx)
  if (head and 1) == 0:
    return ""
  let slen = head shr 1
  if slen > 0 and idx + slen <= data.len:
    result = seqSliceToStr(data, idx, slen)
    idx += slen
  else:
    result = ""

proc writeUint16BE(`out`: var seq[byte], v: uint16) =
  `out`.add(byte((v shr 8) and 0xFF))
  `out`.add(byte(v and 0xFF))

proc writeUint32BE(`out`: var seq[byte], v: uint32) =
  `out`.add(byte((v shr 24) and 0xFF))
  `out`.add(byte((v shr 16) and 0xFF))
  `out`.add(byte((v shr 8) and 0xFF))
  `out`.add(byte(v and 0xFF))

proc writeFloat64BE(`out`: var seq[byte], f: float64) =
  var u: uint64 = cast[ptr uint64](addr f)[]     # reinterpret bits
  for i in countdown(7, 0):
    `out`.add(byte((u shr (i*8)) and 0xFF))

#
# Decoder for AMF0-encoded data (used in RTMP action messages)
# 
proc decodeOneAMF0*(data: seq[byte], idx: var int): AMF0Value =
  if idx >= data.len:
    return nil
  let marker = int(data[idx])
  idx.inc
  case marker
  of 0: # Number (8 bytes)
    let v = readFloat64BE(data, idx)
    result = newNumber(v)
  of 1: # Boolean (1 byte)
    result = newBoolean(data[idx] != 0)
    idx.inc
  of 2: # String (2-byte length)
    let l = int(readUint16BE(data, idx))
    if l > 0:
      result = newString(seqSliceToStr(data, idx, l))
    else:
      result = newString("")
    idx += l
  of 3: # Object (key-value pairs terminated by 0x00 0x00 0x09)
    var t = initTable[string, AMF0Value]()
    while true:
      if idx + 3 > data.len:
        break
      let nameLen = int(readUint16BE(data, idx))
      if nameLen == 0 and data[idx].ord == 9:
        idx.inc # consume end marker 0x09
        break
      var name = ""
      if nameLen > 0:
        name = seqSliceToStr(data, idx, nameLen)
        idx += nameLen
      let v = decodeOneAMF0(data, idx)
      if v == nil: break
      t[name] = v
    result = newObject()
    result.obj = t
  of 5:
    result = newNull()
  of 6:
    result = newUndefined()
  of 8: # ECMA Array: 4-byte count followed by associative entries then object-end
    let count = readUint32BE(data, idx) # often ignored
    var t = initTable[string, AMF0Value]()
    while true:
      if idx + 3 > data.len:
        break
      let nameLen = int(readUint16BE(data, idx))
      if nameLen == 0 and data[idx].ord == 9:
        idx.inc
        break
      var name = ""
      if nameLen > 0:
        name = seqSliceToStr(data, idx, nameLen)
        idx += nameLen
      let v = decodeOneAMF0(data, idx)
      if v == nil: break
      t[name] = v
    result = newEcmaArray()
    result.obj = t
  of 10: # Strict Array: 4-byte count then that many items
    let cnt = int(readUint32BE(data, idx))
    var arr: seq[AMF0Value]
    for i in 0..<cnt:
      let v = decodeOneAMF0(data, idx)
      if v == nil:
        break
      arr.add(v)
    result = newStrictArray(arr)
  of 12: # Long string: 4-byte length
    let l = int(readUint32BE(data, idx))
    if l > 0:
      result = newString(seqSliceToStr(data, idx, l))
    else:
      result = newString("")
    idx += l
  else:
    # unsupported marker -> treat as undefined
    result = newUndefined()

proc decodeAllAMF0*(data: seq[byte]): seq[AMF0Value] =
  var idx = 0
  var val: seq[AMF0Value] = @[]
  while idx < data.len:
    let v = decodeOneAMF0(data, idx)
    if v == nil: break
    val.add(v)
  result = val

# Rename to avoid duplicate exported signature later (ptr-based wrapper keeps name decodeAllAMF0*)
proc decodeAllAMF0Seq*(data: seq[byte]): seq[AMF0Value] =
  decodeAllAMF0(data)

#
# AMF3 decoder
#
proc decodeOneAMF3*(data: seq[byte], idx: var int): AMF0Value =
  ## Decode a single AMF3 value. Returns AMF0Value for interop.
  if idx >= data.len: return nil
  let marker = int(data[idx])
  idx.inc
  case marker
  of 0x00: # AMF3 Undefined
    result = newUndefined()
  of 0x01: # AMF3 Null
    result = newNull()
  of 0x02: # AMF3 False
    result = newBoolean(false)
  of 0x03: # AMF3 True
    result = newBoolean(true)
  of 0x04: # AMF3 Integer (U29, 1-4 bytes)
    let v = readU29(data, idx)
    result = newNumber(float64(v))
  of 0x05: # AMF3 Double (8 bytes IEEE-754)
    let v = readFloat64BE(data, idx)
    result = newNumber(v)
  of 0x06: # AMF3 String (U29-length-prefixed)
    let s = readAmf3String(data, idx)
    result = newString(s)
  of 0x08: # AMF3 Object
    var t = initTable[string, AMF0Value]()
    # Read trait head (U29): inline + sealed + dynamic flags
    discard readU29(data, idx)
    # Read class name (always empty for anonymous objects)
    discard readAmf3String(data, idx)
    # Read dynamic members until empty-string terminator
    while idx < data.len:
      let name = readAmf3String(data, idx)
      if name.len == 0: break
      let v = decodeOneAMF3(data, idx)
      if v == nil: break
      t[name] = v
    result = newObject()
    result.obj = t
  of 0x09: # AMF3 Array (dense + associative)
    let denseCount = readU29(data, idx) shr 1
    var t = initTable[string, AMF0Value]()
    # Read associative members (same as object dynamic members)
    while idx < data.len:
      let name = readAmf3String(data, idx)
      if name.len == 0: break
      let v = decodeOneAMF3(data, idx)
      if v == nil: break
      t[name] = v
    # Read dense members
    for i in 0 ..< denseCount:
      let v = decodeOneAMF3(data, idx)
      if v == nil: break
      t[$i] = v
    result = newObject()
    result.obj = t
  of 0x0C: # AMF3 Byte Array (skip content)
    let head = readU29(data, idx)
    if (head and 1) != 0:
      let blen = head shr 1
      idx += blen
    result = newUndefined()
  else:
    # Unknown AMF3 type — skip and return undefined
    result = newUndefined()

proc decodeAllAMF3*(data: seq[byte]): seq[AMF0Value] =
  ## Decode all AMF3 values from a byte sequence.
  result = @[]
  var idx = 0
  while idx < data.len:
    let v = decodeOneAMF3(data, idx)
    if v == nil: break
    result.add(v)

#
# pointer-based helpers (zero-copy input parsing)
#
type
  ByteSlice = object
    data: ptr UncheckedArray[byte]
    len: int

  ByteReader = object
    data: ptr UncheckedArray[byte]
    len: int
    idx: int

proc initByteReader(data: ptr byte, len: int): ByteReader =
  ByteReader(
    data: cast[ptr UncheckedArray[byte]](data),
    len: len,
    idx: 0
  )

proc remaining(r: ByteReader): int =
  r.len - r.idx

#
# Primitive readers
#
proc readByte(r: var ByteReader): Option[byte] =
  if r.idx >= r.len:
    return none(byte)
  let b = r.data[r.idx]
  r.idx.inc
  some(b)

proc readUint16BE(r: var ByteReader): Option[uint16] =
  if r.idx > r.len - 2:
    return none(uint16)

  let v =
    (uint16(r.data[r.idx]) shl 8) or
     uint16(r.data[r.idx+1])

  r.idx += 2
  some(v)

proc readUint32BE(r: var ByteReader): Option[uint32] =
  if r.idx > r.len - 4:
    return none(uint32)

  let v =
    (uint32(r.data[r.idx])   shl 24) or
    (uint32(r.data[r.idx+1]) shl 16) or
    (uint32(r.data[r.idx+2]) shl 8)  or
     uint32(r.data[r.idx+3])

  r.idx += 4
  some(v)

proc readFloat64BE(r: var ByteReader): Option[float64] =
  if r.idx > r.len - 8:
    return none(float64)

  var u: uint64 = 0
  for i in 0 ..< 8:
    u = (u shl 8) or uint64(r.data[r.idx + i])

  r.idx += 8
  var f: float64
  cast[ptr uint64](addr f)[] = u # bit reinterpret (not numeric cast)
  some(f)

proc readSlice(r: var ByteReader, n: int): Option[ByteSlice] =
  if n < 0 or r.idx > r.len - n:
    return none(ByteSlice)

  let s = ByteSlice(
    data: cast[ptr UncheckedArray[byte]](r.data[r.idx].addr),
    len: n
  )

  r.idx += n
  some(s)

proc sliceToString(s: ByteSlice): string =
  result = newString(s.len)
  if s.len > 0:
    copyMem(result[0].addr, s.data[0].addr, s.len)


#
# AMF0 Decoder
#
proc decodeOneAMF0*(r: var ByteReader): AMF0Value =
  let markerOpt = r.readByte()
  if markerOpt.isNone:
    return nil

  let marker = int(markerOpt.get())

  case marker

  of 0: # Number
    let fOpt = r.readFloat64BE()
    if fOpt.isNone: return nil
    result = newNumber(fOpt.get())

  of 1: # Boolean
    let bOpt = r.readByte()
    if bOpt.isNone: return nil
    result = newBoolean(bOpt.get() != 0)

  of 2: # String (2-byte length)
    let lOpt = r.readUint16BE()
    if lOpt.isNone: return nil

    let sliceOpt = r.readSlice(int(lOpt.get()))
    if sliceOpt.isNone: return nil

    result = newString(sliceToString(sliceOpt.get()))

  of 3: # Object
    var obj = newObject()

    while true:
      if r.remaining() < 3:
        break

      let nameLenOpt = r.readUint16BE()
      if nameLenOpt.isNone: break

      let nameLen = int(nameLenOpt.get())

      # Object end marker
      if nameLen == 0 and r.remaining() > 0 and r.data[r.idx] == byte(9):
        r.idx.inc
        break

      var name = ""
      if nameLen > 0:
        let nameSliceOpt = r.readSlice(nameLen)
        if nameSliceOpt.isNone: break
        name = sliceToString(nameSliceOpt.get())

      let value = decodeOneAMF0(r)
      if value == nil: break

      obj.obj[name] = value

    result = obj

  of 5:
    result = newNull()

  of 6:
    result = newUndefined()

  of 8: # ECMA Array
    let cntOpt = r.readUint32BE()
    if cntOpt.isNone: return nil
    discard cntOpt # AMF spec: associative, count not strictly trusted
    var obj = newEcmaArray()
    while true:
      if r.remaining() < 3: break
      let nameLenOpt = r.readUint16BE()
      if nameLenOpt.isNone: break
      let nameLen = int(nameLenOpt.get())
      if nameLen == 0 and r.remaining() > 0 and r.data[r.idx] == byte(9):
        r.idx.inc
        break
      var name = ""
      if nameLen > 0:
        let nameSliceOpt = r.readSlice(nameLen)
        if nameSliceOpt.isNone: break
        name = sliceToString(nameSliceOpt.get())
      let value = decodeOneAMF0(r)
      if value == nil: break
      obj.obj[name] = value
    result = obj

  of 10: # Strict Array
    let cntOpt = r.readUint32BE()
    if cntOpt.isNone: return nil
    let cnt = int(cntOpt.get())
    var arr: seq[AMF0Value] = @[]
    for _ in 0 ..< cnt:
      let v = decodeOneAMF0(r)
      if v == nil: break
      arr.add(v)
    result = newStrictArray(arr)
  of 12: # Long string
    let lOpt = r.readUint32BE()
    if lOpt.isNone: return nil

    let sliceOpt = r.readSlice(int(lOpt.get()))
    if sliceOpt.isNone: return nil
    result = newString(sliceToString(sliceOpt.get()))
  else:
    result = newUndefined()

# pointer-based bulk decode
proc decodeAllAMF0Ptr*(data: ptr byte, dataLen: int): seq[AMF0Value] =
  ## Decodes a sequence of AMF0 values from a raw byte pointer and length, returning a sequence of AMF0Value. Uses ByteReader for zero-copy parsing.
  var r = initByteReader(data, dataLen)
  result = @[]
  while r.remaining() > 0:
    let v = decodeOneAMF0(r)
    if v == nil: break
    result.add(v)

#
# Encoder for AMF0 values (used for RTMP action message payloads)
#
proc encodeOneAMF0*(v: AMF0Value, outSeq: var seq[byte]) =
  ## Encodes a single AMF0Value into the provided byte sequence.
  ## Handles all AMF0 types according to the specification.
  if v == nil:
    outSeq.add(byte(6)) # undefined
    return
  case v.typ
  of AMF0_Number:
    outSeq.add(byte(0))
    writeFloat64BE(outSeq, v.num)
  of AMF0_Boolean:
    outSeq.add(byte(1))
    outSeq.add(if v.b: byte(1) else: byte(0))
  of AMF0_String:
    let bytes = v.s # Nim string is a byte-seq (typically UTF-8 already)
    if bytes.len <= 0xFFFF:
      outSeq.add(byte(2))
      writeUint16BE(outSeq, uint16(bytes.len))
      for ch in bytes: outSeq.add(byte(ord(ch)))
    else:
      outSeq.add(byte(12))
      writeUint32BE(outSeq, uint32(bytes.len))
      for ch in bytes: outSeq.add(byte(ord(ch)))
  of AMF0_Object:
    outSeq.add(byte(3))
    for k, av in v.obj.pairs:
      let kbytes = k
      writeUint16BE(outSeq, uint16(kbytes.len))
      for ch in kbytes: outSeq.add(byte(ord(ch)))
      encodeOneAMF0(av, outSeq)
    # object end marker
    outSeq.add(byte(0)); outSeq.add(byte(0)); outSeq.add(byte(9))
  of AMF0_ECMAArray:
    outSeq.add(byte(8))
    # count (can be 0 or actual size)
    writeUint32BE(outSeq, uint32(v.obj.len))
    for k, av in v.obj.pairs:
      let kbytes = k
      writeUint16BE(outSeq, uint16(kbytes.len))
      for ch in kbytes: outSeq.add(byte(ord(ch)))
      encodeOneAMF0(av, outSeq)
    outSeq.add(byte(0)); outSeq.add(byte(0)); outSeq.add(byte(9))
  of AMF0_StrictArray:
    outSeq.add(byte(10))
    writeUint32BE(outSeq, uint32(v.arr.len))
    for item in v.arr:
      encodeOneAMF0(item, outSeq)
  of AMF0_Null:
    outSeq.add(byte(5))
  of AMF0_Undefined:
    outSeq.add(byte(6))
  else:
    outSeq.add(byte(6))

proc encodeAMF0Values*(vals: seq[AMF0Value]): seq[byte] =
  ## Encodes a sequence of AMF0Value into a byte sequence suitable for RTMP action message payloads.
  var outSeq: seq[byte] = @[]
  for v in vals:
    encodeOneAMF0(v, outSeq)
  result = outSeq

# Small helpers for convenience
proc amf0ToStr*(v: AMF0Value): string =
  ## Converts an AMF0Value to a human-readable string for debugging purposes. Complex types are summarized as "<complex>".
  case v.typ
  of AMF0_Number: return $(v.num)
  of AMF0_Boolean: return if v.b: "true" else: "false"
  of AMF0_String: return v.s
  of AMF0_Null: return "null"
  of AMF0_Undefined: return "undefined"
  else: return "<complex>"
