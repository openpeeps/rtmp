# Shared builders for RTMP unit tests.
#
# Byte-exact fixture constructors (chunk frames, ADTS frames, FLV files)
# so tests never depend on repo media binaries.

import std/[os, times]

var tmpCounter = 0

proc writeTempBytes*(prefix, ext: string, data: seq[byte]): string =
  ## Write raw bytes to a unique temp file, return its path.
  inc tmpCounter
  result = getTempDir() / (prefix & "-" & $getTime().toUnix() &
    "-" & $tmpCounter & ext)
  var s = newString(data.len)
  for i, b in data: s[i] = char(b)
  writeFile(result, s)

proc be16*(v: int): seq[byte] =
  @[byte((v shr 8) and 0xFF), byte(v and 0xFF)]

proc be24*(v: int): seq[byte] =
  @[byte((v shr 16) and 0xFF), byte((v shr 8) and 0xFF), byte(v and 0xFF)]

proc be32*(v: uint32): seq[byte] =
  @[byte((v shr 24) and 0xFF), byte((v shr 16) and 0xFF),
    byte((v shr 8) and 0xFF), byte(v and 0xFF)]

proc le32*(v: uint32): seq[byte] =
  @[byte(v and 0xFF), byte((v shr 8) and 0xFF),
    byte((v shr 16) and 0xFF), byte((v shr 24) and 0xFF)]

proc chunkBasicHeader*(fmt, csid: int): seq[byte] =
  ## 1-, 2- or 3-byte basic header for fmt (0..3) and csid.
  if csid >= 2 and csid <= 63:
    result = @[byte((fmt shl 6) or csid)]
  elif csid >= 64 and csid <= 319:
    result = @[byte((fmt shl 6) or 0), byte(csid - 64)]
  else:
    let v = csid - 64
    result = @[byte((fmt shl 6) or 1), byte(v and 0xFF), byte((v shr 8) and 0xFF)]

proc chunkMessage*(fmt, csid: int, ts: uint32, msgLen: int, typeId: uint8,
                   streamId: uint32, payload: seq[byte],
                   chunkSize: int = 128): seq[byte] =
  ## One message header (fmt0/1/2/3) + payload split at chunkSize,
  ## continuations emitted as bare fmt3 basic headers.
  result = chunkBasicHeader(fmt, csid)
  let tsField = min(ts, 0xFFFFFF'u32)
  case fmt
  of 0:
    result.add be24(int(tsField))
    result.add be24(msgLen)
    result.add typeId
    result.add le32(streamId)
  of 1:
    result.add be24(int(tsField))
    result.add be24(msgLen)
    result.add typeId
  of 2:
    result.add be24(int(tsField))
  else: discard
  if ts > 0xFFFFFF'u32:
    result.add be32(ts)
  var off = 0
  var first = true
  while off < payload.len:
    if not first:
      result.add chunkBasicHeader(3, csid)
      if ts > 0xFFFFFF'u32:
        result.add be32(ts)
    let n = min(chunkSize, payload.len - off)
    result.add payload[off ..< off + n]
    off += n
    first = false

proc adtsFrame*(profile, sfIdx, channels: int, payload: seq[byte]): seq[byte] =
  ## 7-byte ADTS header (MPEG-4, no CRC) + payload. profile is 1-based (2 = AAC-LC).
  let frameLen = 7 + payload.len
  result = @[
    0xFF'u8, 0xF1'u8,
    byte((((profile - 1) and 0x03) shl 6) or ((sfIdx and 0x0F) shl 2) or
      ((channels shr 2) and 0x01)),
    byte((((channels and 0x03) shl 6) or ((frameLen shr 11) and 0x03))),
    byte((frameLen shr 3) and 0xFF),
    byte((((frameLen and 0x07) shl 5) or 0x1F)),
    0xFC'u8
  ]
  result.add payload

proc flvFile*(flags: uint8, tags: seq[seq[byte]]): seq[byte] =
  ## FLV header + already-framed tags (each tag includes its prevTagSize).
  result = @[byte('F'), byte('L'), byte('V'), 0x01'u8, flags]
  result.add be32(9'u32)
  result.add be32(0'u32) # first prevTagSize
  for t in tags: result.add t

proc flvTag*(tagType: uint8, ts: uint32, payload: seq[byte]): seq[byte] =
  ## One framed FLV tag: 11-byte header + payload + prevTagSize.
  result = @[tagType]
  result.add be24(payload.len)
  result.add be24(int(ts and 0xFFFFFF'u32))
  result.add byte((ts shr 24) and 0xFF)
  result.add @[0'u8, 0'u8, 0'u8] # streamID always 0
  result.add payload
  result.add be32(uint32(11 + payload.len))
