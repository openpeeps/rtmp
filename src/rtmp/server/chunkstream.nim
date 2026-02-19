# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

import std/[tables, sequtils, options]

type
  ChunkMessageCb* = proc(msgTypeId: int, msgStreamId: int, timestamp: uint32,
                              payloadPtr: ptr byte, payloadLen: int, arg: pointer)

  ChunkHeader* = object
    ## Represents the header of an RTMP chunk, containing metadata about the message being received.
    timestamp*: uint32
    msgLength*: int
    msgTypeId*: int
    msgStreamId*: int

  ChunkStreamState* = ref object
    ## Maintains the state of an individual chunk stream, including
    ## the last header information, timestamp delta, and payload assembly.
    lastHeader*: ChunkHeader
    lastTimestampDelta*: uint32
    chunkRemaining*: int
    payload*: seq[byte]
    bytesRead*: int
    remaining*: int

  ChunkStreamCtx* = ref object
    ## Context for managing multiple chunk streams, tracking their states,
    ## peer chunk size, and callback for completed messages.
    streams*: Table[int, ChunkStreamState]
    peerChunkSize*: int
    onMessageCb*: ChunkMessageCb
    cbArg*: pointer
    partialCsid*: int

  BytePtr = ptr UncheckedArray[byte]

proc asBytePtr(data: ptr byte): BytePtr {.inline.} =
  cast[BytePtr](data)

#
# public API
#
proc initChunkStreamCtx*(peerChunkSize: int = 128): ChunkStreamCtx =
  ## Initializes a new ChunkStreamCtx with an optional peer chunk size
  ## (default 128). The context will manage multiple chunk streams and invoke callbacks when complete messages are assembled.
  result = ChunkStreamCtx(
    streams: initTable[int, ChunkStreamState](),
    peerChunkSize: if peerChunkSize > 0: peerChunkSize else: 128,
    onMessageCb: nil,
    cbArg: nil,
    partialCsid: -1
  )

proc setPeerChunkSize*(ctx: ChunkStreamCtx, size: int) =
  if ctx == nil or size <= 0: return
  ctx.peerChunkSize = size

proc setOnMessage*(ctx: ChunkStreamCtx, cb: ChunkMessageCb, arg: pointer) =
  if ctx == nil: return
  ctx.onMessageCb = cb
  ctx.cbArg = arg

# internal helpers
proc ensureStreamState(ctx: ChunkStreamCtx, csid: int): ChunkStreamState =
  var st = ctx.streams.getOrDefault(csid, nil)
  if st == nil:
    st = ChunkStreamState(
      lastHeader: ChunkHeader(timestamp: 0'u32, msgLength: 0, msgTypeId: 0, msgStreamId: 0),
      lastTimestampDelta: 0'u32,
      chunkRemaining: 0,
      payload: @[],
      bytesRead: 0,
      remaining: 0
    )
    ctx.streams[csid] = st
  return st

# read helpers using pointer+bounds
proc canRead(data: ptr byte, len, idx, need: int): bool =
  return idx + need <= len

proc read3BE(data: ptr byte, idx: var int, len: int): Option[uint32] =
  if not canRead(data, len, idx, 3): return none(uint32)
  let p = asBytePtr(data)
  let v: uint32 = (uint32(p[idx]) shl 16) or (uint32(p[idx+1]) shl 8) or uint32(p[idx+2])
  idx += 3
  some(v)

proc read3IntBE(data: ptr byte, idx: var int, len: int): Option[int] =
  if not canRead(data, len, idx, 3): return none(int)
  let p = asBytePtr(data)
  let v = (int(p[idx]) shl 16) or (int(p[idx+1]) shl 8) or int(p[idx+2])
  idx += 3
  some(v)

proc read4BE(data: ptr byte, idx: var int, len: int): Option[uint32] =
  if not canRead(data, len, idx, 4): return none(uint32)
  let p = asBytePtr(data)
  let v: uint32 = (uint32(p[idx]) shl 24) or (uint32(p[idx+1]) shl 16) or (uint32(p[idx+2]) shl 8) or uint32(p[idx+3])
  idx += 4
  some(v)

proc read4LEint(data: ptr byte, idx: var int, len: int): Option[int] =
  if not canRead(data, len, idx, 4): return none(int)
  let p = asBytePtr(data)
  let v = int(p[idx]) or (int(p[idx+1]) shl 8) or (int(p[idx+2]) shl 16) or (int(p[idx+3]) shl 24)
  idx += 4
  some(v)

# feedBytes: consume as many bytes as possible; return consumed count
proc feedBytes*(ctx: ChunkStreamCtx, data: ptr byte, len: int): int =
  var i = 0
  if ctx == nil or data == nil or len <= 0:
    return 0

  let p = asBytePtr(data)

  while i < len:
    # if we're mid-chunk (payload continuation), consume
    # payload without parsing a new header
    if ctx.partialCsid >= 0:
      let stCont = ctx.streams.getOrDefault(ctx.partialCsid, nil)
      if stCont != nil and stCont.chunkRemaining > 0:
        let availCont = len - i
        if availCont <= 0: break
        let toTakeCont = min(availCont, stCont.chunkRemaining)
        let oldLen = stCont.payload.len
        stCont.payload.setLen(oldLen + toTakeCont)
        for k in 0 ..< toTakeCont:
          stCont.payload[oldLen + k] = p[i + k]
        i += toTakeCont
        stCont.bytesRead += toTakeCont
        stCont.remaining -= toTakeCont
        stCont.chunkRemaining -= toTakeCont
        if stCont.remaining == 0:
          if ctx.onMessageCb != nil:
            ctx.onMessageCb(
              stCont.lastHeader.msgTypeId,
              stCont.lastHeader.msgStreamId,
              stCont.lastHeader.timestamp,
              (if stCont.payload.len > 0: cast[ptr byte](addr stCont.payload[0]) else: nil),
              stCont.payload.len,
              ctx.cbArg
            )
          stCont.payload.setLen(0)
          stCont.bytesRead = 0
          stCont.remaining = 0
          stCont.chunkRemaining = 0
          ctx.partialCsid = -1
        elif stCont.chunkRemaining == 0:
          ctx.partialCsid = -1
        # if stCont.remaining > 0 and stCont.bytesRead > 0:
        #   echo "chunk partial: csid=", ctx.partialCsid, " type=", stCont.lastHeader.msgTypeId,
        #        " msgLen=", stCont.lastHeader.msgLength, " bytesRead=", stCont.bytesRead,
        #        " remaining=", stCont.remaining, " chunkRemaining=", stCont.chunkRemaining
        continue
      else:
        ctx.partialCsid = -1
    if not canRead(data, len, i, 1):
      break
    let b0 = int(p[i])
    let fmt = (b0 shr 6) and 0x03
    var csid = b0 and 0x3F
    var headerBytes = 1

    # extended CSID handling
    if csid == 0:
      if not canRead(data, len, i, headerBytes + 1): break
      csid = 64 + int(p[i + headerBytes])
      headerBytes += 1
    elif csid == 1:
      if not canRead(data, len, i, headerBytes + 2): break
      csid = 64 + int(p[i + headerBytes]) + (int(p[i + headerBytes + 1]) shl 8)
      headerBytes += 2

    var needed = headerBytes
    case fmt
    of 0: needed += 11
    of 1: needed += 7
    of 2: needed += 3
    of 3: needed += 0
    else: discard

    if not canRead(data, len, i, needed): break

    var idx = i + headerBytes
    var timestampOpt: Option[uint32] = none(uint32)
    var msgLenOpt: Option[int] = none(int)
    var typeId = 0
    var msgStreamIdOpt: Option[int] = none(int)

    if fmt == 0:
      let t = read3BE(data, idx, len)
      if t.isNone: break
      timestampOpt = t
      let ml = read3IntBE(data, idx, len)
      if ml.isNone: break
      msgLenOpt = ml
      typeId = int(p[idx]); idx += 1
      let msid = read4LEint(data, idx, len)
      if msid.isNone: break
      msgStreamIdOpt = msid
    elif fmt == 1:
      let t = read3BE(data, idx, len)
      if t.isNone: break
      timestampOpt = t
      let ml = read3IntBE(data, idx, len)
      if ml.isNone: break
      msgLenOpt = ml
      typeId = int(p[idx]); idx += 1
    elif fmt == 2:
      let t = read3BE(data, idx, len)
      if t.isNone: break
      timestampOpt = t
    else:
      discard

    # extended timestamp if any timestamp == 0xFFFFFF
    var extTsNeeded = false
    if timestampOpt.isSome and timestampOpt.get() == 0xFFFFFF'u32:
      extTsNeeded = true
    if fmt == 3:
      let stPeek = ctx.streams.getOrDefault(csid, nil)
      if stPeek != nil and stPeek.lastHeader.timestamp == 0xFFFFFF'u32:
        extTsNeeded = true

    if extTsNeeded:
      if not canRead(data, len, idx, 4): break
      let ext = read4BE(data, idx, len)
      if ext.isNone: break
      timestampOpt = some(ext.get())

    # header parsed; set i to payload start
    i = idx

    # update stream state
    let st = ensureStreamState(ctx, csid)
    if fmt == 0:
      st.lastHeader.timestamp = timestampOpt.get()
      st.lastTimestampDelta = 0'u32
      st.lastHeader.msgLength = msgLenOpt.get()
      st.lastHeader.msgTypeId = typeId
      st.lastHeader.msgStreamId = msgStreamIdOpt.get()
      st.payload.setLen(0)
      st.bytesRead = 0
      st.remaining = st.lastHeader.msgLength
    elif fmt == 1:
      let delta = timestampOpt.get()
      st.lastTimestampDelta = delta
      st.lastHeader.timestamp = st.lastHeader.timestamp + delta
      if msgLenOpt.isSome: st.lastHeader.msgLength = msgLenOpt.get()
      st.lastHeader.msgTypeId = typeId
      st.payload.setLen(0)
      st.bytesRead = 0
      st.remaining = st.lastHeader.msgLength
    elif fmt == 2:
      let delta = timestampOpt.get()
      st.lastTimestampDelta = delta
      st.lastHeader.timestamp = st.lastHeader.timestamp + delta
      if st.remaining <= 0:
        st.payload.setLen(0)
        st.bytesRead = 0
        st.remaining = st.lastHeader.msgLength
    else:
      if st.remaining <= 0:
        if st.lastTimestampDelta > 0'u32:
          st.lastHeader.timestamp = st.lastHeader.timestamp + st.lastTimestampDelta
        st.payload.setLen(0)
        st.bytesRead = 0
        st.remaining = st.lastHeader.msgLength

    # echo "chunk header: csid=", csid, " fmt=", fmt,
    #      " type=", st.lastHeader.msgTypeId, " msgLen=", st.lastHeader.msgLength,
    #      " msgStreamId=", st.lastHeader.msgStreamId, " ts=", st.lastHeader.timestamp,
    #      " remaining=", st.remaining

    # read chunk payload up to peerChunkSize and remaining
    let avail = len - i
    st.chunkRemaining = min(ctx.peerChunkSize, st.remaining)
    if avail <= 0:
      if st.remaining > 0 and st.chunkRemaining > 0:
        ctx.partialCsid = csid
      break
    let toTake = min(avail, st.chunkRemaining)
    if toTake <= 0: break

    if st.bytesRead == 0 and toTake == st.lastHeader.msgLength and st.chunkRemaining == toTake:
      if ctx.onMessageCb != nil:
        ctx.onMessageCb(
          st.lastHeader.msgTypeId,
          st.lastHeader.msgStreamId,
          st.lastHeader.timestamp,
          cast[ptr byte](addr p[i]), # pointer into input buffer (zero-copy)
          toTake,
          ctx.cbArg
        )
      i += toTake
      st.bytesRead += toTake
      st.remaining -= toTake
      st.chunkRemaining -= toTake
      st.payload.setLen(0)
      st.bytesRead = 0
      st.remaining = 0
      st.chunkRemaining = 0
    else:
      let oldLen = st.payload.len
      st.payload.setLen(oldLen + toTake)
      for k in 0 ..< toTake:
        st.payload[oldLen + k] = p[i + k]
      i += toTake
      st.bytesRead += toTake
      st.remaining -= toTake
      st.chunkRemaining -= toTake
      if st.remaining == 0 and st.payload.len > 0:
        if ctx.onMessageCb != nil:
          ctx.onMessageCb(
            st.lastHeader.msgTypeId,
            st.lastHeader.msgStreamId,
            st.lastHeader.timestamp,
            (if st.payload.len > 0: cast[ptr byte](addr st.payload[0]) else: nil),
            st.payload.len,
            ctx.cbArg
          )
        st.payload.setLen(0)
        st.bytesRead = 0
        st.remaining = 0
        st.chunkRemaining = 0
      elif st.chunkRemaining > 0:
        ctx.partialCsid = csid
      # if st.remaining > 0 and st.bytesRead > 0:
      #   echo "chunk partial: csid=", csid, " type=", st.lastHeader.msgTypeId,
      #        " msgLen=", st.lastHeader.msgLength, " bytesRead=", st.bytesRead,
      #        " remaining=", st.remaining, " chunkRemaining=", st.chunkRemaining

  return i
