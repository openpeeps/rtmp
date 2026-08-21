# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

## This module implements a RTMP client for publishing live streams to an RTMP server,
## and a RTMP server for receiving and processing incoming RTMP connections.
##
## It uses powpow for asynchronous network I/O and supports basic RTMP commands,
## control messages, and zero-copy file streaming. The client can be used to
## publish live video/audio streams from FLV files, while the server can be
## extended to handle incoming streams as needed.

import std/[net, os, tables,
        random, times, posix, strutils, uri]

import powpow
import ./rtmpplaylist
import ./server/handshake

export rtmpplaylist
export Loop, run, close, stop

type
  RtmpHandshakeState* = enum
    ## Handshake states for client state machine
    hsSendC0C1, hsRecvS0S1S2, hsSendC2, hsDone

  ConnStage* = enum
    ## Connection stages for client state machine
    stInit, stHandshakeDone, stConnectSent, stConnectOk,
    stCreateStreamSent, stStreamIdOk, stPublishSent, stPublishing

  StreamFileCallback* = proc(client: RtmpClient, st: StreamState, bytesSent: int)
  StreamAudioCallback* = proc(client: RtmpClient, aac: AacStreamState)
  StreamErrorCallback* = proc(client: RtmpClient, st: StreamState, errMsg: cstring)

  StreamState* = ref object
    fd*: cint
    totalSize*: int64
    offset*: int64
    msgType*: uint8
    csid*: uint8
    msgStreamId*: uint32
    ts*: uint32
    lowWater*: int
    done*: bool
    tsOffset*: uint32
    tagInProgress*: bool
    tagRemaining*: int
    tagPayloadPos*: int64
    tagTsAbs*: uint32
    tagSentAny*: bool

  AacStreamState* = ref object
    fd*: cint
    pos*: int64
    sampleRate*: int
    channels*: int
    ts*: uint32
    seqHeaderSent*: bool
    msgStreamId*: uint32
    csid*: uint8
    lowWater*: int
    done*: bool
    tsRem*: int
    preRollFrames*: int

  RtmpPacketHeader = object
    fmt: uint8
    cid: uint32
    timestamp: uint32
    msgLen: uint32
    msgType: uint8
    msgStreamId: uint32

  ChunkStreamState* = object
    prev: RtmpPacketHeader
    lastHadExtended: bool
    lastDelta: uint32

  RtmpClient* = ref object
    loop*: Loop
    conn*: Connection
    host: string
    port: int
    scheme: string
      ## "rtmp" or "rtmps"
    handshakeState: RtmpHandshakeState
    c1: array[1536, byte]
    s1: array[1536, byte]
    s2: array[1536, byte]
    c2: array[1536, byte]
    handshakeBuf: seq[byte]
    stage: ConnStage
    txid: int
    app: string
    tcUrl: string
    streamName: string
    msgStreamId*: uint32
    stream: StreamState
    onStreamStart*: StreamFileCallback
    onStreamProgress*: StreamFileCallback
    onStreamEnd*: StreamFileCallback
    onStreamError*: StreamErrorCallback
    onError*: proc(c: RtmpClient, msg: string)
      ## Callback when server returns _error (connect failure, etc.)
    onPublishOk*: proc(c: RtmpClient)
    aac*: AacStreamState
    ps*: PlaylistState
    sendTimerId*: TimerId
    hsWatchdog: TimerId
      ## Guards against servers that accept TCP but never complete the RTMP handshake
    pendingCloseFd: cint
      ## A media fd retired while its range was still in flight; closed once idle
    sendLeadMs*: int = 1_200
    wallOriginMs*: int64
    lastSendMs*: int64
    inChunks*: Table[uint32, ChunkStreamState]
    ackWindow*: uint32
    inBytes*: uint64
    lastAcked*: uint64
    outChunkSize*: int = 2048
      ## Outbound chunk size

proc scheduleSend(client: RtmpClient, delayMs: int)
proc pushNextFlvTag(client: RtmpClient): bool

#
# AMF0 helpers
#
proc amf0PutString(s: string, outp: var seq[byte]) =
  outp.add 0x02.byte
  outp.add ((s.len shr 8) and 0xFF).byte
  outp.add (s.len and 0xFF).byte
  outp.add s.toOpenArrayByte(0, s.len - 1)

proc amf0PutNumber(txid: int, outp: var seq[byte]) =
  outp.add 0x00.byte
  case txid
  of 1: outp.add @[0x3F'u8,0xF0'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]
  of 2: outp.add @[0x40'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]
  of 3: outp.add @[0x40'u8,0x08'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]
  else: outp.add @[0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]

proc amf0PutNull(outp: var seq[byte]) =
  outp.add 0x05.byte

proc amf0PutBool(b: bool, outp: var seq[byte]) =
  outp.add 0x01.byte
  outp.add (if b: 1'u8 else: 0'u8)

proc amf0PutDouble(v: float64, outp: var seq[byte]) =
  outp.add 0x00.byte
  var tmp = v
  let raw = cast[ptr array[8, uint8]](addr tmp)
  for i in countdown(7, 0):
    outp.add raw[][i]

proc amf0ReadString(b: openArray[byte], i: var int): string =
  if i >= b.len or b[i] != 0x02.byte: return ""
  inc i
  if i+1 >= b.len: return ""
  let n = (int(b[i]) shl 8) or int(b[i+1]); i += 2
  if i+n > b.len: return ""
  result = cast[string](b[i ..< i+n])
  i += n

proc amf0ReadNumberAsInt(b: openArray[byte], i: var int): int =
  if i >= b.len or b[i] != 0x00.byte: return 0
  inc i
  if i+7 >= b.len: return 0
  let a = b[i ..< i+8]; i += 8
  if a == @[0x3F.byte,0xF0.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte]: return 1
  if a == @[0x40.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte]: return 2
  if a == @[0x40.byte,0x08.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte,0x00.byte]: return 3
  0

#
# RTMP Command messages
#
proc sendCommand(client: RtmpClient, csid: uint8, msgStreamId: uint32, payload: seq[byte]) =
  var hdr: array[12, uint8]
  hdr[0] = ((0'u8 shl 6) or csid)
  hdr[1] = 0; hdr[2] = 0; hdr[3] = 0
  let L = payload.len
  hdr[4] = ((L shr 16) and 0xFF).uint8
  hdr[5] = ((L shr 8) and 0xFF).uint8
  hdr[6] = (L and 0xFF).uint8
  hdr[7] = 0x14'u8
  hdr[8]  = (msgStreamId and 0xFF).uint8
  hdr[9]  = ((msgStreamId shr 8) and 0xFF).uint8
  hdr[10] = ((msgStreamId shr 16) and 0xFF).uint8
  hdr[11] = ((msgStreamId shr 24) and 0xFF).uint8
  discard client.conn.send(hdr)
  discard client.conn.send(payload)

proc sendConnect(client: RtmpClient) =
  var p: seq[byte] = @[]
  amf0PutString("connect", p)
  amf0PutNumber(1, p)
  p.add 0x03.byte
  p.add @[0x00.byte,0x03.byte]; p.add "app".toOpenArrayByte(0,2)
  amf0PutString(client.app, p)
  p.add @[0x00.byte,0x05.byte]; p.add "tcUrl".toOpenArrayByte(0,4)
  amf0PutString(client.tcUrl, p)
  p.add @[0x00.byte,0x08.byte]; p.add "flashVer".toOpenArrayByte(0,7)
  amf0PutString("FMLE/3.0 (compatible; RTMP-Nim)", p)
  p.add @[0x00.byte,0x06.byte]; p.add "swfUrl".toOpenArrayByte(0,5)
  amf0PutString("", p)
  p.add @[0x00.byte,0x07.byte]; p.add "pageUrl".toOpenArrayByte(0,6)
  amf0PutString("", p)
  p.add @[0x00.byte,0x04.byte]; p.add "fpad".toOpenArrayByte(0,3)
  amf0PutBool(false, p)
  p.add @[0x00.byte,0x0C.byte]; p.add "capabilities".toOpenArrayByte(0,11)
  amf0PutDouble(15.0, p)
  p.add @[0x00.byte,0x0A.byte]; p.add "audioCodecs".toOpenArrayByte(0,9)
  amf0PutDouble(3191.0, p)
  p.add @[0x00.byte,0x0A.byte]; p.add "videoCodecs".toOpenArrayByte(0,9)
  amf0PutDouble(252.0, p)
  p.add @[0x00.byte,0x0D.byte]; p.add "videoFunction".toOpenArrayByte(0,12)
  amf0PutDouble(1.0, p)
  p.add @[0x00.byte,0x0E.byte]; p.add "objectEncoding".toOpenArrayByte(0,13)
  amf0PutDouble(0.0, p)
  p.add @[0x00.byte,0x00.byte,0x09.byte]
  sendCommand(client, 3'u8, 0'u32, p)
  client.stage = stConnectSent

proc sendCreateStream(client: RtmpClient) =
  var p: seq[byte] = @[]
  amf0PutString("createStream", p)
  amf0PutNumber(2, p)
  amf0PutNull(p)
  sendCommand(client, 3'u8, 0'u32, p)
  client.stage = stCreateStreamSent

proc sendPublish(client: RtmpClient) =
  var p: seq[byte] = @[]
  amf0PutString("publish", p)
  amf0PutNumber(3, p)
  amf0PutNull(p)
  amf0PutString(client.streamName, p)
  amf0PutString("live", p)
  sendCommand(client, 3'u8, client.msgStreamId, p)
  client.stage = stPublishSent

#
# Control messages
#
proc sendSetChunkSize(client: RtmpClient, size: int) =
  var header: array[12, uint8]
  header[0] = ((0'u8 shl 6) or 2'u8)
  header[1] = 0; header[2] = 0; header[3] = 0
  header[4] = 0; header[5] = 0; header[6] = 4
  header[7] = 0x01'u8
  header[8] = 0; header[9] = 0; header[10] = 0; header[11] = 0
  var payload: array[4, uint8]
  payload[0] = ((size shr 24) and 0xFF).uint8
  payload[1] = ((size shr 16) and 0xFF).uint8
  payload[2] = ((size shr 8) and 0xFF).uint8
  payload[3] = (size and 0xFF).uint8
  discard client.conn.send(header)
  discard client.conn.send(payload)

proc u32be(b: openArray[byte], i: int): uint32 =
  (uint32(b[i]) shl 24) or (uint32(b[i+1]) shl 16) or (uint32(b[i+2]) shl 8) or uint32(b[i+3])

proc writeControl(conn: Connection, msgType: uint8, payloadLen: int) =
  var h: array[12, uint8]
  h[0] = ((0'u8 shl 6) or 2'u8)
  h[1] = 0; h[2] = 0; h[3] = 0
  h[4] = ((payloadLen shr 16) and 0xFF).uint8
  h[5] = ((payloadLen shr 8) and 0xFF).uint8
  h[6] = (payloadLen and 0xFF).uint8
  h[7] = msgType
  h[8] = 0; h[9] = 0; h[10] = 0; h[11] = 0
  discard conn.send(h)

proc sendWindowAck(client: RtmpClient, size: uint32) =
  writeControl(client.conn, 0x05'u8, 4)
  var p: array[4, uint8]
  p[0] = ((size shr 24) and 0xFF).uint8
  p[1] = ((size shr 16) and 0xFF).uint8
  p[2] = ((size shr 8) and 0xFF).uint8
  p[3] = (size and 0xFF).uint8
  discard client.conn.send(p)

proc sendPeerBandwidth(client: RtmpClient, size: uint32, limitType: uint8 = 2'u8) =
  writeControl(client.conn, 0x06'u8, 5)
  var p: array[5, uint8]
  p[0] = ((size shr 24) and 0xFF).uint8
  p[1] = ((size shr 16) and 0xFF).uint8
  p[2] = ((size shr 8) and 0xFF).uint8
  p[3] = (size and 0xFF).uint8
  p[4] = limitType
  discard client.conn.send(p)

proc sendAcknowledgement(client: RtmpClient, totalRecv: uint32) =
  writeControl(client.conn, 0x03'u8, 4)
  var p: array[4, uint8]
  p[0] = ((totalRecv shr 24) and 0xFF).uint8
  p[1] = ((totalRecv shr 16) and 0xFF).uint8
  p[2] = ((totalRecv shr 8) and 0xFF).uint8
  p[3] = (totalRecv and 0xFF).uint8
  discard client.conn.send(p)

proc sendUserControl(client: RtmpClient, eventType: uint16, eventData: uint32) =
  writeControl(client.conn, 0x04'u8, 6)
  var p: array[6, uint8]
  p[0] = ((eventType shr 8) and 0xFF).uint8
  p[1] = (eventType and 0xFF).uint8
  p[2] = ((eventData shr 24) and 0xFF).uint8
  p[3] = ((eventData shr 16) and 0xFF).uint8
  p[4] = ((eventData shr 8) and 0xFF).uint8
  p[5] = (eventData and 0xFF).uint8
  discard client.conn.send(p)

proc parseBasicHeader(data: openArray[byte], start: int): (uint8, uint32, int, bool) =
  if start >= data.len: return (0'u8, 0'u32, start, false)
  let b = data[start]
  let fmt = (b shr 6) and 0x03
  let low = b and 0x3F
  var idx = start + 1
  var cid: uint32
  if low == 0'u8:
    if idx >= data.len: return (fmt, 0'u32, start, false)
    cid = 64'u32 + uint32(data[idx])
    inc idx
  elif low == 1'u8:
    if idx + 1 >= data.len: return (fmt, 0'u32, start, false)
    cid = 64'u32 + uint32(data[idx]) + (uint32(data[idx+1]) shl 8)
    idx += 2
  else:
    cid = uint32(low)
  (fmt, cid, idx, true)

proc handleControlMessage(client: RtmpClient, msgType: uint8, payload: seq[byte]) =
  case msgType
  of 0x05'u8:
    if payload.len == 4:
      client.ackWindow = u32be(payload, 0)
      sendWindowAck(client, client.ackWindow)
      sendPeerBandwidth(client, client.ackWindow, 2'u8)
  of 0x06'u8:
    if payload.len >= 5:
      let bw = u32be(payload, 0)
      let typ = payload[4]
      sendPeerBandwidth(client, bw, 2'u8)
  of 0x01'u8:
    if payload.len == 4:
      let inCs = u32be(payload, 0)
      discard inCs
  of 0x04'u8:
    if payload.len >= 2:
      let evt = (uint16(payload[0]) shl 8) or uint16(payload[1])
      if evt == 6'u16 and payload.len >= 6:
        let pingTs = u32be(payload, 2)
        sendUserControl(client, 7'u16, pingTs)
      elif evt == 0'u16 and payload.len >= 6:
        let streamId = u32be(payload, 2)
        discard streamId
  else:
    discard

proc parseRtmpHeader(client: RtmpClient, data: openArray[byte],
                      start: int): (RtmpPacketHeader, int, bool) =
  var idx = start
  let (fmt, cid, nextIdx, okBasic) = parseBasicHeader(data, idx)
  if not okBasic: return (RtmpPacketHeader(), start, false)
  idx = nextIdx

  var st: ChunkStreamState
  let havePrev = client.inChunks.hasKey(cid)
  if havePrev: st = client.inChunks[cid]

  var hdr: RtmpPacketHeader
  hdr.fmt = fmt
  hdr.cid = cid

  var hadExt = false
  case fmt
  of 0'u8:
    if idx + 11 > data.len: return (RtmpPacketHeader(), start, false)
    let ts3 = (uint32(data[idx]) shl 16) or (uint32(data[idx+1]) shl 8) or uint32(data[idx+2]); idx += 3
    let msgLen = (uint32(data[idx]) shl 16) or (uint32(data[idx+1]) shl 8) or uint32(data[idx+2]); idx += 3
    let msgType = data[idx]; inc idx
    let msgStreamId = uint32(data[idx]) or (uint32(data[idx+1]) shl 8) or (uint32(data[idx+2]) shl 16) or (uint32(data[idx+3]) shl 24); idx += 4
    hdr.msgLen = msgLen
    hdr.msgType = msgType
    hdr.msgStreamId = msgStreamId
    if ts3 == 0xFFFFFF'u32:
      if idx + 4 > data.len: return (RtmpPacketHeader(), start, false)
      hdr.timestamp = (uint32(data[idx]) shl 24) or (uint32(data[idx+1]) shl 16) or (uint32(data[idx+2]) shl 8) or uint32(data[idx+3])
      idx += 4
      hadExt = true
      st.lastDelta = 0'u32
    else:
      hdr.timestamp = ts3
      st.lastDelta = 0'u32
  of 1'u8:
    if not havePrev: return (RtmpPacketHeader(), start, false)
    if idx + 7 > data.len: return (RtmpPacketHeader(), start, false)
    let d3 = (uint32(data[idx]) shl 16) or (uint32(data[idx+1]) shl 8) or uint32(data[idx+2]); idx += 3
    let msgLen = (uint32(data[idx]) shl 16) or (uint32(data[idx+1]) shl 8) or uint32(data[idx+2]); idx += 3
    let msgType = data[idx]; inc idx
    hdr.msgLen = msgLen
    hdr.msgType = msgType
    hdr.msgStreamId = st.prev.msgStreamId
    if d3 == 0xFFFFFF'u32:
      if idx + 4 > data.len: return (RtmpPacketHeader(), start, false)
      let dExt = (uint32(data[idx]) shl 24) or (uint32(data[idx+1]) shl 16) or (uint32(data[idx+2]) shl 8) or uint32(data[idx+3])
      idx += 4
      hdr.timestamp = st.prev.timestamp + dExt
      st.lastDelta = dExt
      hadExt = true
    else:
      hdr.timestamp = st.prev.timestamp + d3
      st.lastDelta = d3
  of 2'u8:
    if not havePrev: return (RtmpPacketHeader(), start, false)
    if idx + 3 > data.len: return (RtmpPacketHeader(), start, false)
    let d3 = (uint32(data[idx]) shl 16) or (uint32(data[idx+1]) shl 8) or uint32(data[idx+2]); idx += 3
    hdr.msgLen = st.prev.msgLen
    hdr.msgType = st.prev.msgType
    hdr.msgStreamId = st.prev.msgStreamId
    if d3 == 0xFFFFFF'u32:
      if idx + 4 > data.len: return (RtmpPacketHeader(), start, false)
      let dExt = (uint32(data[idx]) shl 24) or (uint32(data[idx+1]) shl 16) or (uint32(data[idx+2]) shl 8) or uint32(data[idx+3])
      idx += 4
      hdr.timestamp = st.prev.timestamp + dExt
      st.lastDelta = dExt
      hadExt = true
    else:
      hdr.timestamp = st.prev.timestamp + d3
      st.lastDelta = d3
  else:
    if not havePrev: return (RtmpPacketHeader(), start, false)
    hdr = st.prev
    if st.lastHadExtended:
      if idx + 4 > data.len: return (RtmpPacketHeader(), start, false)
      let dExt = (uint32(data[idx]) shl 24) or (uint32(data[idx+1]) shl 16) or (uint32(data[idx+2]) shl 8) or uint32(data[idx+3])
      idx += 4
      hdr.timestamp = st.prev.timestamp + dExt
      st.lastDelta = dExt
      hadExt = true
    else:
      hdr.timestamp = st.prev.timestamp + st.lastDelta
  st.prev = hdr
  st.lastHadExtended = hadExt
  client.inChunks[cid] = st
  result = (hdr, idx, true)

proc handleCommandMessage(client: RtmpClient, payload: seq[byte]) =
  var i = 0
  let cmd = amf0ReadString(payload, i)
  let tx  = amf0ReadNumberAsInt(payload, i)
  if i < payload.len and payload[i] == 0x03.byte:
    while i+2 < payload.len and not (payload[i] == 0x00 and payload[i+1] == 0x00 and payload[i+2] == 0x09.byte): inc i
    i += 3
  elif i < payload.len and payload[i] == 0x05.byte:
    inc i
  if cmd == "_result" and client.stage == stConnectSent:
    client.stage = stConnectOk
    sendCreateStream(client)
  elif cmd == "_result" and client.stage == stCreateStreamSent:
    let sid = amf0ReadNumberAsInt(payload, i)
    if sid > 0:
      client.msgStreamId = uint32(sid)
      client.stage = stStreamIdOk
      sendPublish(client)
  elif cmd == "onStatus" and client.stage == stPublishSent:
    client.stage = stPublishing
    if client.onPublishOk != nil:
      client.onPublishOk(client)
  elif cmd == "_error":
    # Server returned _error — extract description and notify
    var errMsg = ""
    if i < payload.len and payload[i] == 0x03.byte:
      # Skip the command object
      while i+2 < payload.len and not (payload[i] == 0x00 and payload[i+1] == 0x00 and payload[i+2] == 0x09.byte): inc i
      i += 3
    elif i < payload.len and payload[i] == 0x05.byte:
      inc i
    if i < payload.len:
      errMsg = amf0ReadString(payload, i)
    if client.onError != nil:
      client.onError(client, errMsg)
    # Reset state on fatal errors (connect/createStream failures)
    if client.stage in {stConnectSent, stCreateStreamSent}:
      client.stage = stInit

proc parseRtmpPackets(client: RtmpClient, data: openArray[byte]) =
  var idx = 0
  while idx < data.len:
    let (hdr, nextIdx, ok) = parseRtmpHeader(client, data, idx)
    if not ok: break
    idx = nextIdx
    if idx + int(hdr.msgLen) > data.len: break
    let payload = data[idx ..< idx+int(hdr.msgLen)]
    case hdr.msgType
    of 0x14'u8:
      handleCommandMessage(client, payload)
    of 0x01'u8, 0x04'u8, 0x05'u8, 0x06'u8, 0x03'u8:
      handleControlMessage(client, hdr.msgType, payload)
    else:
      discard
    idx += int(hdr.msgLen)

proc sendC0C1(client: RtmpClient) =
  # Create enhanced C1 with HMAC-SHA256 digest for interoperability
  var c0c1 = newSeq[byte](1 + RTMP_HANDSHAKE_SIZE)
  c0c1[0] = 0x03  # C0: version byte (always 0x03)
  # Create enhanced C1 (with HMAC digest at Scheme 0 position)
  createC1Enhanced(cast[ptr UncheckedArray[byte]](addr c0c1[1]))
  # Store C1 for later use
  for i in 0 ..< RTMP_HANDSHAKE_SIZE: client.c1[i] = c0c1[1 + i]
  discard client.conn.send(c0c1)
  client.handshakeState = hsRecvS0S1S2

proc sendC2(client: RtmpClient) =
  # Create C2 with HMAC signature over S1 using full FMS key
  var c2 = newSeq[byte](RTMP_HANDSHAKE_SIZE)
  signC2(cast[ptr UncheckedArray[byte]](addr c2[0]),
         cast[ptr UncheckedArray[byte]](addr client.s1[0]))
  discard client.conn.send(c2)
  client.handshakeState = hsDone
  client.stage = stHandshakeDone
  sendWindowAck(client, 20_000_000'u32)
  sendPeerBandwidth(client, 50_000_000'u32, 2'u8)
  sendSetChunkSize(client, client.outChunkSize)
  sendConnect(client)

#
# Zero-copy streaming with chunking and backpressure
#
proc closeStreamFd(st: var StreamState) =
  if st != nil and st.fd >= 0:
    discard posix.close(st.fd)
    st.fd = -1

proc retireMediaFd(client: RtmpClient, fd: cint) =
  ## Close a media fd, deferring if a sendfile range from it is still in
  ## flight (closing the active source would EBADF mid-transfer).
  if fd < 0: return
  if client.conn.sendFileActive():
    client.pendingCloseFd = fd
  else:
    discard posix.close(fd)

proc flushPendingClose(client: RtmpClient) =
  if client.pendingCloseFd >= 0 and not client.conn.sendFileActive():
    discard posix.close(client.pendingCloseFd)
    client.pendingCloseFd = -1

proc onDataReceived(conn: Connection, data: openArray[byte]) =
  ## powpow onData callback
  let client = cast[RtmpClient](conn.data)
  if client == nil: return
  let avail = data.len
  if avail == 0: return
  client.inBytes += avail.uint64

  if client.ackWindow != 0'u32 and (client.inBytes - client.lastAcked) >= client.ackWindow.uint64:
    client.lastAcked = client.inBytes
    sendAcknowledgement(client, uint32(client.inBytes and 0xFFFF_FFFF'u64))

  if client.handshakeState == hsRecvS0S1S2:
    client.handshakeBuf.add(data)
    if client.handshakeBuf.len >= 1+RTMP_HANDSHAKE_SIZE+RTMP_HANDSHAKE_SIZE:
      if client.handshakeBuf[0] != 0x03.byte:
        return
      for i in 0..<RTMP_HANDSHAKE_SIZE:
        client.s1[i] = client.handshakeBuf[1+i]
        client.s2[i] = client.handshakeBuf[1+RTMP_HANDSHAKE_SIZE+i]
      client.handshakeBuf.setLen(0)
      # Check if server responded with enhanced S1 (validate digest with FMS key)
      let s1ptr = cast[ptr UncheckedArray[byte]](addr client.s1[0])
      if isEnhancedS1(s1ptr) and validateS1Digest(s1ptr):
        discard  # Enhanced handshake accepted
      if client.hsWatchdog != TimerId(0):
        client.loop.cancelTimer(client.hsWatchdog)
        client.hsWatchdog = TimerId(0)
      client.sendC2()
  elif client.handshakeState == hsDone:
    parseRtmpPackets(client, data)

#
# ADTS/AAC Streaming parsing and AAC/FLV packing
#
proc samplingRateFromIndex(idx: int): int =
  let table = [96000,88200,64000,48000,44100,32000,24000,22050,16000,12000,11025,8000,7350]
  if idx >= 0 and idx < table.len: table[idx] else: 44100

proc parseAdts(fd: cint, pos: int64,
               frameLen: var int, headerLen: var int,
               profile: var int, sfIndex: var int, channels: var int): bool =
  var hdr: array[9, uint8]
  if posix.lseek(fd, pos, SEEK_SET) < 0: return false
  let n = posix.read(fd, addr hdr[0], 9)
  if n < 7: return false
  if hdr[0] != 0xFF'u8 or (hdr[1] and 0xF0'u8) != 0xF0'u8: return false
  let protectionAbsent = int(hdr[1] and 0x01'u8)
  profile = ((int(hdr[2]) shr 6) and 0x03) + 1
  sfIndex = (int(hdr[2]) shr 2) and 0x0F
  channels = (((int(hdr[2]) and 0x01) shl 2) or ((int(hdr[3]) shr 6) and 0x03))
  let f1 = (int(hdr[3]) and 0x03) shl 11
  let f2 = int(hdr[4]) shl 3
  let f3 = (int(hdr[5]) shr 5) and 0x07
  frameLen = f1 or f2 or f3
  headerLen = if protectionAbsent == 1: 7 else: 9
  true

proc buildAacAsc(profile, sfIndex, channels: int): seq[byte] =
  var asc = newSeq[byte](2)
  let x = (profile shl 11) or (sfIndex shl 7) or (channels shl 3)
  asc[0] = ((x shr 8) and 0xFF).uint8
  asc[1] = (x and 0xFF).uint8
  asc

proc writeExtendedTimestamp(conn: Connection, ts: uint32) =
  var ex: array[4, uint8]
  ex[0] = ((ts shr 24) and 0xFF).uint8
  ex[1] = ((ts shr 16) and 0xFF).uint8
  ex[2] = ((ts shr 8) and 0xFF).uint8
  ex[3] = (ts and 0xFF).uint8
  discard conn.send(ex)

proc writeRtmpFmt0Header(conn: Connection, csid: uint8,
            ts: uint32, msgLen: int, msgType: uint8, msgStreamId: uint32) =
  var h0: array[12, uint8]
  h0[0] = ((0'u8 shl 6) or csid)
  let ts3 = min(ts, 0xFFFFFF'u32)
  h0[1] = ((ts3 shr 16) and 0xFF).uint8
  h0[2] = ((ts3 shr 8) and 0xFF).uint8
  h0[3] = (ts3 and 0xFF).uint8
  h0[4] = ((msgLen shr 16) and 0xFF).uint8
  h0[5] = ((msgLen shr 8) and 0xFF).uint8
  h0[6] = (msgLen and 0xFF).uint8
  h0[7] = msgType
  h0[8]  = (msgStreamId and 0xFF).uint8
  h0[9]  = ((msgStreamId shr 8) and 0xFF).uint8
  h0[10] = ((msgStreamId shr 16) and 0xFF).uint8
  h0[11] = ((msgStreamId shr 24) and 0xFF).uint8
  discard conn.send(h0)
  if ts > 0xFFFFFF'u32:
    writeExtendedTimestamp(conn, ts)

proc writeRtmpFmt3Header(conn: Connection, csid: uint8, ts: uint32) =
  var b: uint8 = ((3'u8 shl 6) or csid)
  discard conn.send([b])
  if ts > 0xFFFFFF'u32:
    writeExtendedTimestamp(conn, ts)

proc pushNextAacFrame(client: RtmpClient): bool =
  let a = client.aac
  if a == nil or a.done: return
  # Backpressure: same contract — in flight means push nothing this tick.
  if client.conn.sendFileActive(): return false

  var frameLen, headerLen, profile, sfIndex, ch: int
  if not parseAdts(a.fd, a.pos, frameLen, headerLen, profile, sfIndex, ch):
    a.done = true
    return

  a.sampleRate = samplingRateFromIndex(sfIndex)
  a.channels = ch

  proc soundRateCode(sr: int): uint8 =
    if sr >= 44100: 3'u8
    elif sr >= 22050: 2'u8
    elif sr >= 11025: 1'u8
    else: 0'u8
  let soundType = if a.channels == 1: 0'u8 else: 1'u8
  let soundHeader = ((10'u8 shl 4) or (soundRateCode(a.sampleRate) shl 2) or (1'u8 shl 1) or soundType)

  if not a.seqHeaderSent:
    let asc = buildAacAsc(profile, sfIndex, ch)
    let msgLen = 1 + 1 + asc.len
    writeRtmpFmt0Header(client.conn, a.csid, a.ts, msgLen, 0x08'u8, a.msgStreamId)
    discard client.conn.send([soundHeader])
    var aacPktType0: uint8 = 0
    discard client.conn.send([aacPktType0])
    discard client.conn.send(asc)
    a.seqHeaderSent = true

  let rawLen = frameLen - headerLen
  if rawLen <= 0:
    a.pos += frameLen
    return

  let msgLen = 1 + 1 + rawLen
  writeRtmpFmt0Header(client.conn, a.csid, a.ts, msgLen, 0x08'u8, a.msgStreamId)
  discard client.conn.send([soundHeader])
  var aacPktType1: uint8 = 1
  discard client.conn.send([aacPktType1])

  # Zero-copy kernel sendfile for the frame payload. keepOpen=true because the
  # same ADTS fd feeds every subsequent frame.
  if not client.conn.sendFile(a.fd, a.pos + headerLen, int64(rawLen), keepOpen = true):
    # Transfer still draining / connection gone — retry on next pacer tick.
    return false

  if a.preRollFrames > 0: dec a.preRollFrames

  let stepNum = 1024 * 1000
  let stepDen = max(1, a.sampleRate)
  a.ts += uint32(stepNum div stepDen)
  a.tsRem += stepNum mod stepDen
  if a.tsRem >= stepDen:
    a.ts += 1'u32
    a.tsRem -= stepDen

  if client.ps.globalTs < a.ts: client.ps.globalTs = a.ts
  a.pos += frameLen
  result = true

proc startStreamAacAdtsZeroCopy*(client: RtmpClient, filePath: string,
                                 msgStreamId: uint32, csid: uint8 = 4'u8,
                                 lowWater: int = 256 * 1024, startTs: uint32 = 0'u32) =
  ## Start streaming an ADTS AAC file with zero-copy file segments.
  if client.aac != nil:
    retireMediaFd(client, client.aac.fd)
    client.aac = nil

  let fd = posix.open(filePath, O_RDONLY)
  if fd < 0:
    if client.onStreamError != nil:
      client.onStreamError(client, nil, "Failed to open AAC: " & filePath)
    return
  client.aac = AacStreamState(
    fd: fd, pos: 0'i64, sampleRate: 44100, channels: 2,
    seqHeaderSent: false, msgStreamId: msgStreamId,
    csid: csid, lowWater: lowWater, done: false, ts: startTs,
    tsRem: 0, preRollFrames: 3
  )

  if client.onStreamStart != nil:
    client.onStreamStart(client, nil, 0)

  discard pushNextAacFrame(client)
  if client.sendTimerId != TimerId(0): scheduleSend(client, 0)

#
# RTMP FLV
#
type
  FlvTagHeader = object
    tagType*: uint8
    dataSize*: int
    timestamp*: uint32
    posPayload*: int64

proc readFlvHeader(fd: cint): int64 =
  var hdr: array[9, uint8]
  if posix.lseek(fd, 0, SEEK_SET) < 0: return -1
  let n = posix.read(fd, addr hdr[0], 9)
  if n != 9: return -1
  if hdr[0] != 'F'.uint8 or hdr[1] != 'L'.uint8 or hdr[2] != 'V'.uint8: return -1
  let headerSize = (int(hdr[5]) shl 24) or (int(hdr[6]) shl 16) or (int(hdr[7]) shl 8) or int(hdr[8])
  int64(headerSize + 4)

proc readFlvTagHeader(fd: cint, pos: int64, th: var FlvTagHeader): bool =
  var h: array[11, uint8]
  if posix.lseek(fd, pos, SEEK_SET) < 0: return false
  let n = posix.read(fd, addr h[0], 11)
  if n != 11: return false
  th.tagType = h[0]
  let dsz = (int(h[1]) shl 16) or (int(h[2]) shl 8) or int(h[3])
  th.dataSize = dsz
  let ts = (uint32(h[4]) shl 16) or (uint32(h[5]) shl 8) or uint32(h[6]) or (uint32(h[7]) shl 24)
  th.timestamp = ts
  th.posPayload = pos + 11
  true

proc flvTagToRtmp(tagType: uint8): (uint8, uint8) =
  case tagType
  of 0x08'u8: (0x08'u8, 4'u8)
  of 0x09'u8: (0x09'u8, 6'u8)
  of 0x12'u8: (0x12'u8, 5'u8)
  else: (0x12'u8, 5'u8)

proc peekNextFlvTagTs(fd: cint, pos: int64, nextTs: var uint32): bool =
  var h: array[11, uint8]
  if posix.lseek(fd, pos, SEEK_SET) < 0: return false
  let n = posix.read(fd, addr h[0], 11)
  if n != 11: return false
  let ts = (uint32(h[4]) shl 16) or (uint32(h[5]) shl 8) or uint32(h[6]) or (uint32(h[7]) shl 24)
  nextTs = ts
  true

proc pushNextFlvTag(client: RtmpClient): bool =
  let st = client.stream
  if st == nil or st.done: return
  # Backpressure: wait for the previous range to drain first. Reporting
  # false breaks the send-until-limit loop; the next pacer tick retries.
  if client.conn.sendFileActive(): return false

  if not st.tagInProgress:
    while true:
      var th: FlvTagHeader
      if not readFlvTagHeader(st.fd, st.offset, th):
        st.done = true
        return
      if th.tagType == 0x08'u8 and client.aac != nil and not client.aac.done:
        st.offset = th.posPayload + th.dataSize.int64 + 4
        if st.offset >= st.totalSize:
          st.done = true
          return
        continue
      let (msgType, csid) = flvTagToRtmp(th.tagType)
      st.msgType = msgType
      st.csid = csid
      st.tagPayloadPos = th.posPayload
      st.tagRemaining = th.dataSize
      st.tagTsAbs = uint32(th.timestamp) + st.tsOffset
      writeRtmpFmt0Header(client.conn, csid, st.tagTsAbs, st.tagRemaining, msgType, st.msgStreamId)
      st.tagInProgress = true
      st.tagSentAny = false
      break

  let chunkSize = max(128, client.outChunkSize)
  let toSend = min(st.tagRemaining, chunkSize)
  if toSend <= 0:
    st.offset = st.tagPayloadPos + 4
    st.ts = st.tagTsAbs
    st.tagInProgress = false
    st.tagRemaining = 0
    st.tagSentAny = false
    if st.offset >= st.totalSize: st.done = true
    if client.ps.globalTs < st.ts: client.ps.globalTs = st.ts
    if client.onStreamProgress != nil:
      client.onStreamProgress(client, st, st.offset.int)
    return

  if st.tagSentAny:
    writeRtmpFmt3Header(client.conn, st.csid, st.tagTsAbs)

  # Zero-copy kernel sendfile for this tag chunk. keepOpen=true because the
  # same FLV fd feeds every subsequent chunk/tag.
  if not client.conn.sendFile(st.fd, st.tagPayloadPos, int64(toSend), keepOpen = true):
    # Transfer still draining / connection gone — retry this chunk next tick.
    return false

  st.tagPayloadPos += toSend.int64
  st.tagRemaining -= toSend
  st.tagSentAny = true

  if st.tagRemaining == 0:
    st.offset = st.tagPayloadPos + 4
    st.ts = st.tagTsAbs
    st.tagInProgress = false
    st.tagSentAny = false
    if st.offset >= st.totalSize: st.done = true

  if client.ps.globalTs < st.ts: client.ps.globalTs = st.ts
  if client.onStreamProgress != nil:
    client.onStreamProgress(client, st, st.offset.int)
  result = true

proc startStreamFlvZeroCopy*(client: RtmpClient, filePath: string,
                            msgStreamId: uint32,
                            lowWater: int = 256 * 1024, startTs: uint32 = 0'u32) =
  ## Start streaming an FLV file with zero-copy file segments.
  if client.stream != nil:
    retireMediaFd(client, client.stream.fd)
    client.stream = nil
  let fd = posix.open(filePath, O_RDONLY)
  if fd < 0:
    return
  let startPos = readFlvHeader(fd)
  if startPos < 0:
    discard posix.close(fd)
    return
  client.stream = StreamState(
    fd: fd,
    totalSize: int64(getFileSize(filePath)),
    offset: startPos,
    msgStreamId: msgStreamId,
    lowWater: lowWater,
    done: false,
    ts: 0'u32,
    tsOffset: startTs
  )

  if client.onStreamStart != nil:
    client.onStreamStart(client, client.stream, 0)

  discard pushNextFlvTag(client)
  scheduleSend(client, 0)

proc newRtmpClient*(address: string): RtmpClient =
  ## Create new RTMP client and initiate connection to address.
  ## Address should be in form "rtmp://host[:port]/app/streamKey".
  let loop = newLoop()
  let uri = parseUri(address)
  assert uri.scheme == "rtmp" or uri.scheme == "rtmps"
  let port =
    if uri.port.len > 0: parseInt(uri.port)
    else:
      if uri.scheme == "rtmp": 1935 else: 443
  new(result)
  let path = uri.path.split("/")
  result.loop = loop
  result.host = uri.hostname
  result.port = port

  let appPart = if path.len > 1: path[1] else: ""
  let streamPart = if path.len > 2: path[2] else: ""

  result.app = appPart
  result.tcUrl = uri.scheme & "://" & uri.hostname & "/" & appPart
  result.streamName = streamPart

  result.handshakeState = hsSendC0C1
  result.stage = stInit
  result.msgStreamId = 1
  result.inChunks = initTable[uint32, ChunkStreamState]()
  result.scheme = uri.scheme

  # Store client pointer on the connection's data slot for callbacks
  let client = result
  let clientPtr = cast[pointer](client)

  loop.connect(uri.hostname, port,
    onConnect = proc(conn: Connection) =
      conn.data = clientPtr
      client.conn = conn
      # Tune socket: disable Nagle, enlarge buffers
      let yes: cint = 1
      discard posix.setsockopt(conn.fd, IPPROTO_TCP, TCP_NODELAY, cast[pointer](addr yes), sizeof(cint).cuint)
      var snd: cint = 8 * 1024 * 1024
      discard posix.setsockopt(conn.fd, SOL_SOCKET, SO_SNDBUF, cast[pointer](addr snd), sizeof(cint).cuint)
      var rcv: cint = 8 * 1024 * 1024
      discard posix.setsockopt(conn.fd, SOL_SOCKET, SO_RCVBUF, cast[pointer](addr rcv), sizeof(cint).cuint)
      # Wrap with TLS for rtmps:// — handshake data is buffered until TLS is active
      if client.scheme == "rtmps":
        let tlsCtx = newClientTlsContext(verifyPeer = false)
        conn.wrapTls(tlsCtx, serverName = client.host)
      # Start handshake
      randomize()
      client.sendC0C1()
      # Watchdog: abort if the peer never answers the handshake
      client.hsWatchdog = client.loop.addTimer(10_000) do (id: int) {.closure.}:
        if client.handshakeState != hsDone:
          if client.onError != nil:
            client.onError(client, "handshake timeout")
          conn.close()
    ,
    onData = onDataReceived,
    onClose = proc(conn: Connection) {.closure.} =
      let client = cast[RtmpClient](conn.data)
      if client == nil: return
      if client.sendTimerId != TimerId(0):
        client.loop.cancelTimer(client.sendTimerId)
        client.sendTimerId = TimerId(0)
      if client.hsWatchdog != TimerId(0):
        client.loop.cancelTimer(client.hsWatchdog)
        client.hsWatchdog = TimerId(0)
    ,
    onError = proc(err: string) {.closure.} =
      # Surface transport-level failures (refused, unreachable, reset) to the app
      if not client.isNil and client.onError != nil:
        client.onError(client, err)
      if client.sendTimerId != TimerId(0):
        client.loop.cancelTimer(client.sendTimerId)
        client.sendTimerId = TimerId(0)
      if client.hsWatchdog != TimerId(0):
        client.loop.cancelTimer(client.hsWatchdog)
        client.hsWatchdog = TimerId(0)
  )

proc nowMs(): int64 =
  when declared(posix.clock_gettime):
    var ts: posix.Timespec
    discard posix.clock_gettime(posix.CLOCK_MONOTONIC, ts)
    ts.tv_sec.int64 * 1000 + ts.tv_nsec.int64 div 1_000_000
  else:
    var tv: posix.Timeval
    discard posix.gettimeofday(addr tv, nil)
    tv.tv_sec.int64 * 1000 + tv.tv_usec.int64 div 1000

proc computeWallTsLimit(client: RtmpClient): uint32 =
  let now = nowMs()
  let deltaMs = max(0'i64, now - client.wallOriginMs) + client.sendLeadMs.int64
  uint32(deltaMs)

proc sendUntilLimit(client: RtmpClient, limitTs: uint32) =
  while true:
    var pushed = false
    var nextVideoTs: uint32 = high(uint32)
    var nextAudioTs: uint32 = high(uint32)

    if client.stream != nil and not client.stream.done:
      if client.stream.tagInProgress:
        nextVideoTs = client.stream.tagTsAbs
      else:
        discard peekNextFlvTagTs(client.stream.fd, client.stream.offset, nextVideoTs)
        nextVideoTs = nextVideoTs + client.stream.tsOffset

    if client.aac != nil and not client.aac.done:
      nextAudioTs = client.aac.ts

    let chooseVideo =
      client.stream != nil and not client.stream.done and
      (client.aac == nil or client.aac.done or nextVideoTs + 5'u32 < nextAudioTs)

    if chooseVideo:
      if nextVideoTs <= limitTs: pushed = pushNextFlvTag(client)
    elif client.aac != nil and not client.aac.done:
      if nextAudioTs <= limitTs: pushed = pushNextAacFrame(client)

    if not pushed: break

proc scheduleSend(client: RtmpClient, delayMs: int) =
  var d = delayMs
  if d < 5: d = 5
  if d > 1000: d = 1000
  let clientAddr = client
  if client.sendTimerId != TimerId(0):
    client.loop.cancelTimer(client.sendTimerId)
  client.sendTimerId = client.loop.addTimer(d) do (id: int):
    clientAddr.lastSendMs = nowMs()
    let limitTs = computeWallTsLimit(clientAddr)
    sendUntilLimit(clientAddr, limitTs)

    # Per-stream end events fire independently (video and audio each notify
    # on their own completion), but only once the socket has fully drained —
    # closing a media fd while its range is in flight would EBADF.
    flushPendingClose(clientAddr)
    let drained = clientAddr.conn.flushWriteBuffer() and
                  not clientAddr.conn.sendFileActive()
    var restarted = false
    if drained:
      if clientAddr.stream != nil and clientAddr.stream.done:
        var finished = clientAddr.stream
        closeStreamFd(finished)
        if clientAddr.onStreamEnd != nil:
          clientAddr.onStreamEnd(clientAddr, finished, int(finished.offset))
        if clientAddr.stream == finished:
          clientAddr.stream = nil
        else:
          restarted = true  # callback already started the next video stream
      if clientAddr.aac != nil and clientAddr.aac.done:
        let a = clientAddr.aac
        retireMediaFd(clientAddr, a.fd)
        if clientAddr.onStreamEnd != nil:
          var dummy = StreamState(
            fd: -1,
            totalSize: 0'i64,
            offset: a.pos,
            msgType: 0x08'u8,
            csid: a.csid,
            msgStreamId: a.msgStreamId,
            ts: a.ts,
            lowWater: a.lowWater,
            done: true,
            tagTsAbs: a.ts,
          )
          clientAddr.onStreamEnd(clientAddr, dummy, int(a.pos))
        if clientAddr.aac == a:
          clientAddr.aac = nil
        else:
          restarted = true  # callback already started the next audio stream

    # Scheduling: next media timestamp, or keep polling while draining.
    var nextTs: int64 = -1
    if clientAddr.stream != nil and not clientAddr.stream.done:
      var peekTs: uint32
      if peekNextFlvTagTs(clientAddr.stream.fd, clientAddr.stream.offset, peekTs):
        nextTs = (peekTs + clientAddr.stream.tsOffset).int64
    if clientAddr.aac != nil and not clientAddr.aac.done and (nextTs < 0 or clientAddr.aac.ts.int64 < nextTs):
      nextTs = clientAddr.aac.ts.int64

    if nextTs >= 0:
      let now = nowMs()
      let targetMs = clientAddr.wallOriginMs + nextTs - clientAddr.sendLeadMs.int64
      scheduleSend(clientAddr, int(targetMs - now))
    elif restarted:
      # A replay was kicked from inside onStreamEnd; it armed its own timer.
      discard
    elif not drained:
      # Files are at EOF but bytes are still in flight — do NOT let the pacer
      # die here; poll until the socket drains so end-events can fire.
      scheduleSend(clientAddr, 25)
    else:
      if clientAddr.sendTimerId != TimerId(0):
        clientAddr.loop.cancelTimer(clientAddr.sendTimerId)
        clientAddr.sendTimerId = TimerId(0)

proc startPacer*(client: RtmpClient, initCb: proc(c: RtmpClient) = nil) =
  ## Start pacing loop if not already active, and call initCb for any one-time initialization.
  if client.sendTimerId == TimerId(0):
    client.wallOriginMs = nowMs() - int64(client.ps.globalTs)
    client.lastSendMs = nowMs()
    if initCb != nil:
      initCb(client)
    scheduleSend(client, 0)
