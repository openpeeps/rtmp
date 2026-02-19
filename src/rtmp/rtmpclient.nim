# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

## This module implements a RTMP client for publishing live streams to an RTMP server,
## and a RTMP server for receiving and processing incoming RTMP connections.
## 
## It uses libevent for asynchronous network I/O and supports basic RTMP commands,
## control messages, and zero-copy file streaming. The client can be used to
## publish live video/audio streams from FLV files, while the server can be
## extended to handle incoming streams as needed.

import std/[net, os, sequtils, tables,
        random, times, posix, strutils, uri]

import pkg/libevent/bindings/[event, buffer, bufferevent]
import ./rtmpplaylist

export rtmpplaylist
export event_base_dispatch, event_base_free, bufferevent_free

type
  RtmpHandshakeState* = enum
    ## Handshake states for client state machine
    hsSendC0C1, hsRecvS0S1S2, hsSendC2, hsDone

  ConnStage* = enum
    ## Connection stages for client state machine
    stInit, stHandshakeDone, stConnectSent, stConnectOk,
    stCreateStreamSent, stStreamIdOk, stPublishSent, stPublishing

  StreamFileCallback* = proc(client: RtmpClient, st: StreamState, bytesSent: int)
    ## Callback for streaming events, called with current stream state and bytes sent in this callback (for pacing)
  StreamAudioCallback* = proc(client: RtmpClient, aac: AacStreamState)
    ## Callback for AAC streaming events, called with current AAC stream state (for pacing)
  StreamErrorCallback* = proc(client: RtmpClient, st: StreamState, errMsg: cstring)
    ## Callback for streaming errors, called with current stream state and error message

  StreamState* = ref object
    ## Streaming state for zero-copy file streaming
    fd*: cint
      ## File descriptor
    totalSize*: int64
      ## Total size of the file
    offset*: int64
      ## Current offset in the file
    msgType*: uint8
      ## RTMP message type (e.g., 0x09 for video, 0x08 for audio)
    csid*: uint8
      ## Chunk Stream ID
    msgStreamId*: uint32
      ## Message Stream ID
    ts*: uint32
      ## Timestamp
    lowWater*: int
      ## Low water mark for backpressure
    done*: bool
      ## Whether streaming is done
    tsOffset*: uint32
      ## Timestamp offset for synchronization
    tagInProgress*: bool
      ## Whether we're in the middle of sending a tag (for pacing)
    tagRemaining*: int
      ## Remaining bytes in the current tag being sent
    tagPayloadPos*: int64
      ## File position of the current tag payload being sent
    tagTsAbs*: uint32
      ## Absolute timestamp of the current tag being sent (ts + tsOffset)
    tagSentAny*: bool
      ## Whether we've sent any bytes of the current tag (for pacing)

  AacStreamState* = ref object
    ## AAC streaming state for audio streaming
    fd*: cint
      ## File descriptor
    pos*: int64
      ## Current position in the file
    sampleRate*: int
      ## Sampling rate
    channels*: int
      ## Number of channels
    ts*: uint32
      ## Timestamp
    seqHeaderSent*: bool
      ## Whether AAC sequence header has been sent
    msgStreamId*: uint32
      ## Message Stream ID
    csid*: uint8            # use 4 for audio
      ## Chunk Stream ID
    lowWater*: int
      ## Low water mark for backpressure
    done*: bool
      ## Whether streaming is done
    tsRem*: int
      ## Remaining timestamp for pacing
    preRollFrames*: int
      ## Pre-roll frames for AAC

  RtmpPacketHeader = object
    fmt: uint8
    cid: uint32
    timestamp: uint32
    msgLen: uint32
    msgType: uint8
    msgStreamId: uint32
  
  ChunkStreamState* = object
    prev: RtmpPacketHeader
      ## Previous RTMP packet header for this chunk stream
    lastHadExtended: bool
      ## Whether the last chunk had an extended timestamp
    lastDelta: uint32
      ## Last timestamp delta

  RtmpClient* = ref object
    base*: ptr event_base
    bev*: ptr bufferevent
    host: string
    port: int
    handshakeState: RtmpHandshakeState
    c1: array[1536, byte]
    s1: array[1536, byte]
    s2: array[1536, byte]
    c2: array[1536, byte]
    handshakeBuf: seq[byte]
    stage: ConnStage
      # Current connection stage
    txid: int
      # Transaction ID counter
    app: string
      # RTMP app and stream
    tcUrl: string
      # RTMP tcUrl (e.g., rtmp://host:port/app)
    streamName: string
      # Stream name to publish
    msgStreamId*: uint32
      # Message Stream ID assigned by server
    stream: StreamState
      ## Current streaming state
    onStreamStart*: StreamFileCallback
      ## Callbacks for streaming events
    onStreamProgress*: StreamFileCallback
      ## Callbacks for streaming events
    onStreamEnd*: StreamFileCallback
      ## Callbacks for streaming events
    onStreamError*: StreamErrorCallback
      ## Callbacks for streaming events
    onPublishOk*: proc(c: RtmpClient)
      ## Callback when publish is successful
    aac*: AacStreamState
      ## AAC state for audio streaming (if needed)
    ps*: PlaylistState
      ## Playlist state for synchronized streaming
    sendTimer*: ptr event
      ## libevent timer for pacing
    sendLeadMs*: int = 1_200
      ## milliseconds of lead to keep ahead of real-time
    wallOriginMs*: int64
      ## wall-clock ms corresponding to globalTs==0
    lastSendMs*: int64
      ## last time we actually scheduled/sent
    inChunks*: Table[uint32, ChunkStreamState]
      ## Incoming chunk streams by chunk stream ID
    
    # Flow control/keepalive
    ackWindow*: uint32
      ## Acknowledgement window size
    inBytes*: uint64
      ## Total bytes received
    lastAcked*: uint64
      ## Last acknowledged byte count
    outChunkSize*: int = 2048 # 2 KB
      ## Outbound chunk size

proc scheduleSend(client: RtmpClient, delayMs: int)
proc sendTimerCb(fd: cint, what: cshort, arg: pointer) {.cdecl.}
proc pushNextFlvTag(client: RtmpClient): bool

proc isIPv4(host: string): bool =
  # Simple check for IPv4 format (four dot-separated numbers)
  let parts = host.split('.')
  if parts.len != 4: return false
  for p in parts:
    if p.len == 0: return false
    for ch in p:
      if ch < '0' or ch > '9': return false
    let v = try:
      parseInt(p)
    except ValueError:
      return false
    if v < 0 or v > 255: return false
  true

proc isIpAddress(host: string): bool =
  # Check if the host is an IP address (IPv4 or IPv6)
  if isIPv4(host): return true
  if host.contains(':'): return true # simple check for IPv6 format (contains colons)
  false

#
# AMF0 helpers
#
proc amf0PutString(s: string, outp: var seq[byte]) =
  # AMF0 string (0x02) + 2-byte length + data
  outp.add 0x02.byte
  outp.add ((s.len shr 8) and 0xFF).byte
  outp.add (s.len and 0xFF).byte
  outp.add s.toOpenArrayByte(0, s.len - 1)

proc amf0PutNumber(txid: int, outp: var seq[byte]) =
  # AMF0 number (0x00) + 8-byte IEEE-754 big-endian double
  outp.add 0x00.byte
  case txid
  of 1: outp.add @[0x3F'u8,0xF0'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]
  of 2: outp.add @[0x40'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]
  of 3: outp.add @[0x40'u8,0x08'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]
  else: outp.add @[0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8,0x00'u8]

proc amf0PutNull(outp: var seq[byte]) =
  # AMF0 null (0x05)
  outp.add 0x05.byte

proc amf0PutBool(b: bool, outp: var seq[byte]) =
  # AMF0 boolean (0x01) + 1-byte value
  outp.add 0x01.byte
  outp.add (if b: 1'u8 else: 0'u8)

proc amf0PutDouble(v: float64, outp: var seq[byte]) =
  # AMF0 number (0x00) + 8-byte IEEE-754 big-endian double
  outp.add 0x00.byte
  var tmp = v
  let raw = cast[ptr array[8, uint8]](addr tmp)   # access raw bytes of the float64
  # write big-endian (most-significant byte first)
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
  hdr[7] = 0x14'u8 # AMF0 command message
  hdr[8]  = (msgStreamId and 0xFF).uint8
  hdr[9]  = ((msgStreamId shr 8) and 0xFF).uint8
  hdr[10] = ((msgStreamId shr 16) and 0xFF).uint8
  hdr[11] = ((msgStreamId shr 24) and 0xFF).uint8
  discard bufferevent_write(client.bev, addr hdr[0], 12.csize_t)
  discard bufferevent_write(client.bev, unsafeAddr payload[0], payload.len.csize_t)

proc sendConnect(client: RtmpClient) =
  var p: seq[byte] = @[]
  amf0PutString("connect", p)
  amf0PutNumber(1, p)
  p.add 0x03.byte # object start

  # app
  p.add @[0x00.byte,0x03.byte]; p.add "app".toOpenArrayByte(0,2)
  amf0PutString(client.app, p)

  # tcUrl
  p.add @[0x00.byte,0x05.byte]; p.add "tcUrl".toOpenArrayByte(0,4)
  amf0PutString(client.tcUrl, p)

  # flashVer (recommended)
  p.add @[0x00.byte,0x08.byte]; p.add "flashVer".toOpenArrayByte(0,7)
  amf0PutString("FMLE/3.0 (compatible; RTMP-Nim)", p)

  # swfUrl (optional)
  # should point to the SWF player URL if applicable
  p.add @[0x00.byte,0x06.byte]; p.add "swfUrl".toOpenArrayByte(0,5)
  amf0PutString("", p)

  # pageUrl (optional) should point to the page embedding the player
  p.add @[0x00.byte,0x07.byte]; p.add "pageUrl".toOpenArrayByte(0,6)
  amf0PutString("", p)

  # fpad (boolean, often false)
  # indicates if a Flash player is using a fixed-size padding
  p.add @[0x00.byte,0x04.byte]; p.add "fpad".toOpenArrayByte(0,3)
  amf0PutBool(false, p)

  # capabilities (number) - common value 15
  p.add @[0x00.byte,0x0C.byte]; p.add "capabilities".toOpenArrayByte(0,11)
  amf0PutDouble(15.0, p)

  # audioCodecs (number) - include AAC bit if needed; common value 3191 (example)
  p.add @[0x00.byte,0x0A.byte]; p.add "audioCodecs".toOpenArrayByte(0,9)
  amf0PutDouble(3191.0, p)

  # videoCodecs (number) - include H264 bit; common value 252 (example)
  p.add @[0x00.byte,0x0A.byte]; p.add "videoCodecs".toOpenArrayByte(0,9)
  amf0PutDouble(252.0, p)

  # videoFunction (number) - usually 1
  p.add @[0x00.byte,0x0D.byte]; p.add "videoFunction".toOpenArrayByte(0,12)
  amf0PutDouble(1.0, p)

  # objectEncoding (number) - 0 for AMF0
  p.add @[0x00.byte,0x0E.byte]; p.add "objectEncoding".toOpenArrayByte(0,13)
  amf0PutDouble(0.0, p)

  # end
  p.add @[0x00.byte,0x00.byte,0x09.byte]
  sendCommand(client, 3'u8, 0'u32, p)
  client.stage = stConnectSent
  ## debugEcho "[rtmp] Sent connect"

proc sendCreateStream(client: RtmpClient) =
  var p: seq[byte] = @[]
  amf0PutString("createStream", p)
  amf0PutNumber(2, p)
  amf0PutNull(p)
  sendCommand(client, 3'u8, 0'u32, p)
  client.stage = stCreateStreamSent
  ## debugEcho "[rtmp] Sent createStream"

proc sendPublish(client: RtmpClient) =
  var p: seq[byte] = @[]
  amf0PutString("publish", p)
  amf0PutNumber(3, p)
  amf0PutNull(p)
  amf0PutString(client.streamName, p)
  amf0PutString("live", p)
  sendCommand(client, 3'u8, client.msgStreamId, p)
  client.stage = stPublishSent
  ## debugEcho "[rtmp] Sent publish"

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
  discard bufferevent_write(client.bev, addr header[0], 12.csize_t)
  discard bufferevent_write(client.bev, addr payload[0], 4.csize_t)
  ## debugEcho "[rtmp] Set chunk size to ", size

proc u32be(b: openArray[byte], i: int): uint32 =
  (uint32(b[i]) shl 24) or (uint32(b[i+1]) shl 16) or (uint32(b[i+2]) shl 8) or uint32(b[i+3])

proc writeControl(bev: ptr bufferevent, msgType: uint8, payloadLen: int) =
  var h: array[12, uint8]
  # Control messages use chunk stream ID 2, message stream id 0
  h[0] = ((0'u8 shl 6) or 2'u8)
  h[1] = 0; h[2] = 0; h[3] = 0
  h[4] = ((payloadLen shr 16) and 0xFF).uint8
  h[5] = ((payloadLen shr 8) and 0xFF).uint8
  h[6] = (payloadLen and 0xFF).uint8
  h[7] = msgType
  h[8] = 0; h[9] = 0; h[10] = 0; h[11] = 0
  discard bufferevent_write(bev, addr h[0], 12.csize_t)

proc sendWindowAck(client: RtmpClient, size: uint32) =
  writeControl(client.bev, 0x05'u8, 4)
  var p: array[4, uint8]
  p[0] = ((size shr 24) and 0xFF).uint8
  p[1] = ((size shr 16) and 0xFF).uint8
  p[2] = ((size shr 8) and 0xFF).uint8
  p[3] = (size and 0xFF).uint8
  discard bufferevent_write(client.bev, addr p[0], 4.csize_t)
  ## debugEcho "[rtmp] Sent WindowAck=", size

proc sendPeerBandwidth(client: RtmpClient, size: uint32, limitType: uint8 = 2'u8) =
  writeControl(client.bev, 0x06'u8, 5)
  var p: array[5, uint8]
  p[0] = ((size shr 24) and 0xFF).uint8
  p[1] = ((size shr 16) and 0xFF).uint8
  p[2] = ((size shr 8) and 0xFF).uint8
  p[3] = (size and 0xFF).uint8
  p[4] = limitType
  discard bufferevent_write(client.bev, addr p[0], 5.csize_t)
  ## debugEcho "[rtmp] Sent PeerBandwidth=", size, " type=", limitType

proc sendAcknowledgement(client: RtmpClient, totalRecv: uint32) =
  writeControl(client.bev, 0x03'u8, 4)
  var p: array[4, uint8]
  p[0] = ((totalRecv shr 24) and 0xFF).uint8
  p[1] = ((totalRecv shr 16) and 0xFF).uint8
  p[2] = ((totalRecv shr 8) and 0xFF).uint8
  p[3] = (totalRecv and 0xFF).uint8
  discard bufferevent_write(client.bev, addr p[0], 4.csize_t)
  ## debugEcho "[rtmp] Sent Ack totalRecv=", totalRecv

proc sendUserControl(client: RtmpClient, eventType: uint16, eventData: uint32) =
  writeControl(client.bev, 0x04'u8, 6)
  var p: array[6, uint8]
  p[0] = ((eventType shr 8) and 0xFF).uint8
  p[1] = (eventType and 0xFF).uint8
  p[2] = ((eventData shr 24) and 0xFF).uint8
  p[3] = ((eventData shr 16) and 0xFF).uint8
  p[4] = ((eventData shr 8) and 0xFF).uint8
  p[5] = (eventData and 0xFF).uint8
  discard bufferevent_write(client.bev, addr p[0], 6.csize_t)

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
  # Handle RTMP control messages from server
  case msgType
  of 0x05'u8: # Window Acknowledgement Size
    if payload.len == 4:
      client.ackWindow = u32be(payload, 0)
      ## debugEcho "[rtmp] server WindowAck=", client.ackWindow
      # Mirror back a window and peer bandwidth to be explicit
      sendWindowAck(client, client.ackWindow)
      sendPeerBandwidth(client, client.ackWindow, 2'u8) # dynamic
  of 0x06'u8: # Set Peer Bandwidth
    if payload.len >= 5:
      let bw = u32be(payload, 0)
      let typ = payload[4]
      ## debugEcho "[rtmp] server PeerBandwidth=", bw, " type=", typ
      # Optionally echo back with 'dynamic'
      sendPeerBandwidth(client, bw, 2'u8)
  of 0x01'u8: # Set Chunk Size
    if payload.len == 4:
      let inCs = u32be(payload, 0)
      ## debugEcho "[rtmp] server SetChunkSize=", inCs
  of 0x04'u8: # User Control
    if payload.len >= 2:
      let evt = (uint16(payload[0]) shl 8) or uint16(payload[1])
      # PingRequest (6) > PingResponse (7)
      if evt == 6'u16 and payload.len >= 6:
        let pingTs = u32be(payload, 2)
        ## debugEcho "[rtmp] PingRequest ts=", pingTs
        sendUserControl(client, 7'u16, pingTs)
      elif evt == 0'u16 and payload.len >= 6:
        let streamId = u32be(payload, 2)
        ## debugEcho "[rtmp] StreamBegin id=", streamId
      else:
        ## debugEcho "[rtmp] UserControl evt=", evt
  else:
    discard

proc parseRtmpHeader(client: RtmpClient, data: openArray[byte],
                      start: int): (RtmpPacketHeader, int, bool) =
  # Parse RTMP packet header from data starting at 'start' index
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
    # 11-byte message header
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
    # 7-byte: timestamp delta(3), msgLen(3), msgType(1), msgStreamId unchanged
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
    # 3-byte: timestamp delta(3), other fields unchanged
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
  else: # fmt=3
    if not havePrev: return (RtmpPacketHeader(), start, false)
    hdr = st.prev
    # if last chunk used extended timestamp, a 4-byte extended field follows
    if st.lastHadExtended:
      if idx + 4 > data.len: return (RtmpPacketHeader(), start, false)
      let dExt = (uint32(data[idx]) shl 24) or (uint32(data[idx+1]) shl 16) or (uint32(data[idx+2]) shl 8) or uint32(data[idx+3])
      idx += 4
      hdr.timestamp = st.prev.timestamp + dExt
      st.lastDelta = dExt
      hadExt = true
    else:
      hdr.timestamp = st.prev.timestamp + st.lastDelta
  # store state
  st.prev = hdr
  st.lastHadExtended = hadExt
  client.inChunks[cid] = st
  result = (hdr, idx, true)

proc handleCommandMessage(client: RtmpClient, payload: seq[byte]) =
  # Handle RTMP command message (AMF0)
  var i = 0
  let cmd = amf0ReadString(payload, i)
  let tx  = amf0ReadNumberAsInt(payload, i)
  ## debugEcho "[rtmp] Got command: ", cmd, " tx=", tx, " payloadLen=", payload.len
  # Skip command object/null if present
  if i < payload.len and payload[i] == 0x03.byte:
    while i+2 < payload.len and not (payload[i] == 0x00 and payload[i+1] == 0x00 and payload[i+2] == 0x09.byte): inc i
    i += 3
  elif i < payload.len and payload[i] == 0x05.byte:
    inc i
  # Transitions
  if cmd == "_result" and client.stage == stConnectSent:
    client.stage = stConnectOk
    ## debugEcho "[rtmp] connect ok"
    sendCreateStream(client)
  elif cmd == "_result" and client.stage == stCreateStreamSent:
    let sid = amf0ReadNumberAsInt(payload, i)
    if sid > 0:
      client.msgStreamId = uint32(sid)
      client.stage = stStreamIdOk
      ## debugEcho "[rtmp] streamId=", sid
      sendPublish(client)
  elif cmd == "onStatus" and client.stage == stPublishSent:
    client.stage = stPublishing
    ## debugEcho "[rtmp] publish ok"
    if client.onPublishOk != nil:
      client.onPublishOk(client)

proc parseRtmpPackets(client: RtmpClient, data: openArray[byte]) =
  # Parse RTMP packets from data buffer
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
  # Handshake C0+C1
  var c0c1 = newSeq[byte](1 + 1536)
  c0c1[0] = 0x03
  let t = epochTime().int32
  c0c1[1] = (t shr 24).byte
  c0c1[2] = (t shr 16).byte
  c0c1[3] = (t shr 8).byte
  c0c1[4] = (t).byte
  for i in 5 ..< 1+1536: c0c1[i] = rand(255).byte
  for i in 0..<1536: client.c1[i] = c0c1[1+i]
  discard bufferevent_write(client.bev, addr c0c1[0], c0c1.len.csize_t)
  client.handshakeState = hsRecvS0S1S2

proc sendC2(client: RtmpClient) =
  discard bufferevent_write(client.bev, addr client.s1[0], 1536.csize_t)
  client.handshakeState = hsDone
  client.stage = stHandshakeDone
  ## debugEcho "[rtmp] Sent C2, handshake done"
  # Flow-control defaults (safer values)
  sendWindowAck(client, 20_000_000'u32) # 20 MB
  sendPeerBandwidth(client, 50_000_000'u32, 2'u8) # dynamic
  sendSetChunkSize(client, client.outChunkSize)
  sendConnect(client)

#
# Zero-copy streaming with chunking and backpressure
#
proc closeStreamFd(st: var StreamState) =
  if st != nil and st.fd >= 0:
    discard posix.close(st.fd)
    st.fd = -1

proc onRead(bev: ptr bufferevent, ctx: pointer) {.cdecl.} =
  # Libevent read callback
  let client = cast[RtmpClient](ctx)
  let buf = bufferevent_get_input(bev)
  let avail = evbuffer_get_length(buf)
  if avail == 0: return
  var tmp = newSeq[byte](avail)
  discard evbuffer_remove(buf, addr tmp[0], avail)
  # track inbound bytes and send ack when crossing window
  client.inBytes += tmp.len.uint64
  
  if client.ackWindow != 0'u32 and (client.inBytes - client.lastAcked) >= client.ackWindow.uint64:
    # Send acknowledgement
    client.lastAcked = client.inBytes
    sendAcknowledgement(client, uint32(client.inBytes and 0xFFFF_FFFF'u64))
  
  if client.handshakeState == hsRecvS0S1S2:
    # Accumulate handshake bytes
    client.handshakeBuf.add(tmp)
    if client.handshakeBuf.len >= 1+1536+1536:
      if client.handshakeBuf[0] != 0x03.byte:
        ## debugEcho "[rtmp] Unexpected S0 version"; return
      for i in 0..<1536:
        client.s1[i] = client.handshakeBuf[1+i]
        client.s2[i] = client.handshakeBuf[1+1536+i]
      ## debugEcho "[rtmp] Received S0, S1, S2"
      client.handshakeBuf.setLen(0)
      client.sendC2()
  elif client.handshakeState == hsDone:
    # Normal RTMP packet parsing
    parseRtmpPackets(client, tmp)

proc onWrite(bev: ptr bufferevent, ctx: pointer) {.cdecl.} =
  let client = cast[RtmpClient](ctx)
  let outLen = evbuffer_get_length(bufferevent_get_output(bev)).int

  # If pacer is active, do NOT push more, but still check for completion.
  if client.sendTimer != nil:
    if client.stream != nil and client.stream.done and outLen == 0:
      var finished = client.stream
      closeStreamFd(finished)                 # safe to close when drained
      if client.onStreamEnd != nil:
        client.onStreamEnd(client, finished, int(finished.offset))
      # If onStreamEnd started a new stream, keep pacing
      if client.stream == finished:
        client.stream = nil
        ## debugEcho "[rtmp] Stream fully sent"
      else:
        scheduleSend(client, 0)
    # Audio: trigger next track immediately when done (independent of outLen)
    if client.aac != nil and client.aac.done:
      var a = client.aac
      if a.fd >= 0:
        discard posix.close(a.fd)
        a.fd = -1
      if client.onStreamEnd != nil:
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
        client.onStreamEnd(client, dummy, int(a.pos))
      # client.aac = nil
      scheduleSend(client, 0)

#
# ADTS/AAC Streaming parsing and AAC/FLV packing
#
proc samplingRateFromIndex(idx: int): int =
  let table = [96000,88200,64000,48000,44100,32000,24000,22050,16000,12000,11025,8000,7350]
  if idx >= 0 and idx < table.len: table[idx] else: 44100

proc parseAdts(fd: cint, pos: int64,
               frameLen: var int, headerLen: var int,
               profile: var int, sfIndex: var int, channels: var int): bool =
  # Parse ADTS header at given file position
  var hdr: array[9, uint8]
  if posix.lseek(fd, pos, SEEK_SET) < 0: return false
  let n = posix.read(fd, addr hdr[0], 9)
  if n < 7: return false
  if hdr[0] != 0xFF'u8 or (hdr[1] and 0xF0'u8) != 0xF0'u8: return false
  let protectionAbsent = int(hdr[1] and 0x01'u8)

  # Cast to int before bit operations
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
  # AudioSpecificConfig (2 bytes for LC without SBR/PS)
  var asc = newSeq[byte](2)
  let x = (profile shl 11) or (sfIndex shl 7) or (channels shl 3)
  asc[0] = ((x shr 8) and 0xFF).uint8
  asc[1] = (x and 0xFF).uint8
  asc

proc writeExtendedTimestamp(bev: ptr bufferevent, ts: uint32) =
  ## write 4-byte extended timestamp (big-endian)
  var ex: array[4, uint8]
  ex[0] = ((ts shr 24) and 0xFF).uint8
  ex[1] = ((ts shr 16) and 0xFF).uint8
  ex[2] = ((ts shr 8) and 0xFF).uint8
  ex[3] = (ts and 0xFF).uint8
  discard bufferevent_write(bev, addr ex[0], 4.csize_t)

proc writeRtmpFmt0Header(bev: ptr bufferevent, csid: uint8,
            ts: uint32, msgLen: int, msgType: uint8, msgStreamId: uint32) =
  # First chunk of a message: fmt=0 (12-byte message header)
  var h0: array[12, uint8]
  h0[0] = ((0'u8 shl 6) or csid)                  # basic header: fmt=0, csid in [2..63]
  let ts3 = min(ts, 0xFFFFFF'u32)
  h0[1] = ((ts3 shr 16) and 0xFF).uint8
  h0[2] = ((ts3 shr 8) and 0xFF).uint8
  h0[3] = (ts3 and 0xFF).uint8
  h0[4] = ((msgLen shr 16) and 0xFF).uint8
  h0[5] = ((msgLen shr 8) and 0xFF).uint8
  h0[6] = (msgLen and 0xFF).uint8
  h0[7] = msgType
  # Little-endian msgStreamId
  h0[8]  = (msgStreamId and 0xFF).uint8
  h0[9]  = ((msgStreamId shr 8) and 0xFF).uint8
  h0[10] = ((msgStreamId shr 16) and 0xFF).uint8
  h0[11] = ((msgStreamId shr 24) and 0xFF).uint8
  discard bufferevent_write(bev, addr h0[0], 12.csize_t)
  # Extended timestamp if needed
  if ts > 0xFFFFFF'u32:
    writeExtendedTimestamp(bev, ts)

proc writeRtmpFmt3Header(bev: ptr bufferevent, csid: uint8, ts: uint32) =
  # Subsequent chunks of the same message: fmt=3 (no message header fields)
  var b: uint8 = ((3'u8 shl 6) or csid)
  discard bufferevent_write(bev, addr b, 1.csize_t)
  if ts > 0xFFFFFF'u32:
    writeExtendedTimestamp(bev, ts)

proc pushNextAacFrame(client: RtmpClient): bool =
  # Push next AAC frame from ADTS file to RTMP client
  let a = client.aac
  if a == nil or a.done: return
  let outBuf = bufferevent_get_output(client.bev)
  let outLen = evbuffer_get_length(outBuf).int

  # Backpressure: block only after preroll and after seq header went out
  let blocked = outLen >= a.lowWater
  if blocked and a.seqHeaderSent and a.preRollFrames <= 0:
    return
  
  var frameLen, headerLen, profile, sfIndex, ch: int
  if not parseAdts(a.fd, a.pos, frameLen, headerLen, profile, sfIndex, ch):
    a.done = true
    return

  a.sampleRate = samplingRateFromIndex(sfIndex)
  a.channels = ch

  # Map sampleRate to FLV soundRate code (AAC decoders use ASC, but keep sane values)
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
    writeRtmpFmt0Header(client.bev, a.csid, a.ts, msgLen, 0x08'u8, a.msgStreamId)
    discard bufferevent_write(client.bev, addr soundHeader, 1.csize_t)
    var aacPktType0: uint8 = 0
    discard bufferevent_write(client.bev, addr aacPktType0, 1.csize_t)
    discard bufferevent_write(client.bev, unsafeAddr asc[0], asc.len.csize_t)
    a.seqHeaderSent = true

  let rawLen = frameLen - headerLen
  if rawLen <= 0:
    a.pos += frameLen
    return

  let msgLen = 1 + 1 + rawLen
  writeRtmpFmt0Header(client.bev, a.csid, a.ts, msgLen, 0x08'u8, a.msgStreamId)
  assert bufferevent_write(client.bev, addr soundHeader, 1.csize_t) == 0
  var aacPktType1: uint8 = 1
  assert bufferevent_write(client.bev, addr aacPktType1, 1.csize_t) == 0

  let seg = evbuffer_file_segment_new(a.fd, a.pos + headerLen, rawLen, EVBUF_FS_DISABLE_MMAP)
  if seg == nil:
    a.done = true
    if client.onStreamError != nil:
      var dummy = StreamState(fd: -1, totalSize: 0'i64, offset: 0'i64, msgType: 0x08'u8, csid: a.csid,
                              msgStreamId: a.msgStreamId, ts: a.ts, lowWater: a.lowWater, done: true)
      client.onStreamError(client, dummy, "aac segment failed")
    return
  assert evbuffer_add_file_segment(outBuf, seg, 0, rawLen) == 0
  evbuffer_file_segment_free(seg)

  # Consume preroll allowance once we actually queued a frame
  if a.preRollFrames > 0: dec a.preRollFrames

  # Precise ms step for 1024 samples/frame
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
  # Start streaming AAC from ADTS file with zero-copy
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
  
  # Keep bev watermark as-is (set by video). Audio uses its own gating now.
  if client.onStreamStart != nil:
    client.onStreamStart(client, nil, 0)
  
  discard pushNextAacFrame(client)
  if client.sendTimer != nil: scheduleSend(client, 0)

proc tuneSocket(client: RtmpClient) =
  # Disable Nagle and enlarge socket buffers
  let fd = bufferevent_getfd(client.bev)
  if fd >= 0:
    var yes: cint = 1
    discard posix.setsockopt(fd.SocketHandle, IPPROTO_TCP, TCP_NODELAY, cast[pointer](addr yes), sizeof(cint).cuint)
    var snd: cint = 8 * 1024 * 1024
    discard posix.setsockopt(fd.SocketHandle, SOL_SOCKET, SO_SNDBUF, cast[pointer](addr snd), sizeof(cint).cuint)
    var rcv: cint = 8 * 1024 * 1024
    discard posix.setsockopt(fd.SocketHandle, SOL_SOCKET, SO_RCVBUF, cast[pointer](addr rcv), sizeof(cint).cuint)

proc onEvent*(bev: ptr bufferevent, what: cshort, ctx: pointer) {.cdecl.} =
  ## Libevent event callback for connection events
  let client = cast[RtmpClient](ctx)
  if (what and BEV_EVENT_CONNECTED.cshort) != 0:
    ## debugEcho "[rtmp] Connected to ", client.host, ":", client.port
    tuneSocket(client) # disable Nagle, enlarge buffers
    client.handshakeState = hsSendC0C1
    client.handshakeBuf = @[]
    randomize()
    client.sendC0C1() # Start handshake
  elif (what and BEV_EVENT_ERROR.cshort) != 0:
    ## debugEcho "[rtmp] Connection error: errno=", posix.errno, " ", posix.strerror(posix.errno)
    if client.sendTimer != nil:
      discard event_del(client.sendTimer)
      event_free(client.sendTimer)
      client.sendTimer = nil
  elif (what and BEV_EVENT_EOF.cshort) != 0:
    ## debugEcho "[rtmp] Connection closed by peer"
    if client.sendTimer != nil:
      discard event_del(client.sendTimer)
      event_free(client.sendTimer)
      client.sendTimer = nil

#
# RTMP FLV
#
type
  FlvTagHeader = object
    tagType*: uint8
      ## 8=audio, 9=video, 18=script/data
    dataSize*: int
      ## size of tag payload
    timestamp*: uint32
      ## timestamp of tag
    posPayload*: int64
      ## file position of tag payload

proc readFlvHeader(fd: cint): int64 =
  # Read FLV header and return position of first tag, or -1 on error
  var hdr: array[9, uint8]
  if posix.lseek(fd, 0, SEEK_SET) < 0: return -1
  let n = posix.read(fd, addr hdr[0], 9)
  if n != 9: return -1
  if hdr[0] != 'F'.uint8 or hdr[1] != 'L'.uint8 or hdr[2] != 'V'.uint8: return -1
  let headerSize = (int(hdr[5]) shl 24) or (int(hdr[6]) shl 16) or (int(hdr[7]) shl 8) or int(hdr[8])
  # Skip PreviousTagSize0 (4 bytes)
  int64(headerSize + 4)

proc readFlvTagHeader(fd: cint, pos: int64, th: var FlvTagHeader): bool =
  # Read FLV tag header at given file position
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
  # Map FLV tagType to RTMP msgType and csid; keep csid in 2..63.
  case tagType
  of 0x08'u8: (0x08'u8, 4'u8)   # audio -> csid 4
  of 0x09'u8: (0x09'u8, 6'u8)   # video -> csid 6
  of 0x12'u8: (0x12'u8, 5'u8)   # script/meta -> csid 5
  else: (0x12'u8, 5'u8)

proc peekNextFlvTagTs(fd: cint, pos: int64, nextTs: var uint32): bool =
  # Peek timestamp of next FLV tag without advancing file position
  var h: array[11, uint8]
  if posix.lseek(fd, pos, SEEK_SET) < 0: return false
  let n = posix.read(fd, addr h[0], 11)
  if n != 11: return false
  let ts = (uint32(h[4]) shl 16) or (uint32(h[5]) shl 8) or uint32(h[6]) or (uint32(h[7]) shl 24)
  nextTs = ts
  true

proc pushNextFlvTag(client: RtmpClient): bool =
  # Push next FLV tag from file to RTMP client
  let st = client.stream
  if st == nil or st.done: return

  let outBuf = bufferevent_get_output(client.bev)
  if evbuffer_get_length(outBuf) >= st.lowWater.csize_t: return

  if not st.tagInProgress:
    # Read next non-audio tag if AAC is active, otherwise normal.
    while true:
      var th: FlvTagHeader
      if not readFlvTagHeader(st.fd, st.offset, th):
        st.done = true
        return
      # If AAC ADTS is active, skip FLV audio tags to avoid double audio
      if th.tagType == 0x08'u8 and client.aac != nil and not client.aac.done:
        st.offset = th.posPayload + th.dataSize.int64 + 4 # skip payload + PreviousTagSize
        if st.offset >= st.totalSize:
          st.done = true
          return
        continue
      # Use this tag
      let (msgType, csid) = flvTagToRtmp(th.tagType)
      st.msgType = msgType
      st.csid = csid
      st.tagPayloadPos = th.posPayload
      st.tagRemaining = th.dataSize
      st.tagTsAbs = uint32(th.timestamp) + st.tsOffset
      writeRtmpFmt0Header(client.bev, csid, st.tagTsAbs, st.tagRemaining, msgType, st.msgStreamId)
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
    writeRtmpFmt3Header(client.bev, st.csid, st.tagTsAbs)

  let seg = evbuffer_file_segment_new(st.fd, st.tagPayloadPos, toSend, EVBUF_FS_DISABLE_MMAP)
  if seg == nil:
    st.done = true
    return
  discard evbuffer_add_file_segment(outBuf, seg, 0, toSend)
  evbuffer_file_segment_free(seg)

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
  # Start streaming FLV from file with zero-copy
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
  bufferevent_setwatermark(client.bev, EV_WRITE, lowWater.csize_t, 0)
  if client.onStreamStart != nil:
    client.onStreamStart(client, client.stream, 0)

  # Prime one tag and ensure pacer is (re)scheduled
  discard pushNextFlvTag(client)
  scheduleSend(client, 0)

proc newRtmpClient*(address: string): RtmpClient =
  ## Create new RTMP client and initiate connection to address.
  ## Address should be in form "rtmp://host[:port]/app/streamKey". Port is optional (default 1935 for rtmp, 443 for rtmps).
  let base = event_base_new()
  let bev = bufferevent_socket_new(base, -1, BEV_OPT_CLOSE_ON_FREE)
  let uri = parseUri(address)
  assert uri.scheme == "rtmp" or uri.scheme == "rtmps"
  let port =
    if uri.port.len > 0: parseInt(uri.port)
    else:
      if uri.scheme == "rtmp": 1935 else: 443
  new(result)
  let path = uri.path.split("/")
  result.base = base
  result.host = uri.hostname
  result.port = port
  
  let appPart = if path.len > 1: path[1] else: ""
  let streamPart = if path.len > 2: path[2] else: ""

  result.app = appPart
  result.tcUrl = uri.scheme & "://" & uri.hostname & "/" & appPart
  result.streamName = streamPart # stream name or stream key

  result.handshakeState = hsSendC0C1
  result.stage = stInit
  result.bev = bev
  result.msgStreamId = 1
  result.inChunks = initTable[uint32, ChunkStreamState]()  # init inbound state

  bufferevent_setcb(bev, onRead, onWrite, onEvent, cast[pointer](result))
  discard bufferevent_enable(bev, EV_READ or EV_WRITE)
  # if hostname is an IP address, connect directly
  if result.host.isIPv4:
    var sock: SockAddr_in
    sock.sin_family = uint8(AF_INET)
    sock.sin_port = htons(uint16(port))
    sock.sin_addr.s_addr = inet_addr(uri.hostname)
    discard bufferevent_socket_connect(bev, sock.unsafeAddr, sizeof(sock).cint)
  else:
    # resolve hostname
    if bufferevent_socket_connect_hostname(bev, result.base, AF_UNSPEC, uri.hostname.cstring, port.cint) != 0:
      ## debugEcho "[rtmp] Failed to start hostname resolution"

proc nowMs(): int64 =
  ## High-resolution monotonic ms to drive pacing (macOS/Linux)
  when declared(posix.clock_gettime):
    var ts: posix.Timespec
    discard posix.clock_gettime(posix.CLOCK_MONOTONIC, ts)
    ts.tv_sec.int64 * 1000 + ts.tv_nsec.int64 div 1_000_000
  else:
    var tv: posix.Timeval
    discard posix.gettimeofday(addr tv, nil)
    tv.tv_sec.int64 * 1000 + tv.tv_usec.int64 div 1000

proc computeWallTsLimit(client: RtmpClient): uint32 =
  # compute the maximum stream timestamp (globalTs)
  # we should have sent given wall clock + lead
  let now = nowMs()
  let deltaMs = max(0'i64, now - client.wallOriginMs) + client.sendLeadMs.int64
  uint32(deltaMs)

proc sendUntilLimit(client: RtmpClient, limitTs: uint32) =
  # Send FLV tags and AAC frames until limitTs is reached or backpressure occurs
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

    # Stronger audio priority (allow small lead over video)
    let chooseVideo =
      client.stream != nil and not client.stream.done and
      (client.aac == nil or client.aac.done or nextVideoTs + 5'u32 < nextAudioTs)

    if chooseVideo:
      if nextVideoTs <= limitTs: pushed = pushNextFlvTag(client)
    elif client.aac != nil and not client.aac.done:
      if nextAudioTs <= limitTs: pushed = pushNextAacFrame(client)

    if not pushed: break

proc scheduleSend(client: RtmpClient, delayMs: int) =
  # Schedule sendTimer to fire after delayMs (clamped)
  var d = delayMs
  if d < 5: d = 5            # avoid 0–1ms busy loops
  if d > 1000: d = 1000
  ## debugEcho "[rtmp] scheduleSend delay=", d
  var tv: event.Timeval
  tv.tv_sec = (d div 1000).cint
  tv.tv_usec = ((d mod 1000) * 1000).cint
  if client.sendTimer == nil:
    client.sendTimer = event_new(client.base, -1, 0, cast[EventCallbackFn](sendTimerCb), cast[pointer](client))
  assert event_add(client.sendTimer, addr tv) == 0

proc sendTimerCb(fd: cint, what: cshort, arg: pointer) {.cdecl.} =
  # Libevent timer callback for pacing
  let client = cast[RtmpClient](arg)
  ## debugEcho "[rtmp] sendTimerCb fired"
  client.lastSendMs = nowMs()
  let limitTs = computeWallTsLimit(client)
  ## debugEcho "[rtmp] sendTimer limitTs=", limitTs, " globalTs=", client.ps.globalTs
  sendUntilLimit(client, limitTs)

  var nextTs: int64 = -1
  if client.stream != nil and not client.stream.done:
    var peekTs: uint32
    if peekNextFlvTagTs(client.stream.fd, client.stream.offset, peekTs):
      nextTs = (peekTs + client.stream.tsOffset).int64
  if client.aac != nil and not client.aac.done and (nextTs < 0 or client.aac.ts.int64 < nextTs):
    nextTs = client.aac.ts.int64

  if nextTs >= 0:
    let now = nowMs()
    let targetMs = client.wallOriginMs + nextTs - client.sendLeadMs.int64
    scheduleSend(client, int(targetMs - now))
  else:
    # Nothing queued. If output drained, notify completion.
    let outLen = evbuffer_get_length(bufferevent_get_output(client.bev)).int
    if (client.stream == nil or client.stream.done) and (client.aac == nil or client.aac.done) and outLen == 0:
      if client.stream != nil:
        var finished = client.stream
        closeStreamFd(finished)
        if client.onStreamEnd != nil:
          client.onStreamEnd(client, finished, int(finished.offset))
        if client.stream == finished:
          client.stream = nil
          ## debugEcho "[rtmp] Stream fully sent"
      if client.stream != nil and not client.stream.done:
        scheduleSend(client, 0)
      else:
        if client.sendTimer != nil:
          discard event_del(client.sendTimer)
          event_free(client.sendTimer)
          client.sendTimer = nil
          ## debugEcho "[rtmp] Pacer stopped"

proc startPacer*(client: RtmpClient, initCb: proc(c: RtmpClient) = nil) =
  ## Start pacing loop if not already active, and call initCb for any one-time initialization (e.g. start first stream).
  if client.sendTimer == nil:
    client.wallOriginMs = nowMs() - int64(client.ps.globalTs) # map current globalTs to now
    client.lastSendMs = nowMs()
    if initCb != nil:
      initCb(client)
    scheduleSend(client, 0)
