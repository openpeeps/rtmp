# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

## This module implements RTMP server functionality, including connection handling,
## RTMP message parsing, and a simple pub/sub mechanism for streams.
##
## It uses powpow for asynchronous network I/O and supports basic RTMP commands and
## control messages. The server can be extended to handle incoming streams as needed.

import std/[times, tables, httpcore, json, jsonutils]

import powpow
import powpow/proto/httpserver

import ./server/[actionmessage, chunkstream, rtmpmonitor, handshake]
import ./private/memutils

from std/net import Port, `$`

const
  DEFAULT_RTMP_PORT* = 1935
  RTMP_HANDSHAKE_SIZE* = 1536
  MAX_SUBSCRIBER_OUTBUF* = 3 * (1024 * 1024)
  SLOW_SUBSCRIBER_RESUME_OUTBUF = MAX_SUBSCRIBER_OUTBUF div 2

const
  HS_INIT* = 0
  HS_S0S1_SENT* = 1
  HS_DONE* = 2
  RTMP_DEFAULT_CHUNK_SIZE* = 128
  RTMP_MAX_CHUNK_SIZE* = 65536

proc epochMs(): int64 {.inline.} =
  int64(times.epochTime() * 1000.0)

proc buildServerS1(): seq[byte] =
  var s = newSeq[byte](RTMP_HANDSHAKE_SIZE)
  let ts = int(times.getTime().toUnix())
  let ts32 = int32(ts)
  cast[ptr int32](addr s[0])[] = ts32
  cast[ptr int32](addr s[4])[] = 0'i32
  for i in 8 ..< RTMP_HANDSHAKE_SIZE:
    s[i] = byte((ts + i) and 0xFF)
  s

type
  RtmpConnState* = object
    peerChunkSize*: int
    localChunkSize*: int
    windowAckSize*: uint32
    bytesReceivedSinceAck*: uint64
    streams*: Table[int, pointer]

  RtmpServerSettings* = object
    enableRestApi*: bool = true
    restApiPort*: Port = Port(4000)
    rtmpPort*: Port = Port(DEFAULT_RTMP_PORT)

  RtmpServer* = ref object
    loop*: Loop
    httpServer*: HttpServer
    settings*: RtmpServerSettings

  RTMPServerError* = object of CatchableError

  ConnCtx* = ref object
    conn*: Connection
    state*: RtmpConnState
    hsState*: int
    partialHdr*: seq[byte]
    partialMsg*: seq[byte]
    expectedMsgLen*: int
    msgTypeId*: int
    msgStreamId*: int
    chunkCtx*: ChunkStreamCtx
    serverS1*: seq[byte]
    nextStreamId*: int
    connId*: int
    clientIp*: string
    closed*: bool
    closeReason*: string
    publishedStreamName*: string
    publishedStreamId*: int
    subscriptions*: Table[string, int]
    streamNameById*: Table[int, string]
    slowSubscriber*: bool
    pausedSubscriber*: bool
    waitForKeyframe*: bool
    lastLiveJumpMs*: int64
    recoveringUntilMs*: int64

proc sendAmfCommand(conn: ConnCtx; msgStreamId: int; vals: seq[AMF0Value])
proc sendRtmpMessage(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; payload: seq[byte]; timestamp: int = 0): bool {.discardable.}
proc removeSubscriber(name: string, conn: ConnCtx)
proc connSummary(conn: ConnCtx): string

var
  gNextConnId = 1

#
# Shared payload management for zero-copy streaming (simplified)
#
type
  SharedPayload = ref object
    bytes: seq[byte]

proc newSharedPayload(payloadPtr: pointer, payloadLen: int): SharedPayload =
  result = SharedPayload(bytes: @[])
  if payloadLen <= 0 or payloadPtr == nil: return
  result.bytes = newSeq[byte](payloadLen)
  copyMem(addr result.bytes[0], payloadPtr, payloadLen)

proc monitorAddStream*(name: string, pubConn: ConnCtx) =
  var pub = RtmpPublisher(id: $pubConn.connId, ip: pubConn.clientIp, published_at: toUnix(toTime(now())))
  let stream = RtmpStream(
    id: name, publisher: pub, created_at: toUnix(toTime(now()))
  )
  gMonitor.streams[name] = stream

proc monitorRemoveStream*(name: string) =
  gMonitor.streams.del(name)

proc monitorAddSubscriber*(streamName: string, subConn: ConnCtx) =
  if gMonitor.streams.hasKey(streamName):
    let sub = RtmpSubscriber(id: $subConn.connId, ip: subConn.clientIp, subscribed_at: toUnix(toTime(now())))
    gMonitor.streams[streamName].subscribers.add(sub)

proc monitorRemoveSubscriber*(streamName: string, subConn: ConnCtx) =
  if gMonitor.streams.hasKey(streamName):
    var stream = gMonitor.streams[streamName]
    var subPos: int = -1
    for i, s in stream.subscribers:
      if s.id == $subConn.connId:
        subPos = i; break
    if subPos >= 0: gMonitor.streams[streamName].subscribers.del(subPos)

#
# Stream pub/sub registry
#
type
  SubscriberEntry* = object
    conn*: ConnCtx
    msgStreamId*: int

  StreamEntry* = ref object
    name*: string
    publisher*: ConnCtx
    publisherStreamId*: int
    subscribers*: seq[SubscriberEntry]
    metaPayload*: seq[byte]
    videoSeqPayload*: seq[byte]
    audioSeqPayload*: seq[byte]

let
  kAmfNull = newNull()
  kAmfNum0 = newNumber(0.0)
  kAmfCmdOnStatus = newString("onStatus")
  kAmfCmdResult = newString("_result")

  kInfoConnectSuccess = amfObj({
    "level": newString("status"),
    "code": newString("NetConnection.Connect.Success"),
    "description": newString("Connection succeeded."),
    "objectEncoding": kAmfNum0
  })

  kPropsConnect = amfObj({
    "fmsVer": newString("FMS/3,5,7,7009"),
    "capabilities": newNumber(31.0)
  })

  kInfoPlayStop = amfObj({
    "level": newString("status"),
    "code": newString("NetStream.Play.Stop"),
    "description": newString("Stream ended")
  })

var gStreams = initTable[string, StreamEntry]()

proc drainOutputQueue(conn: ConnCtx) =
  if conn == nil or conn.conn == nil: return
  discard conn.conn.flushWriteBuffer()

proc addPublisher(name: string, conn: ConnCtx, pubStreamId: int) =
  if name.len == 0 or conn == nil: return
  var se = gStreams.getOrDefault(name, nil)
  if se == nil:
    se = StreamEntry(name: name, publisher: conn, publisherStreamId: pubStreamId)
    gStreams[name] = se
  else:
    se.publisher = conn
    se.publisherStreamId = pubStreamId
  conn.publishedStreamName = name
  conn.publishedStreamId = pubStreamId
  monitorAddStream(name, conn)
  se.metaPayload.setLen(0)
  se.videoSeqPayload.setLen(0)
  se.audioSeqPayload.setLen(0)

proc addSubscriber(name: string, conn: ConnCtx, subStreamId: int, skipInitSend: bool = false) =
  if name.len == 0 or conn == nil: return
  if conn.subscriptions.hasKey(name) and conn.subscriptions[name] == subStreamId:
    return
  var se = gStreams.getOrDefault(name, nil)
  if se == nil:
    se = StreamEntry(name: name)
    gStreams[name] = se
  for s in se.subscribers:
    if s.conn == conn and s.msgStreamId == subStreamId:
      conn.subscriptions[name] = subStreamId
      conn.streamNameById[subStreamId] = name
      return
  se.subscribers.add(SubscriberEntry(conn: conn, msgStreamId: subStreamId))
  conn.subscriptions[name] = subStreamId
  conn.streamNameById[subStreamId] = name
  if not skipInitSend:
    if se.metaPayload.len > 0:
      if not sendRtmpMessage(conn, csid = 4, msgTypeId = 18, msgStreamId = subStreamId, payload = se.metaPayload):
        removeSubscriber(name, conn)
        return
    if se.videoSeqPayload.len > 0:
      if not sendRtmpMessage(conn, csid = 4, msgTypeId = 9, msgStreamId = subStreamId, payload = se.videoSeqPayload):
        removeSubscriber(name, conn)
        return
    if se.audioSeqPayload.len > 0:
      if not sendRtmpMessage(conn, csid = 4, msgTypeId = 8, msgStreamId = subStreamId, payload = se.audioSeqPayload):
        removeSubscriber(name, conn)
        return
  monitorAddSubscriber(name, conn)

proc removeSubscriber(name: string, conn: ConnCtx) =
  if name.len == 0 or conn == nil: return
  let se = gStreams.getOrDefault(name, nil)
  if se == nil: return
  let sid = conn.subscriptions.getOrDefault(name, 0)
  var o: seq[SubscriberEntry] = @[]
  for s in se.subscribers:
    if s.conn != conn:
      o.add(s)
  se.subscribers = o
  if conn.subscriptions.hasKey(name):
    conn.subscriptions.del(name)
  if sid != 0 and conn.streamNameById.hasKey(sid):
    if conn.streamNameById[sid] == name:
      conn.streamNameById.del(sid)
  if se.publisher == nil and se.subscribers.len == 0 and gStreams.hasKey(name):
    gStreams.del(name)
  monitorRemoveSubscriber(name, conn)
  releaseUnusedMemory()

proc pauseSubscriber(name: string, conn: ConnCtx) =
  if name.len == 0 or conn == nil: return
  let se = gStreams.getOrDefault(name, nil)
  if se == nil: return
  var kept: seq[SubscriberEntry] = @[]
  for s in se.subscribers:
    if s.conn != conn:
      kept.add(s)
  se.subscribers = kept
  conn.slowSubscriber = true
  conn.pausedSubscriber = true
  if conn.subscriptions.hasKey(name):
    conn.subscriptions.del(name)
  if se.publisher == nil and se.subscribers.len == 0 and gStreams.hasKey(name):
    gStreams.del(name)
  monitorRemoveSubscriber(name, conn)

proc removePublisher(name: string) =
  if name.len == 0: return
  if gStreams.hasKey(name):
    let se = gStreams[name]
    for s in se.subscribers:
      if s.conn != nil:
        sendAmfCommand(s.conn, s.msgStreamId, @[ kAmfCmdOnStatus, kAmfNum0, kAmfNull, kInfoPlayStop ])
    monitorRemoveStream(name)
    gStreams.del(name)
  releaseUnusedMemory()

proc cleanupConn(conn: ConnCtx) =
  if conn == nil: return
  if conn.closed: return
  conn.closed = true
  if conn.closeReason.len == 0:
    conn.closeReason = "cleanup"
  if conn.publishedStreamName.len > 0:
    removePublisher(conn.publishedStreamName)
  var subNames: seq[string] = @[]
  for name, _ in conn.subscriptions.pairs:
    subNames.add(name)
  for name in subNames:
    removeSubscriber(name, conn)
  conn.subscriptions.clear()

proc removeSubscriptionsByStreamId(conn: ConnCtx; streamId: int) =
  if conn == nil: return
  var toRemove: seq[string] = @[]
  for name, sid in conn.subscriptions.pairs:
    if sid == streamId:
      toRemove.add(name)
  for name in toRemove:
    removeSubscriber(name, conn)
    if conn.subscriptions.hasKey(name):
      conn.subscriptions.del(name)
  if conn.streamNameById.hasKey(streamId):
    conn.streamNameById.del(streamId)

proc connSummary(conn: ConnCtx): string =
  if conn == nil: return "conn=nil"
  result = "connId=" & $conn.connId &
    " published=\"" & conn.publishedStreamName & "\"" &
    " pubStreamId=" & $conn.publishedStreamId &
    " subs=" & $conn.subscriptions.len

#
# RTMP output helpers
#
proc put3BE(outp: var seq[byte], v: int) =
  outp.add(byte((v shr 16) and 0xFF))
  outp.add(byte((v shr 8) and 0xFF))
  outp.add(byte(v and 0xFF))

proc put4BE(outp: var seq[byte], v: uint32) =
  outp.add(byte((v shr 24) and 0xFF))
  outp.add(byte((v shr 16) and 0xFF))
  outp.add(byte((v shr 8) and 0xFF))
  outp.add(byte(v and 0xFF))

proc put4LE(outp: var seq[byte], v: int) =
  outp.add(byte(v and 0xFF))
  outp.add(byte((v shr 8) and 0xFF))
  outp.add(byte((v shr 16) and 0xFF))
  outp.add(byte((v shr 24) and 0xFF))

proc sendRtmpMessage(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; payload: seq[byte]; timestamp: int = 0): bool =
  if conn == nil or conn.conn == nil: return false
  if csid <= 1 or csid >= 64:
    raise newException(RTMPServerError, "Only CSID 2..63 supported in this minimal sender")

  let chunkSize = max(conn.state.localChunkSize, 1)
  var buf: seq[byte] = @[]

  # fmt=0 basic header
  buf.add(byte((0 shl 6) or (csid and 0x3F)))

  # message header (11 bytes)
  put3BE(buf, timestamp)
  put3BE(buf, payload.len)
  buf.add(byte(msgTypeId and 0xFF))
  put4LE(buf, msgStreamId)

  # payload split into chunks
  var off = 0
  while off < payload.len:
    let take = min(chunkSize, payload.len - off)
    if take > 0:
      buf.add(payload[off ..< off + take])
      off += take
    if off < payload.len:
      buf.add(byte((3 shl 6) or (csid and 0x3F)))

  let rc = conn.conn.send(buf)
  return rc > 0

proc sendRtmpMessageShared(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; sp: SharedPayload; timestamp: int = 0): bool =
  if conn == nil or conn.conn == nil or sp == nil: return false
  let payloadLen = sp.bytes.len
  if csid <= 1 or csid >= 64:
    raise newException(RTMPServerError, "Only CSID 2..63 supported in this minimal sender")
  if payloadLen < 0: return false

  let chunkSize = max(conn.state.localChunkSize, 1)
  let ts = max(0, min(timestamp, 0xFFFFFF))

  var buf: seq[byte] = @[]

  # fmt=0 basic header + message header (12 bytes)
  buf.add(byte((0 shl 6) or (csid and 0x3F)))
  put3BE(buf, ts)
  put3BE(buf, payloadLen)
  buf.add(byte(msgTypeId and 0xFF))
  put4LE(buf, msgStreamId)

  # payload split into chunks with continuation headers
  var off = 0
  while off < payloadLen:
    let take = min(chunkSize, payloadLen - off)
    if take > 0:
      buf.add(sp.bytes[off ..< off + take])
      off += take
    if off < payloadLen:
      buf.add(byte((3 shl 6) or (csid and 0x3F)))

  let rc = conn.conn.send(buf)
  return rc > 0

proc sendSetChunkSize(conn: ConnCtx; size: int) =
  var p: seq[byte] = @[]
  put4BE(p, uint32(size))
  sendRtmpMessage(conn, csid = 2, msgTypeId = 1, msgStreamId = 0, payload = p)

proc sendWindowAckSize(conn: ConnCtx; size: uint32) =
  var p: seq[byte] = @[]
  put4BE(p, size)
  sendRtmpMessage(conn, csid = 2, msgTypeId = 5, msgStreamId = 0, payload = p)

proc sendSetPeerBandwidth(conn: ConnCtx; size: uint32; limitType: byte = 2) =
  var p: seq[byte] = @[]
  put4BE(p, size)
  p.add(limitType)
  sendRtmpMessage(conn, csid = 2, msgTypeId = 6, msgStreamId = 0, payload = p)

proc sendAcknowledgement(conn: ConnCtx; seq: uint32) =
  var p: seq[byte] = @[]
  put4BE(p, seq)
  sendRtmpMessage(conn, csid = 2, msgTypeId = 3, msgStreamId = 0, payload = p)

proc sendUserControlStreamBegin(conn: ConnCtx; streamId: int) =
  var p: seq[byte] = @[]
  p.add(byte(0)); p.add(byte(0))
  put4BE(p, uint32(streamId))
  sendRtmpMessage(conn, csid = 2, msgTypeId = 4, msgStreamId = 0, payload = p)

proc amfObj(pairs: openArray[(string, AMF0Value)]): AMF0Value =
  result = newObject()
  for (k, v) in pairs:
    result.obj[k] = v

proc sendAmfCommand(conn: ConnCtx; msgStreamId: int; vals: seq[AMF0Value]) =
  let payload = encodeAMF0Values(vals)
  sendRtmpMessage(conn, csid = 3, msgTypeId = 20, msgStreamId = msgStreamId, payload = payload)

proc getTxnId(vals: seq[AMF0Value]): float64 =
  if vals.len >= 2 and vals[1] != nil and vals[1].typ == AMF0_Number:
    return vals[1].num
  result = 1.0

proc onChunkMessage(msgTypeId: int, msgStreamId: int, timestamp: uint32,
          payloadPtr: ptr byte, payloadLen: int, arg: pointer) =
  let c = cast[ConnCtx](arg)
  if c == nil: return

  if msgTypeId == 1 and payloadLen >= 4 and payloadPtr != nil:
    let b = cast[ptr UncheckedArray[byte]](payloadPtr)
    let newSize = (int(b[0]) shl 24) or (int(b[1]) shl 16) or (int(b[2]) shl 8) or int(b[3])
    if newSize > 0 and newSize <= RTMP_MAX_CHUNK_SIZE:
      c.state.peerChunkSize = newSize
      setPeerChunkSize(c.chunkCtx, newSize)
    return

  if msgTypeId == 5 and payloadLen >= 4 and payloadPtr != nil:
    let b = cast[ptr UncheckedArray[byte]](payloadPtr)
    let win = (uint32(b[0]) shl 24) or (uint32(b[1]) shl 16) or (uint32(b[2]) shl 8) or uint32(b[3])
    c.state.windowAckSize = win
    c.state.bytesReceivedSinceAck = 0'u64
    return

  if msgTypeId == 6 and payloadLen >= 5 and payloadPtr != nil:
    return

  if msgTypeId == 3 and payloadLen >= 4 and payloadPtr != nil:
    return

  if msgTypeId == 2 and payloadLen >= 4 and payloadPtr != nil:
    let b = cast[ptr UncheckedArray[byte]](payloadPtr)
    let abortCsid = (int(b[0]) shl 24) or (int(b[1]) shl 16) or (int(b[2]) shl 8) or int(b[3])
    if c.chunkCtx != nil:
      c.chunkCtx.streams.del(abortCsid)
    return

  if msgTypeId == 4 and payloadLen >= 2 and payloadPtr != nil:
    return

  if (msgTypeId == 20 or msgTypeId == 18 or msgTypeId == 17 or msgTypeId == 15) and payloadLen > 0 and payloadPtr != nil:
    var amfPtr = payloadPtr
    var amfLen = payloadLen
    let isDataMsg = (msgTypeId == 18 or msgTypeId == 15)
    if msgTypeId == 17 or msgTypeId == 15:
      let b = cast[ptr UncheckedArray[byte]](payloadPtr)
      if payloadLen >= 1 and b[0] == 0'u8:
        amfPtr = cast[ptr byte](addr b[1])
        amfLen = payloadLen - 1
      else:
        return
    var vals: seq[AMF0Value]
    try:
      vals = decodeAllAMF0Ptr(amfPtr, amfLen)
    except:
      discard

    # Fallback: if AMF0 decoding failed and this looks like AMF3, try AMF3 decoder
    if (vals.len == 0 or vals[0] == nil or
        (vals[0].typ != AMF0_String and vals[0].typ != AMF0_LongString)):
      if msgTypeId == 17 or msgTypeId == 15:
        # AMF3 message types — try AMF3 decoding
        var amf3data = newSeq[byte](amfLen)
        copyMem(addr amf3data[0], amfPtr, amfLen)
        try:
          vals = decodeAllAMF3(amf3data)
        except:
          discard

    if vals.len == 0 or vals[0] == nil or (vals[0].typ != AMF0_String and vals[0].typ != AMF0_LongString):
      if isDataMsg: discard
      else: return
    let cmd = vals[0].s
    let txn = getTxnId(vals)

    if cmd == "pause":
      var wantsPause = true
      if vals.len >= 4 and vals[3] != nil and vals[3].typ == AMF0_Boolean:
        wantsPause = vals[3].b
      let sid = msgStreamId
      var streamName = c.streamNameById.getOrDefault(sid, "")
      if streamName.len == 0:
        for n, sId in c.subscriptions.pairs:
          if sId == sid:
            streamName = n
            break
      let isSubscribedNow = c.subscriptions.hasKey(streamName) and c.subscriptions[streamName] == sid
      if wantsPause:
        if isSubscribedNow:
          pauseSubscriber(streamName, c)
          c.pausedSubscriber = true
          drainOutputQueue(c)
          let info = amfObj({
            "level": newString("status"),
            "code": newString("NetStream.Pause.Notify"),
            "description": newString("Paused.")
          })
          sendAmfCommand(c, sid, @[ newString("onStatus"), kAmfNum0, kAmfNull, info ])
      else:
        drainOutputQueue(c)
        let se = gStreams.getOrDefault(streamName, nil)
        if se != nil and se.publisher != nil:
          removeSubscriber(streamName, c)
          addSubscriber(streamName, c, sid, false)
          sendUserControlStreamBegin(c, sid)
          let resetInfo = amfObj({
            "level": newString("status"),
            "code": newString("NetStream.Play.Reset"),
            "description": newString("Resetting play state.")
          })
          sendAmfCommand(c, sid, @[ newString("onStatus"), kAmfNum0, kAmfNull, resetInfo ])
          let startInfo = amfObj({
            "level": newString("status"),
            "code": newString("NetStream.Play.Start"),
            "description": newString("Started playing."),
            "details": newString(streamName)
          })
          sendAmfCommand(c, sid, @[ newString("onStatus"), kAmfNum0, kAmfNull, startInfo ])
      return

    if cmd == "connect":
      sendWindowAckSize(c, 5_000_000'u32)
      sendSetPeerBandwidth(c, 5_000_000'u32, 2)
      sendSetChunkSize(c, 4096)
      c.state.localChunkSize = 4096
      let props = amfObj({
        "fmsVer": newString("FMS/3,5,7,7009"),
        "capabilities": newNumber(31.0)
      })
      sendAmfCommand(c, 0, @[ kAmfCmdResult, newNumber(txn), kPropsConnect, kInfoConnectSuccess ])
      return

    if cmd == "releaseStream" or cmd == "FCPublish":
      sendAmfCommand(c, 0, @[ kAmfCmdResult, newNumber(txn), kAmfNull, kAmfNull ])
      return

    if cmd == "createStream":
      let sid = c.nextStreamId
      c.nextStreamId.inc
      sendAmfCommand(c, 0, @[ kAmfCmdResult, newNumber(txn), kAmfNull, newNumber(float64(sid)) ])
      return

    if cmd == "publish":
      var streamName = ""
      for i in 1 ..< vals.len:
        if vals[i] != nil and vals[i].typ == AMF0_String:
          streamName = vals[i].s
          break
      let streamId = msgStreamId
      sendUserControlStreamBegin(c, streamId)
      addPublisher(streamName, c, streamId)
      let info = amfObj({
        "level": newString("status"),
        "code": newString("NetStream.Publish.Start"),
        "description": newString("Start publishing."),
        "details": newString(streamName)
      })
      sendAmfCommand(c, streamId, @[ newString("onStatus"), kAmfNum0, kAmfNull, info ])
      return

    if cmd == "play" or cmd == "play2":
      var streamName = ""
      for i in 1 ..< vals.len:
        if vals[i] != nil and vals[i].typ == AMF0_String:
          streamName = vals[i].s
          break
      let sid = msgStreamId
      let se = gStreams.getOrDefault(streamName, nil)
      if se == nil or se.publisher == nil:
        let nf = amfObj({
          "level": newString("error"),
          "code": newString("NetStream.Play.StreamNotFound"),
          "description": newString("Stream not found")
        })
        sendAmfCommand(c, sid, @[ newString("onStatus"), kAmfNum0, kAmfNull, nf ])
        return
      sendUserControlStreamBegin(c, sid)
      addSubscriber(streamName, c, sid)
      let resetInfo = amfObj({
        "level": newString("status"),
        "code": newString("NetStream.Play.Reset"),
        "description": newString("Resetting play state.")
      })
      sendAmfCommand(c, sid, @[ newString("onStatus"), kAmfNum0, kAmfNull, resetInfo ])
      let startInfo = amfObj({
        "level": newString("status"),
        "code": newString("NetStream.Play.Start"),
        "description": newString("Started playing."),
        "details": newString(streamName)
      })
      sendAmfCommand(c, sid, @[ newString("onStatus"), kAmfNum0, kAmfNull, startInfo ])
      return

    if cmd == "getStreamLength":
      sendAmfCommand(c, 0, @[ kAmfCmdResult, newNumber(txn), kAmfNull, kAmfNum0 ])
      return

    if cmd == "closeStream":
      sendAmfCommand(c, msgStreamId, @[ kAmfCmdResult, newNumber(txn), kAmfNull ])
      removeSubscriptionsByStreamId(c, msgStreamId)
      if c.publishedStreamId == msgStreamId and c.publishedStreamName.len > 0:
        removePublisher(c.publishedStreamName)
        c.publishedStreamName = ""
        c.publishedStreamId = 0
      return

    if cmd == "deleteStream":
      sendAmfCommand(c, msgStreamId, @[ kAmfCmdResult, newNumber(txn), kAmfNull ])
      removeSubscriptionsByStreamId(c, msgStreamId)
      if c.publishedStreamId == msgStreamId and c.publishedStreamName.len > 0:
        removePublisher(c.publishedStreamName)
        reset(c.publishedStreamName)
        reset(c.publishedStreamId)
      return

    if cmd == "FCUnpublish" or cmd == "unpublish":
      if c.publishedStreamName.len > 0:
        removePublisher(c.publishedStreamName)
        reset(c.publishedStreamName)
        reset(c.publishedStreamId)
      sendAmfCommand(c, msgStreamId, @[ kAmfCmdResult, newNumber(txn), kAmfNull ])
      return

    if not isDataMsg: return

  # Forward media (audio/video) and metadata (AMF0 data type 18) from publisher to subscribers
  if (msgTypeId == 8 or msgTypeId == 9 or msgTypeId == 18) and payloadLen > 0 and payloadPtr != nil:
    for name, se in gStreams.pairs:
      var matchPub = false
      if se.publisher == c and se.publisherStreamId == msgStreamId:
        matchPub = true
      elif c.publishedStreamName.len > 0 and se.name == c.publishedStreamName and se.publisherStreamId == msgStreamId:
        matchPub = true
      if not matchPub:
        continue

      var
        isVideoSeq: bool
        isAudioSeq: bool
        isVideoKeyframe: bool

      let bp = cast[ptr UncheckedArray[byte]](payloadPtr)
      if msgTypeId == 9 and payloadLen >= 2:
        let codecId = bp[0] and 0x0F'u8
        let frameType = (bp[0] shr 4) and 0x0F'u8
        isVideoKeyframe = frameType == 1'u8
        isVideoSeq = (codecId == 7'u8 and bp[1] == 0'u8)
      elif msgTypeId == 8 and payloadLen >= 2:
        let soundFormat = (bp[0] shr 4) and 0x0F
        isAudioSeq = (soundFormat == 10 and bp[1] == 0'u8)

      let needCache = (msgTypeId == 18) or isVideoSeq or isAudioSeq
      let hasSubs = se.subscribers.len > 0
      if not hasSubs and not needCache:
        break

      let shared = newSharedPayload(payloadPtr, payloadLen)

      var dropSubs: seq[SubscriberEntry] = @[]
      for s in se.subscribers:
        if s.conn == nil or s.conn.conn == nil or s.conn.closed:
          dropSubs.add(s)
          continue

        let nowTick = epochMs()
        # Detect slow subscriber: if sendfile is active or last send backed up
        let outBusy = s.conn.conn.sendFileFd >= 0

        if s.conn.recoveringUntilMs > nowTick:
          if msgTypeId == 9 and not isVideoKeyframe:
            continue
          if msgTypeId == 8:
            continue

        if s.conn.slowSubscriber or s.conn.waitForKeyframe:
          if outBusy:
            continue
          if se.metaPayload.len > 0:
            sendRtmpMessage(s.conn, csid = 4, msgTypeId = 18,
                        msgStreamId = s.msgStreamId, payload = se.metaPayload)
          if se.audioSeqPayload.len > 0:
            sendRtmpMessage(s.conn, csid = 4, msgTypeId = 8,
                        msgStreamId = s.msgStreamId, payload = se.audioSeqPayload)
          if se.videoSeqPayload.len > 0:
            sendRtmpMessage(s.conn, csid = 4, msgTypeId = 9,
                        msgStreamId = s.msgStreamId, payload = se.videoSeqPayload)
          s.conn.slowSubscriber = false
          s.conn.waitForKeyframe = false
          s.conn.recoveringUntilMs = 0

        if not sendRtmpMessageShared(s.conn, 4, msgTypeId, s.msgStreamId, shared, int(timestamp)):
          removeSubscriber(name, s.conn)
          dropSubs.add(s)
          continue

        if s.conn.recoveringUntilMs > nowTick:
          if msgTypeId == 9 and not isVideoKeyframe:
            continue
          if msgTypeId == 8 and outBusy:
            continue

      if dropSubs.len > 0:
        var aliveSubs: seq[SubscriberEntry] = @[]
        for s in se.subscribers:
          var keep = true
          for d in dropSubs:
            if d.conn == s.conn and d.msgStreamId == s.msgStreamId:
              keep = false
              break
          if keep:
            aliveSubs.add(s)
        se.subscribers = aliveSubs

      if needCache:
        if msgTypeId == 18:
          se.metaPayload = shared.bytes
        elif isVideoSeq:
          se.videoSeqPayload = shared.bytes
        elif isAudioSeq:
          se.audioSeqPayload = shared.bytes
      break

  # bookkeeping: count bytes received and send ACK when threshold hit
  if payloadLen > 0:
    c.state.bytesReceivedSinceAck = c.state.bytesReceivedSinceAck + uint64(payloadLen)
    if c.state.windowAckSize > 0 and c.state.bytesReceivedSinceAck >= uint64(c.state.windowAckSize):
      let ackVal = uint32(c.state.bytesReceivedSinceAck and 0xFFFFFFFF'u64)
      sendAcknowledgement(c, ackVal)
      c.state.bytesReceivedSinceAck = 0'u64

#
# Read callback (powpow onData)
#
var gStaging = initTable[int, seq[byte]]()  # connId -> staging buffer for partial headers

proc onClientData(conn: Connection, data: openArray[byte]) =
  let c = cast[ConnCtx](conn.data)
  if c == nil: return

  let avail = data.len
  if avail <= 0: return

  # Prepend staged bytes from previous read if any
  var feedData: seq[byte]
  var hasStaging = false
  if gStaging.hasKey(c.connId) and gStaging[c.connId].len > 0:
    feedData = gStaging[c.connId]
    feedData.add(data)
    hasStaging = true
    gStaging[c.connId].setLen(0)

  let feedPtr = if hasStaging: cast[ptr byte](addr feedData[0])
                else: cast[ptr byte](unsafeAddr data[0])
  let feedLen = if hasStaging: feedData.len
                else: avail

  while true:
    if c.hsState == HS_INIT:
      if feedLen < 1 + RTMP_HANDSHAKE_SIZE:
        # Stage partial handshake bytes
        if hasStaging and feedLen > 0:
          gStaging[c.connId] = feedData
        elif not hasStaging and feedLen > 0:
          gStaging[c.connId] = @data
        return

      let pbytes = cast[ptr UncheckedArray[byte]](feedPtr)
      let c0 = pbytes[0]
      if c0 != 0x03'u8:
        conn.close()
        return

      let c1ptr = cast[ptr UncheckedArray[byte]](addr pbytes[1])
      let enhanced = isEnhancedC1(c1ptr)

      var outS0 = [byte 0x03]
      discard conn.send(outS0)

      if enhanced and validateC1Digest(c1ptr):
        # Enhanced handshake: create S1 with HMAC digest using FMS key
        var s1enh = newSeq[byte](RTMP_HANDSHAKE_SIZE)
        createS1Enhanced(cast[ptr UncheckedArray[byte]](addr s1enh[0]))
        c.serverS1 = s1enh
        discard conn.send(s1enh)
        # S2 = HMAC signature over C1 using full FP key
        var s2sig = newSeq[byte](RTMP_HANDSHAKE_SIZE)
        signS2(cast[ptr UncheckedArray[byte]](addr s2sig[0]), c1ptr)
        discard conn.send(s2sig)
      else:
        # Plain handshake: echo C1 as S2
        let s1 = buildServerS1()
        c.serverS1 = s1
        discard conn.send(s1)
        var s2echo = newSeq[byte](RTMP_HANDSHAKE_SIZE)
        copyMem(addr s2echo[0], c1ptr, RTMP_HANDSHAKE_SIZE)
        discard conn.send(s2echo)

      c.hsState = HS_S0S1_SENT
      continue

    if c.hsState == HS_S0S1_SENT:
      if feedLen < RTMP_HANDSHAKE_SIZE:
        if hasStaging and feedLen > 0:
          gStaging[c.connId] = feedData
        elif not hasStaging and feedLen > 0:
          gStaging[c.connId] = @data
        return

      let c2bytes = cast[ptr UncheckedArray[byte]](feedPtr)
      if c.serverS1.len == RTMP_HANDSHAKE_SIZE:
        var match = true
        for i in 0 ..< RTMP_HANDSHAKE_SIZE:
          if c.serverS1[i] != c2bytes[i]:
            match = false
            break

      c.hsState = HS_DONE
      continue

    # HS_DONE: feed RTMP chunks
    let consumed = feedBytes(c.chunkCtx, feedPtr, feedLen)
    if consumed <= 0:
      return
    if consumed < feedLen:
      # Stage unconsumed tail
      gStaging[c.connId] = newSeq[byte](feedLen - consumed)
      copyMem(addr gStaging[c.connId][0],
              cast[ptr UncheckedArray[byte]](cast[uint](feedPtr) + consumed.uint),
              feedLen - consumed)
    return

proc onClientClose(conn: Connection) =
  let c = cast[ConnCtx](conn.data)
  if c == nil: return
  cleanupConn(c)
  c.closeReason = "EOF"
  conn.data = nil
  gStaging.del(c.connId)

proc onClientError(conn: Connection, err: string) =
  let c = cast[ConnCtx](conn.data)
  if c == nil: return
  cleanupConn(c)
  c.closeReason = "ERROR"
  conn.data = nil
  gStaging.del(c.connId)

#
# REST API handler (powpow HttpServer)
#
# Plain pointer global — gcsafe (no GC-managed memory).
# Set during newRTMPServer; the ref RtmpMonitor is kept alive by gMonitor.
var gMonitorPtr: pointer = nil

proc apiRequestHandler*(req: HttpRequest, res: HttpResponse) {.gcsafe.} =
  let monitor = cast[ref RtmpMonitor](gMonitorPtr)
  let uri = req.getPath()
  if uri != "/":
    res.status(Http404).send("""{"ok":false,"error":"not_found"}""")
    return
  res.status(Http200)
     .header("Content-Type", "application/json")
     .header("Connection", "close")
     .send($monitor.toJson())

#
# Accept callback (powpow onAccept)
#
proc onClientAccept(conn: Connection) =
  ## powpow onAccept: called after accept, before read registration
  var local = ConnCtx(
    conn: conn,
    state: RtmpConnState(
      peerChunkSize: RTMP_DEFAULT_CHUNK_SIZE,
      localChunkSize: RTMP_DEFAULT_CHUNK_SIZE,
      streams: initTable[int, pointer]()
    ),
    hsState: HS_INIT,
    nextStreamId: 1,
    connId: gNextConnId,
    clientIp: conn.clientIp,
  )
  gNextConnId.inc
  conn.data = cast[pointer](local)
  local.chunkCtx = initChunkStreamCtx(local.state.peerChunkSize)
  setOnMessage(local.chunkCtx, onChunkMessage, cast[pointer](local))

#
# Public API
#
proc newRTMPServer*(settings: RtmpServerSettings = RtmpServerSettings()): RTMPServer =
  ## Creates a new RTMP server instance with the specified settings.
  new(result)
  result.loop = newLoop()
  result.settings = settings
  # Bind REST API listener if enabled
  if settings.enableRestApi:
    gMonitorPtr = cast[pointer](gMonitor)
    result.httpServer = newHttpServer(result.loop, populate = false)
    result.httpServer.handler = apiRequestHandler
    result.httpServer.listen("0.0.0.0", settings.restApiPort.int)

proc startServer*(server: RTMPServer) =
  ## Start RTMP server on specified port (default 1935)
  ## This is a blocking call that runs the event loop.
  let srv = newTcpServer(server.loop,
    onData = onClientData,
    onAccept = onClientAccept,
    onClose = onClientClose)
  srv.listen("0.0.0.0", server.settings.rtmpPort.int)
  server.loop.run()
