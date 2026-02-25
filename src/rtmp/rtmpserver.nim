# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

## This module implements RTMP server functionality, including connection handling,
## RTMP message parsing, and a simple pub/sub mechanism for streams.
## 
## It uses libevent for asynchronous network I/O and supports basic RTMP commands and
## control messages. The server can be extended to handle incoming streams as
## needed.

import std/[posix, times, strutils, tables, sequtils]
import pkg/libevent/bindings/[event, bufferevent, buffer, http, listener]

import ./server/actionmessage
import ./server/chunkstream
import ./server/rtmpmonitor

from std/net import Port, `$`

const
  DEFAULT_RTMP_PORT* = 1935
    ## Default port for RTMP servers to listen on
  RTMP_HANDSHAKE_SIZE* = 1536
    ## RTMP handshake S1/S2 size in bytes
  MAX_SUBSCRIBER_OUTBUF* = 8 * 1024 * 1024
    ## todo allow for configurable limits and better
    ## backpressure handling
  SLOW_SUBSCRIBER_RESUME_OUTBUF = MAX_SUBSCRIBER_OUTBUF div 2
    ## When a subscriber is marked as slow due to exceeding the outbuf limit,
    ## we can resume sending to them once their outbuf drops below this threshold
  LIVE_JUMP_COOLDOWN_MS = 1500'i64
    ## Prevent repeated jump-to-live in very short intervals

# RTMP state / types
const
  HS_INIT* = 0
    ## Initial state before handshake starts
  HS_S0S1_SENT* = 1
    ## Handshake state after sending S0 and S1, waiting for S0S1S2 from client
  HS_DONE* = 2
    ## Handshake complete, ready for RTMP messages

  RTMP_DEFAULT_CHUNK_SIZE* = 128
    ## Default RTMP chunk size before any SetChunkSize messages are processed
  RTMP_MAX_CHUNK_SIZE* = 65536
    ## Maximum RTMP chunk size supported by this implementation (for sanity checking)

proc epochMs(): int64 {.inline.} =
  int64(times.epochTime() * 1000.0)

type
  SharedPayload = ref object
    refs: int
      # Reference count for this payload; used for
      # zero-copy streaming to manage memory across connections
    bytes: seq[byte]
      # Shared payload data for zero-copy streaming.
      # Reference counted to manage memory across connections

  RtmpConnState* = object
    ## Per-connection RTMP state, including chunk sizes, acknowledgment tracking, and stream state
    peerChunkSize*: int
      ## Chunk size specified by the peer (client), used
      ## for parsing incoming messages
    localChunkSize*: int
      ## Chunk size we use for sending messages to the peer;
      ## can be adjusted with SetChunkSize
    windowAckSize*: uint32
      ## Acknowledgment window size specified by the peer;
      ## we track how many bytes we've received since the last ACK
    bytesReceivedSinceAck*: uint64
      ## Counter for bytes received since last acknowledgment sent to peer
    streams*: Table[int, pointer] # placeholder
      ## Table of active streams by stream ID; can be used
      ## to track per-stream state if needed
  
  RtmpServer* = ref object
    ## Main RTMP server object holding the server context and any global state
    base*: ptr event_base
      ## Libevent base for managing events
    listenFd*: cint
      ## File descriptor for the listening socket
    restApiListener*: ptr evconnlistener
      ## Listener used by the REST API HTTP server
    restApiHttp*: ptr evhttp
      ## Libevent HTTP server handle for REST API

  RTMPServerError* = object of CatchableError

  ConnCtx* = ref object
    bev*: ptr bufferevent
      ## Libevent buffer event for this connection, used for reading/writing data
    state*: RtmpConnState
      ## Per-connection RTMP protocol state (chunk sizes, ack window, stream table, etc)
    inbuf*: ptr Evbuffer
      ## Input buffer for reading incoming data from the client
    outbuf*: ptr Evbuffer
      ## Output buffer for writing outgoing data to the client
    hsState*: int
      ## Handshake state: HS_INIT, HS_S0S1_SENT, or HS_DONE
    partialHdr*: seq[byte]
      ## Buffer for accumulating partial RTMP chunk headers across reads
    partialMsg*: seq[byte]
      ## Buffer for accumulating partial RTMP chunk payloads across reads
    expectedMsgLen*: int
      ## Expected length of the current RTMP message being assembled
    msgTypeId*: int
      ## RTMP message type ID of the current message being parsed
    msgStreamId*: int
      ## RTMP message stream ID of the current message being parsed
    chunkCtx*: ChunkStreamCtx
      ## Per-connection chunk stream context for parsing RTMP chunks
    serverS1*: seq[byte]
      ## Server's S1 handshake data (used to validate C2 from client)
    nextStreamId*: int
      ## Next available stream ID to assign for createStream requests
    connId*: int
      ## Unique connection ID for debugging and tracking
    clientIp*: string
      ## IP address of the connected client (for logging and monitoring)
    closed*: bool
      ## True if this connection has been closed and cleaned up
    closeReason*: string
      ## Reason for connection closure (e.g., "EOF", "ERROR", "cleanup")
    publishedStreamName*: string
      ## Name of the stream this connection is publishing (if any)
    publishedStreamId*: int
      ## Stream ID assigned to the published stream (if any)
    subscriptions*: Table[string, int]
      ## Map of stream name to stream ID for all streams this connection is subscribed to
    slowSubscriber*: bool
      ## true when subscriber output queue exceeded MAX_SUBSCRIBER_OUTBUF
    waitForKeyframe*: bool
      ## when recovering from lag, drop media until next video keyframe
    lastLiveJumpMs*: int64
      ## Last time this subscriber was force-jumped to live

proc sendAmfCommand(conn: ConnCtx; msgStreamId: int; vals: seq[AMF0Value])
proc sendRtmpMessage(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; payload: seq[byte]; timestamp: int = 0): bool
proc sendRtmpMessageShared(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; sp: SharedPayload; timestamp: int = 0): bool
proc removeSubscriber(name: string, conn: ConnCtx)
proc connSummary(conn: ConnCtx): string

# Helpers
proc setReuseAndNonblock(fd: cint) =
  var one: cint = 1
  discard setsockopt(SocketHandle(fd), SOL_SOCKET, SO_REUSEADDR, addr one, SockLen(sizeof(one)))
  var flags = fcntl(fd, F_GETFL, 0)
  if flags >= 0:
    discard fcntl(fd, F_SETFL, flags or O_NONBLOCK)

proc buildServerS1(): seq[byte] =
  var s = newSeq[byte](RTMP_HANDSHAKE_SIZE)
  let ts = int(times.getTime().toUnix())
  let ts32 = int32(ts)
  cast[ptr int32](addr s[0])[] = ts32
  cast[ptr int32](addr s[4])[] = 0'i32
  for i in 8 ..< RTMP_HANDSHAKE_SIZE:
    s[i] = byte((ts + i) and 0xFF)
  s

var
  gConnKeepAlive = initTable[pointer, ConnCtx]()
  gSharedKeepAlive = initTable[pointer, SharedPayload]()
  gPendingClientIp = initTable[pointer, string]()
  gNextConnId = 1

#
# Shared payload management for zero-copy streaming
#
proc sharedRetain(sp: SharedPayload) =
  if sp == nil: return
  if sp.refs == 0:
    gSharedKeepAlive[cast[pointer](sp)] = sp
  inc sp.refs

proc sharedRelease(sp: SharedPayload) =
  if sp == nil: return
  if sp.refs <= 0: return
  dec sp.refs
  if sp.refs == 0:
    let k = cast[pointer](sp)
    if gSharedKeepAlive.hasKey(k):
      gSharedKeepAlive.del(k)

proc evbufRefCleanup(data: pointer, datlen: csize_t, cleanupArg: pointer) {.cdecl.} =
  if cleanupArg == nil: return
  let sp = cast[SharedPayload](cleanupArg)
  sharedRelease(sp)

proc newSharedPayload(payloadPtr: pointer, payloadLen: int): SharedPayload =
  result = SharedPayload(refs: 0, bytes: @[])
  if payloadLen <= 0 or payloadPtr == nil:
    return
  result.bytes = newSeq[byte](payloadLen)
  copyMem(addr result.bytes[0], payloadPtr, payloadLen)


proc monitorAddStream*(name: string, pubConn: ConnCtx) =
  ## Add a new stream to the monitor with the given name and publisher connection
  var pub = RtmpPublisher(id: $pubConn.connId, ip: pubConn.clientIp)
  let stream = RtmpStream(
    id: name,
    publisher: pub,
    created_at: now()
  )
  gMonitor.streams[name] = stream

proc monitorRemoveStream*(name: string) =
  gMonitor.streams.del(name)

proc monitorAddSubscriber*(streamName: string, subConn: ConnCtx) =
  ## Add a subscriber to the monitor's stream entry for the given
  ## stream name and subscriber connection
  if gMonitor.streams.hasKey(streamName):
    let sub = RtmpSubscriber(id: $subConn.connId, ip: subConn.clientIp)
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
    ## Represents a subscriber to a stream, holding the connection context
    ## and assigned message stream ID for sending messages to this subscriber
    conn*: ConnCtx
      ## Connection of the subscriber
    msgStreamId*: int
      ## Stream ID assigned to this subscription for
      ## sending messages to the subscriber

  StreamEntry* = ref object
    ## Represents a published stream, holding the publisher connection,
    ## assigned stream ID, list of subscribers, and cached metadata/sequence
    ## headers for new subscribers
    name*: string
      ## Name of the stream (e.g., "live/streamKey")
    publisher*: ConnCtx
      ## Connection that is publishing this stream (if any)
    publisherStreamId*: int
      ## Stream ID assigned to the publisher's stream for this stream name
    subscribers*: seq[SubscriberEntry]
      ## List of subscribers to this stream
    # cached payloads to send to new subscribers
    metaPayload*: seq[byte]
      ## Cached metadata payload (RTMP message type 18) to
      ## send to new subscribers for stream initialization
    videoSeqPayload*: seq[byte]
      ## Cached video sequence header payload (RTMP message type 9)
      ## to send to new subscribers for stream initialization
    audioSeqPayload*: seq[byte]
      ## Cached audio sequence header payload (RTMP message type 8)
      ## to send to new subscribers for stream initialization


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

  kInfoPlayReset = amfObj({
    "level": newString("status"),
    "code": newString("NetStream.Play.Reset"),
    "description": newString("Resetting play state.")
  })

  kInfoPlayStop = amfObj({
    "level": newString("status"),
    "code": newString("NetStream.Play.Stop"),
    "description": newString("Stream ended")
  })

  kInfoStreamNotFound = amfObj({
    "level": newString("error"),
    "code": newString("NetStream.Play.StreamNotFound"),
    "description": newString("Stream not found")
  })

var gStreams = initTable[string, StreamEntry]()

proc addPublisher(name: string, conn: ConnCtx, pubStreamId: int) =
  if name.len == 0 or conn == nil: return
  var se = gStreams.getOrDefault(name, nil)
  if se == nil:
    se = StreamEntry(name: name, publisher: conn, publisherStreamId: pubStreamId, subscribers: @[], metaPayload: @[], videoSeqPayload: @[], audioSeqPayload: @[])
    gStreams[name] = se
  else:
    se.publisher = conn
    se.publisherStreamId = pubStreamId
  conn.publishedStreamName = name
  conn.publishedStreamId = pubStreamId

  if gStreams.hasKey(name):
    let tse = gStreams[name]
  monitorAddStream(name, conn)
  # clear prev cached seq headers when a new publisher arrives
  se.metaPayload.setLen(0)
  se.videoSeqPayload.setLen(0)
  se.audioSeqPayload.setLen(0)

proc addSubscriber(name: string, conn: ConnCtx, subStreamId: int) =
  if name.len == 0 or conn == nil: return
  var se = gStreams.getOrDefault(name, nil)
  if se == nil:
    se = StreamEntry(name: name, publisher: nil, publisherStreamId: 0, subscribers: @[], metaPayload: @[], videoSeqPayload: @[], audioSeqPayload: @[])
    gStreams[name] = se
    # new stream entry created
  se.subscribers.add(SubscriberEntry(conn: conn, msgStreamId: subStreamId))
  conn.subscriptions[name] = subStreamId
  # send cached meta and sequence headers (if any)
  # so the new subscriber can initialize decoders
  if se.metaPayload.len > 0:
    # if sending fails (for example, due to a slow subscriber), we should remove this
    # subscriber to avoid keeping bad connections around
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
  var o: seq[SubscriberEntry] = @[]
  for s in se.subscribers:
    if s.conn != conn:
      o.add(s)
  se.subscribers = o
  
  # also remove from conn's subscription table
  if conn.subscriptions.hasKey(name):
    conn.subscriptions.del(name)

  # prune empty placeholder streams (no publisher + no subscribers)
  if se.publisher == nil and se.subscribers.len == 0 and gStreams.hasKey(name):
    gStreams.del(name)
  monitorRemoveSubscriber(name, conn)

proc removePublisher(name: string) =
  # When a publisher disconnects, we remove the stream and notify all subscribers that the stream has ended
  if name.len == 0: return
  if gStreams.hasKey(name):
    let se = gStreams[name]
    # notify subscribers that stream ended
    for s in se.subscribers:
      if s.conn != nil:
        sendAmfCommand(s.conn, s.msgStreamId, @[ kAmfCmdOnStatus, kAmfNum0, kAmfNull, kInfoPlayStop ])
    monitorRemoveStream(name)
    gStreams.del(name)

proc cleanupConn(conn: ConnCtx) =
  if conn == nil: return
  if conn.closed: return
  conn.closed = true
  if conn.closeReason.len == 0:
    conn.closeReason = "cleanup"

  if conn.publishedStreamName.len > 0:
    removePublisher(conn.publishedStreamName)

  # IMPORTANT: do not mutate table while iterating it
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

proc connSummary(conn: ConnCtx): string =
  if conn == nil: return "conn=nil"
  result = "connId=" & $conn.connId &
    " published=\"" & conn.publishedStreamName & "\"" &
    " pubStreamId=" & $conn.publishedStreamId &
    " subs=" & $conn.subscriptions.len

proc bev_event_cb(bev: ptr bufferevent, what: cshort, ctx: pointer) {.cdecl.} =
  # Event callback: ensure ConnCtx is freed when connection
  # closes/errors to prevent memory leaks; also log events
  # for debugging
  let cbarg = ctx
  # echo "bev_event: conn=", $(cast[int](cbarg)), " what=", what,
  #      " flags=",(if (what and BEV_EVENT_CONNECTED) != 0: " CONNECTED" else: ""),
  #      (if (what and BEV_EVENT_EOF) != 0: " EOF" else: ""),
  #      (if (what and BEV_EVENT_ERROR) != 0: " ERROR" else: ""),
  #      (if (what and BEV_EVENT_TIMEOUT) != 0: " TIMEOUT" else: "")
  if cbarg != nil and gConnKeepAlive.hasKey(cbarg):
    let conn = gConnKeepAlive[cbarg]
    # echo "  ", connSummary(conn), " hsState=", conn.hsState, " closed=", conn.closed

  # if (what and BEV_EVENT_CONNECTED) != 0:
    # echo "Connection established"

  let
    isEof = (what and BEV_EVENT_EOF) != 0
    isErr = (what and BEV_EVENT_ERROR) != 0
  if not (isEof or isErr): return

  if bev != nil:
    let k = cast[pointer](bev)
    if gPendingClientIp.hasKey(k):
      gPendingClientIp.del(k)

  if cbarg != nil and gConnKeepAlive.hasKey(cbarg):
    let conn = gConnKeepAlive[cbarg]
    cleanupConn(conn)
    conn.closeReason = (if isErr: "ERROR" else: "EOF")
    conn.bev = nil
    conn.inbuf = nil
    conn.outbuf = nil
    gConnKeepAlive.del(cbarg)

  # Free exactly once
  if bev != nil:
    bufferevent_free(bev)

# 
# RTMP output helpers (very small)
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

proc sendRtmpMessageShared(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; sp: SharedPayload; timestamp: int = 0): bool =
  if conn == nil or conn.bev == nil or sp == nil: return false
  let payloadLen = sp.bytes.len
  if csid <= 1 or csid >= 64:
    raise newException(RTMPServerError, "Only CSID 2..63 supported in this minimal sender")
  if payloadLen < 0: return false

  let chunkSize = max(conn.state.localChunkSize, 1)
  let ts = max(0, min(timestamp, 0xFFFFFF))
  let output = bufferevent_get_output(conn.bev)
  if output == nil: return false

  var h: array[12, byte]
  h[0] = byte((0 shl 6) or (csid and 0x3F))
  h[1] = byte((ts shr 16) and 0xFF)
  h[2] = byte((ts shr 8) and 0xFF)
  h[3] = byte(ts and 0xFF)
  h[4] = byte((payloadLen shr 16) and 0xFF)
  h[5] = byte((payloadLen shr 8) and 0xFF)
  h[6] = byte(payloadLen and 0xFF)
  h[7] = byte(msgTypeId and 0xFF)
  h[8]  = byte(msgStreamId and 0xFF)
  h[9]  = byte((msgStreamId shr 8) and 0xFF)
  h[10] = byte((msgStreamId shr 16) and 0xFF)
  h[11] = byte((msgStreamId shr 24) and 0xFF)

  if evbuffer_add(output, addr h[0], csize_t(h.len)) != 0:
    return false
  if payloadLen == 0:
    return true

  var off = 0
  var cont: byte = byte((3 shl 6) or (csid and 0x3F))

  while off < payloadLen:
    let take = min(chunkSize, payloadLen - off)
    if take > 0:
      sharedRetain(sp)
      let rc = evbuffer_add_reference(
        output,
        cast[pointer](unsafeAddr sp.bytes[off]),
        csize_t(take),
        evbufRefCleanup,
        cast[pointer](sp)
      )
      if rc != 0:
        sharedRelease(sp)
        return false
      off += take
    if off < payloadLen:
      if evbuffer_add(output, addr cont, 1) != 0:
        # If we fail to add the continuation header, we need to
        # clean up any references already added for this message
        return false
  result = true

proc sendRtmpMessage(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; payload: seq[byte]; timestamp: int = 0): bool =
  # Use zero-copy for cached payloads (AMF/meta/seq headers)
  if payload.len > 0 and (msgTypeId == 18 or msgTypeId == 9 or msgTypeId == 8 or msgTypeId == 20):
    # Wrap in persistent SharedPayload for safe reference
    let sp = SharedPayload(refs: 0, bytes: payload)
    return sendRtmpMessageShared(conn, csid, msgTypeId, msgStreamId, sp, timestamp)
  # Fallback: old method for small/one-off messages
  if conn == nil or conn.bev == nil: return false
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

  # payload split into chunks; continuation chunks use fmt=3
  var off = 0
  while off < payload.len:
    let take = min(chunkSize, payload.len - off)
    if take > 0:
      buf.add(payload[off ..< off + take])
      off += take
    if off < payload.len:
      buf.add(byte((3 shl 6) or (csid and 0x3F)))

  let rc = bufferevent_write(conn.bev, buf[0].addr, csize_t(buf.len))
  return rc == 0

proc sendSetChunkSize(conn: ConnCtx; size: int) =
  var p: seq[byte] = @[]
  put4BE(p, uint32(size))
  discard sendRtmpMessage(conn, csid = 2, msgTypeId = 1, msgStreamId = 0, payload = p)

proc sendWindowAckSize(conn: ConnCtx; size: uint32) =
  var p: seq[byte] = @[]
  put4BE(p, size)
  discard sendRtmpMessage(conn, csid = 2, msgTypeId = 5, msgStreamId = 0, payload = p)

proc sendSetPeerBandwidth(conn: ConnCtx; size: uint32; limitType: byte = 2) =
  var p: seq[byte] = @[]
  put4BE(p, size)
  p.add(limitType) # 0=hard,1=soft,2=dynamic
  discard sendRtmpMessage(conn, csid = 2, msgTypeId = 6, msgStreamId = 0, payload = p)

proc sendAcknowledgement(conn: ConnCtx; seq: uint32) =
  var p: seq[byte] = @[]
  put4BE(p, seq)
  discard sendRtmpMessage(conn, csid = 2, msgTypeId = 3, msgStreamId = 0, payload = p)

proc sendUserControlStreamBegin(conn: ConnCtx; streamId: int) =
  var p: seq[byte] = @[]
  # eventType (2 bytes BE) = 0 (StreamBegin)
  p.add(byte(0)); p.add(byte(0))
  put4BE(p, uint32(streamId))
  discard sendRtmpMessage(conn, csid = 2, msgTypeId = 4, msgStreamId = 0, payload = p)

proc amfObj(pairs: openArray[(string, AMF0Value)]): AMF0Value =
  result = newObject()
  for (k, v) in pairs:
    result.obj[k] = v

proc sendAmfCommand(conn: ConnCtx; msgStreamId: int; vals: seq[AMF0Value]) =
  let payload = encodeAMF0Values(vals)
  discard sendRtmpMessage(conn, csid = 3, msgTypeId = 20, msgStreamId = msgStreamId, payload = payload)

proc getTxnId(vals: seq[AMF0Value]): float64 =
  if vals.len >= 2 and vals[1] != nil and vals[1].typ == AMF0_Number:
    return vals[1].num
  result = 1.0

# 
# Read callback
# 
# initially attach with nil cbarg; bev_read_cb will create and reattach ConnCtx on first read
proc bev_read_cb(bev: ptr bufferevent, ctx: pointer) {.cdecl.} =
  var conn = cast[ConnCtx](ctx)
  if conn == nil:
    var resolvedIp: string
    let bevKey = cast[pointer](bev)
    if gPendingClientIp.hasKey(bevKey):
      resolvedIp = gPendingClientIp[bevKey]
      gPendingClientIp.del(bevKey)

    var local = ConnCtx(
      bev: bev,
      state: RtmpConnState(
        peerChunkSize: RTMP_DEFAULT_CHUNK_SIZE,
        localChunkSize: RTMP_DEFAULT_CHUNK_SIZE, # keep 128 until you successfully send SetChunkSize
        streams: initTable[int, pointer]()
      ),
      inbuf: bufferevent_get_input(bev),
      outbuf: bufferevent_get_output(bev),
      hsState: HS_INIT,
      nextStreamId: 1,
      connId: gNextConnId,
      clientIp: resolvedIp,
    )
    gNextConnId.inc
    conn = local
    gConnKeepAlive[cast[pointer](conn)] = conn

    conn.chunkCtx = initChunkStreamCtx(conn.state.peerChunkSize)
    proc onChunkMessage(msgTypeId: int, msgStreamId: int, timestamp: uint32,
              payloadPtr: ptr byte, payloadLen: int, arg: pointer) =
      let c = cast[ConnCtx](arg)
      if c == nil: return

      # Diagnostic: log every incoming chunk message
      # echo "onChunkMessage: type=", msgTypeId, " stream=", msgStreamId, " len=", payloadLen
      if payloadLen > 0 and payloadPtr != nil:
        let dumpN = if payloadLen < 16: payloadLen else: 16
        let bp = cast[ptr UncheckedArray[byte]](payloadPtr)
        var hs = ""
        for i in 0 ..< dumpN:
          hs.add($((bp[i] shr 4) and 0xF))
          hs.add($((bp[i]) and 0xF))
          hs.add(' ')
        # echo "  data (first ", dumpN, " bytes): ", hs

      # Protocol control: Set Chunk Size (type 1, 4 bytes BE)
      if msgTypeId == 1 and payloadLen >= 4 and payloadPtr != nil:
        let b = cast[ptr UncheckedArray[byte]](payloadPtr)
        let newSize =
          (int(b[0]) shl 24) or (int(b[1]) shl 16) or (int(b[2]) shl 8) or int(b[3])
        if newSize > 0 and newSize <= RTMP_MAX_CHUNK_SIZE:
          c.state.peerChunkSize = newSize
          setPeerChunkSize(c.chunkCtx, newSize)
          # echo "peer chunk size set to ", newSize
        return

      # Window Acknowledgement Size (type 5): client tells us preferred ACK window
      if msgTypeId == 5 and payloadLen >= 4 and payloadPtr != nil:
        let b = cast[ptr UncheckedArray[byte]](payloadPtr)
        let win = (uint32(b[0]) shl 24) or (uint32(b[1]) shl 16) or (uint32(b[2]) shl 8) or uint32(b[3])
        c.state.windowAckSize = win
        c.state.bytesReceivedSinceAck = 0'u64
        # echo "Client requested windowAckSize=", win
        return

      # Set Peer Bandwidth (type 6): client informs us of bandwidth settings
      if msgTypeId == 6 and payloadLen >= 5 and payloadPtr != nil:
        let b = cast[ptr UncheckedArray[byte]](payloadPtr)
        let bw = (uint32(b[0]) shl 24) or (uint32(b[1]) shl 16) or (uint32(b[2]) shl 8) or uint32(b[3])
        let limitType = b[4]
        # echo "Client SetPeerBandwidth: ", bw, " type=", limitType
        return

      # Acknowledgement (type 3): client acknowledging bytes we've sent
      if msgTypeId == 3 and payloadLen >= 4 and payloadPtr != nil:
        let b = cast[ptr UncheckedArray[byte]](payloadPtr)
        let ack = (uint32(b[0]) shl 24) or (uint32(b[1]) shl 16) or (uint32(b[2]) shl 8) or uint32(b[3])
        # echo "Client ACK: ", ack
        return

      # Abort message (type 2): client requests abort of a stream id in payload
      if msgTypeId == 2 and payloadLen >= 4 and payloadPtr != nil:
        let b = cast[ptr UncheckedArray[byte]](payloadPtr)
        let abortCsid = (int(b[0]) shl 24) or (int(b[1]) shl 16) or (int(b[2]) shl 8) or int(b[3])
        echo "Client Abort for CSID: ", abortCsid
        # remove any per-chunkstream state
        if c.chunkCtx != nil:
          c.chunkCtx.streams.del(abortCsid)
        return

      # User control (type 4) can be used for StreamBegin/EOF etc; log minimal info
      if msgTypeId == 4 and payloadLen >= 2 and payloadPtr != nil:
        let b = cast[ptr UncheckedArray[byte]](payloadPtr)
        let eventType = (int(b[0]) shl 8) or int(b[1])
        # echo "UserControl event: ", eventType
        return

      # Commands/data: AMF0 (type 20/18) and AMF3 (type 17/15 with AMF0 marker)
      if (msgTypeId == 20 or msgTypeId == 18 or msgTypeId == 17 or msgTypeId == 15) and payloadLen > 0 and payloadPtr != nil:
        var amfPtr = payloadPtr
        var amfLen = payloadLen
        let isDataMsg = (msgTypeId == 18 or msgTypeId == 15)
        if msgTypeId == 17 or msgTypeId == 15:
          # AMF3 commands/data often start with 0x00 indicating AMF0 encoding
          let b = cast[ptr UncheckedArray[byte]](payloadPtr)
          if payloadLen >= 1 and b[0] == 0'u8:
            amfPtr = cast[ptr byte](addr b[1])
            amfLen = payloadLen - 1
          else:
            # Unsupported AMF3 payload; ignore for now
            return
        var vals: seq[AMF0Value]
        try:
          vals = decodeAllAMF0Ptr(amfPtr, amfLen)
        except:
          echo "AMF0 decode error payloadLen=", amfLen
          # show a small hex snippet to aid debugging
          let maxDump = if amfLen < 64: amfLen else: 64
          let bptr = cast[ptr UncheckedArray[byte]](amfPtr)
          var s = ""
          for i in 0 ..< maxDump:
            s.add($((bptr[i] shr 4) and 0xF))
            s.add($((bptr[i]) and 0xF))
            s.add(' ')
          # echo "AMF0 payload hex (first ", maxDump, " bytes): ", s
          
          # Temporary safety: reply with a generic _result to avoid leaving client waiting
          # echo "Sending generic _result txn=1 to avoid client hang"
          sendAmfCommand(c, 0, @[ kAmfCmdResult, kAmfNum0, kAmfNull ])
          return

        if vals.len == 0 or vals[0] == nil or (vals[0].typ != AMF0_String and vals[0].typ != AMF0_LongString):
          if isDataMsg:
            # allow data messages to fall through to forwarding/caching
            discard
          else:
            return
        let cmd = vals[0].s
        let txn = getTxnId(vals)
        # echo "AMF cmd: ", cmd, " txn=", txn, " msgStreamId=", msgStreamId
        if cmd == "connect":
          # Minimal connection setup
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
          # Many encoders send these; respond with _result to unblock them.
          sendAmfCommand(c, 0, @[ kAmfCmdResult, newNumber(txn), kAmfNull, kAmfNull ])
          return

        if cmd == "createStream":
          let sid = c.nextStreamId
          c.nextStreamId.inc
          # echo "createStream -> assigning sid=", sid, " txn=", txn
          sendAmfCommand(c, 0, @[ kAmfCmdResult, newNumber(txn), kAmfNull, newNumber(float64(sid)) ])
          return
        
        if cmd == "publish":
          # publish(streamName, publishType?)
          var streamName = ""
          for i in 1 ..< vals.len:
            if vals[i] != nil and vals[i].typ == AMF0_String:
              streamName = vals[i].s
              break

          let streamId = msgStreamId
          sendUserControlStreamBegin(c, streamId)

          # register publisher
          addPublisher(streamName, c, streamId)

          let info = amfObj({
            "level": newString("status"),
            "code": newString("NetStream.Publish.Start"),
            "description": newString("Start publishing."),
            "details": newString(streamName)
          })
          sendAmfCommand(c, streamId, @[ newString("onStatus"), kAmfNum0, kAmfNull, info ])
          return
        # echo "Command: " & cmd
        if cmd == "play" or cmd == "play2":
          # play(streamName, ...) - respond with StreamBegin and onStatus Play.Start
          var streamName = ""
          for i in 1 ..< vals.len:
            if vals[i] != nil and vals[i].typ == AMF0_String:
              streamName = vals[i].s
              break

          let sid = msgStreamId
          # If there's no publisher for this stream, inform the player immediately
          let se = gStreams.getOrDefault(streamName, nil)
          if se == nil or se.publisher == nil:
            let nf = amfObj({
              "level": newString("error"),
              "code": newString("NetStream.Play.StreamNotFound"),
              "description": newString("Stream not found")
            })
            sendAmfCommand(c, sid, @[ newString("onStatus"), kAmfNum0, kAmfNull, nf ])
            return

          # Stream begin
          sendUserControlStreamBegin(c, sid)

          # register subscriber mapping
          addSubscriber(streamName, c, sid)

          # send onStatus: NetStream.Play.Reset (optional) then NetStream.Play.Start
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
          # Some players query stream length before playing; reply with a numeric length (0.0 = unknown)
          sendAmfCommand(c, 0, @[ kAmfCmdResult, newNumber(txn), kAmfNull, kAmfNum0 ])
          return

        if cmd == "closeStream":
          # closeStream: reply with _result if txn provided and cleanup subscriptions
          sendAmfCommand(c, msgStreamId, @[ kAmfCmdResult, newNumber(txn), kAmfNull ])
          removeSubscriptionsByStreamId(c, msgStreamId)
          if c.publishedStreamId == msgStreamId and c.publishedStreamName.len > 0:
            removePublisher(c.publishedStreamName)
            c.publishedStreamName = ""
            c.publishedStreamId = 0
          return

        if cmd == "deleteStream":
          # deleteStream: reply with _result and cleanup
          sendAmfCommand(c, msgStreamId, @[ kAmfCmdResult, newNumber(txn), kAmfNull ])
          removeSubscriptionsByStreamId(c, msgStreamId)
          if c.publishedStreamId == msgStreamId and c.publishedStreamName.len > 0:
            removePublisher(c.publishedStreamName)
            reset(c.publishedStreamName)
            reset(c.publishedStreamId)
          return

        if cmd == "FCUnpublish" or cmd == "unpublish":
          # explicit unpublish request from publisher
          if c.publishedStreamName.len > 0:
            removePublisher(c.publishedStreamName)
            reset(c.publishedStreamName)
            reset(c.publishedStreamId)
          sendAmfCommand(c, msgStreamId, @[ kAmfCmdResult, newNumber(txn), kAmfNull ])
          return

        if not isDataMsg: return

      # Forward media (audio/video) and metadata (AMF0 data type 18) from publisher to subscribers
      if (msgTypeId == 8 or msgTypeId == 9 or msgTypeId == 18) and payloadLen > 0 and payloadPtr != nil:
        # find stream entry where this conn is publisher
        for name, se in gStreams.pairs:
          var matchPub = false
          if se.publisher == c and se.publisherStreamId == msgStreamId:
            matchPub = true
          elif c.publishedStreamName.len > 0 and se.name == c.publishedStreamName and se.publisherStreamId == msgStreamId:
            # fallback: match by stream name if publisher pointer didn't line up
            matchPub = true
          
          if not matchPub: continue # not the stream we're publishing to

          # detect cache-worthy packets directly from payloadPtr (no copy yet)
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
            if s.conn == nil or s.conn.bev == nil or s.conn.closed:
              dropSubs.add(s)
              continue

            let output = bufferevent_get_output(s.conn.bev)
            if output == nil:
              dropSubs.add(s)
              continue

            let nowTick = epochMs()
            let outq = cast[int](evbuffer_get_length(output))

            # Force jump-to-live when queue is too large.
            if outq > MAX_SUBSCRIBER_OUTBUF:
              if (nowTick - s.conn.lastLiveJumpMs) >= LIVE_JUMP_COOLDOWN_MS:
                let qlen = evbuffer_get_length(output)
                if qlen > 0:
                  discard evbuffer_drain(output, qlen)
                s.conn.slowSubscriber = true
                s.conn.waitForKeyframe = true
                s.conn.lastLiveJumpMs = nowTick
              continue

            # Recover only on a keyframe and only when queue is low enough.
            if s.conn.slowSubscriber or s.conn.waitForKeyframe:
              if outq > SLOW_SUBSCRIBER_RESUME_OUTBUF:
                continue
              if msgTypeId != 9 or not isVideoKeyframe:
                continue

              if se.metaPayload.len > 0:
                discard sendRtmpMessage(s.conn, csid = 4, msgTypeId = 18, msgStreamId = s.msgStreamId, payload = se.metaPayload)
              if se.audioSeqPayload.len > 0:
                discard sendRtmpMessage(s.conn, csid = 4, msgTypeId = 8, msgStreamId = s.msgStreamId, payload = se.audioSeqPayload)
              if se.videoSeqPayload.len > 0:
                discard sendRtmpMessage(s.conn, csid = 4, msgTypeId = 9, msgStreamId = s.msgStreamId, payload = se.videoSeqPayload)

              s.conn.slowSubscriber = false
              s.conn.waitForKeyframe = false

            if not sendRtmpMessageShared(s.conn, 4, msgTypeId, s.msgStreamId, shared, int(timestamp)):
              dropSubs.add(s)
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

          # cache copy for future subscribers
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

    setOnMessage(conn.chunkCtx, onChunkMessage, cast[pointer](conn))
    bufferevent_setcb(bev, bev_read_cb, nil, bev_event_cb, cast[pointer](conn))

  let inbuf = bufferevent_get_input(bev)

  while true:
    let avail = cast[int](evbuffer_get_length(inbuf))
    if avail <= 0:
      return
    # echo "read-loop: conn=", $(cast[int](addr(conn))), " hsState=", conn.hsState, " avail=", avail
    if conn.hsState == HS_INIT:
      if avail < 1 + RTMP_HANDSHAKE_SIZE: return
      let p = evbuffer_pullup(inbuf, 1 + RTMP_HANDSHAKE_SIZE)
      if p == nil: return

      let pbytes = cast[ptr UncheckedArray[byte]](p)
      let c0 = pbytes[0]
      # Expect RTMP version 3 (0x03) in C0; reject otherwise
      if c0 != 0x03'u8:
        echo "Unsupported RTMP version: ", c0
        # Close connection
        if bev != nil:
          bufferevent_free(bev)
        return

      var outS0 = [byte 0x03]
      let s1 = buildServerS1()
      # store server S1 to validate C2 later
      conn.serverS1 = s1

      let c1ptr = addr pbytes[1] # C1

      assert bufferevent_write(bev, outS0[0].addr, 1) == 0
      assert bufferevent_write(bev, s1[0].addr, csize_t(s1.len)) == 0
      assert bufferevent_write(bev, c1ptr, csize_t(RTMP_HANDSHAKE_SIZE)) == 0 # S2 = echo C1
      assert evbuffer_drain(inbuf, 1 + csize_t(RTMP_HANDSHAKE_SIZE)) == 0

      conn.hsState = HS_S0S1_SENT
      continue

    if conn.hsState == HS_S0S1_SENT:
      if avail < RTMP_HANDSHAKE_SIZE: return
      let p2 = evbuffer_pullup(inbuf, RTMP_HANDSHAKE_SIZE)
      if p2 == nil: return
      let c2bytes = cast[ptr UncheckedArray[byte]](p2)
      # Validate C2 equals our S1 (common check); warn but continue if mismatch
      if conn.serverS1.len == RTMP_HANDSHAKE_SIZE:
        var match = true
        for i in 0 ..< RTMP_HANDSHAKE_SIZE:
          if conn.serverS1[i] != c2bytes[i]:
            match = false
            break
        if not match:
          echo "Warning: C2 did not match server S1 (handshake mismatch)"

      assert evbuffer_drain(inbuf, csize_t(RTMP_HANDSHAKE_SIZE)) == 0
      conn.hsState = HS_DONE
      # echo "RTMP handshake done"
      continue

    # HS_DONE: feed RTMP chunks (do NOT just drain)
    let p = evbuffer_pullup(inbuf, avail)
    if p == nil: return
    let consumed = feedBytes(conn.chunkCtx, cast[ptr byte](p), avail)
    if consumed <= 0:
      return
    discard evbuffer_drain(inbuf, csize_t(consumed))

# Accept callback: accept socket, wrap into bufferevent and set callbacks (ConnCtx will be attached lazily)
proc accept_cb(listenFd: cint, events: cshort, arg: pointer) {.cdecl.} =
  let base = cast[ptr event_base](arg)
  var clientAddr: SockAddr
  var addrLen = Socklen(sizeof(clientAddr))
  let clientFd = accept(SocketHandle(listenFd), addr(clientAddr), addrLen.addr)
  if clientFd.int < 0:
    return
  # echo "accept: fd=", clientFd.int
  setReuseAndNonblock(clientFd.cint)
  let bev = bufferevent_socket_new(base, clientFd.cint, BEV_OPT_CLOSE_ON_FREE)
  if bev == nil:
    discard close(SocketHandle(clientFd))
    return

  # Extract IP address as string
  var ipStr: array[32, char]
  ipStr[0] = '\0'
  let sa = cast[ptr Sockaddr_in](addr clientAddr)
  discard inet_ntop(AF_INET, addr sa.sin_addr, addr ipStr[0], int32(ipStr.len))
  # stash IP by bev pointer (cannot capture local in cdecl callback)
  gPendingClientIp[cast[pointer](bev)] = $cstring(addr ipStr[0])

  bufferevent_setcb(bev, bev_read_cb, nil, bev_event_cb, nil)
  discard bufferevent_enable(bev, EV_READ or EV_WRITE)

#
# REST API listener handle
#
proc bindApiListener(server: RTMPServer, port: int) =
  # Bind a REST API listener on a separate port.
  server.restApiHttp = evhttp_new(server.base)
  if server.restApiHttp == nil:
    raise newException(RTMPServerError, "Failed to create REST API http server")

  evhttp_set_gencb(server.restApiHttp, apiRequestCb, nil)

  let bound = evhttp_bind_socket_with_handle(server.restApiHttp, "0.0.0.0", uint16(port))
  if bound == nil:
    evhttp_free(server.restApiHttp)
    server.restApiHttp = nil
    raise newException(RTMPServerError, "Failed to bind REST API port " & $port)
  server.restApiListener = evhttp_bound_socket_get_listener(bound)


#
# Public API
#
proc newRTMPServer*: RTMPServer =
  new(result)
  result.base = event_base_new()
  if result.base == nil:
    raise newException(RTMPServerError,
          "Failed to create event base")
  # Bind REST API listener on separate port (default 8000)
  result.bindApiListener(8000)

proc startServer*(server: RTMPServer, port: Port = Port(DEFAULT_RTMP_PORT)) =
  ## Start RTMP server on specified port (default 1935)
  ## This is a blocking call that runs the event loop; it will not return
  ## until the server is stopped.
  var listenFd = socket(AF_INET, SOCK_STREAM, 0)
  if listenFd.int < 0:
    raise newException(RTMPServerError, "Failed to create socket")
  setReuseAndNonblock(listenFd.cint)
  var sockAddr: Sockaddr_in
  sockAddr.sin_family = AF_INET.uint8
  sockAddr.sin_port = htons(port.uint16)
  sockAddr.sin_addr.s_addr = inet_addr("0.0.0.0")

  if bindSocket(listenFd, cast[ptr SockAddr](addr sockAddr), SockLen(sizeof(sockAddr))) < 0:
    raise newException(RTMPServerError, "Failed to bind socket")
  
  if listen(listenFd, 128) < 0:
    raise newException(RTMPServerError, "Failed to listen on socket")  
  let ev = event_new(server.base, listenFd.cint, EV_READ or EV_PERSIST, accept_cb, server.base)
  if ev == nil:
    raise newException(RTMPServerError, "Failed to create event")
  discard event_add(ev, nil)
  discard event_base_dispatch(server.base)