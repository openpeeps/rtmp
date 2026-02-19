# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

import std/[posix, times, strutils, tables]
import pkg/libevent/bindings/[event, bufferevent, buffer]

import ./server/actionmessage
import ./server/chunkstream

const
  DEFAULT_RTMP_PORT* = 1935
  RTMP_HANDSHAKE_SIZE* = 1536

# RTMP state / types
const
  HS_INIT* = 0
  HS_S0S1_SENT* = 1
  HS_DONE* = 2

  RTMP_DEFAULT_CHUNK_SIZE* = 128
  RTMP_MAX_CHUNK_SIZE* = 65536

type
  ServerCtx* = ref object
    base*: ptr event_base
    listenFd*: cint

  RtmpConnState* = object
    peerChunkSize*: int
    localChunkSize*: int
    windowAckSize*: uint32
    bytesReceivedSinceAck*: uint64
    streams*: Table[int, pointer] # placeholder

  ConnCtx* = ref object
    bev*: ptr bufferevent
    state*: RtmpConnState
    inbuf*: ptr Evbuffer
    outbuf*: ptr Evbuffer
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
    closed*: bool
    closeReason*: string
    # publish/subscription bookkeeping
    publishedStreamName*: string
    publishedStreamId*: int
    subscriptions*: Table[string, int]
  
  RTMPServerError* = object of CatchableError

proc sendAmfCommand(conn: ConnCtx; msgStreamId: int; vals: seq[AMF0Value])
proc sendRtmpMessage(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; payload: seq[byte]; timestamp: int = 0): bool
proc removeSubscriber(name: string, conn: ConnCtx)

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

# Keep ConnCtx refs alive while C/libevent only holds a raw pointer.
var gConnKeepAlive = initTable[pointer, ConnCtx]()
var gNextConnId = 1

# Stream pub/sub registry
type
  SubscriberEntry* = object
    conn*: ConnCtx
    msgStreamId*: int

  StreamEntry* = ref object
    name*: string
    publisher*: ConnCtx
    publisherStreamId*: int
    subscribers*: seq[SubscriberEntry]
    # cached payloads to send to new subscribers
    metaPayload*: seq[byte]
    videoSeqPayload*: seq[byte]
    audioSeqPayload*: seq[byte]

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

  # echo "addPublisher: name=", name, " conn=", $(cast[int](addr(conn))), " pubStreamId=", pubStreamId
  if gStreams.hasKey(name):
    let tse = gStreams[name]
    # echo "  gStreams entry: publisher=", $(cast[int](addr(tse.publisher))), " publisherStreamId=", tse.publisherStreamId, " subscribers=", tse.subscribers.len

  # clear previous cached seq headers when a new publisher arrives
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
  # echo "addSubscriber: name=", name, " conn=", $(cast[int](addr(conn))), " subStreamId=", subStreamId, " totalSubs=", se.subscribers.len
  # echo "  stream entry publisher=", $(cast[int](addr(se.publisher))), " publisherStreamId=", se.publisherStreamId

  # send cached meta and sequence headers (if any) so the new subscriber can initialize decoders
  if se.metaPayload.len > 0:
    # echo "Sending cached metaPayload to new subscriber msgStreamId=", subStreamId, " len=", se.metaPayload.len
    if not sendRtmpMessage(conn, csid = 4, msgTypeId = 18, msgStreamId = subStreamId, payload = se.metaPayload):
      removeSubscriber(name, conn)
      return
  if se.videoSeqPayload.len > 0:
    # echo "Sending cached videoSeqPayload to new subscriber msgStreamId=", subStreamId, " len=", se.videoSeqPayload.len
    if not sendRtmpMessage(conn, csid = 4, msgTypeId = 9, msgStreamId = subStreamId, payload = se.videoSeqPayload):
      removeSubscriber(name, conn)
      return
  if se.audioSeqPayload.len > 0:
    # echo "Sending cached audioSeqPayload to new subscriber msgStreamId=", subStreamId, " len=", se.audioSeqPayload.len
    if not sendRtmpMessage(conn, csid = 4, msgTypeId = 8, msgStreamId = subStreamId, payload = se.audioSeqPayload):
      removeSubscriber(name, conn)
      return

proc removeSubscriber(name: string, conn: ConnCtx) =
  if name.len == 0 or conn == nil: return
  let se = gStreams.getOrDefault(name, nil)
  if se == nil: return
  var o: seq[SubscriberEntry] = @[]
  for s in se.subscribers:
    if s.conn != conn:
      o.add(s)
  se.subscribers = o
  if conn.subscriptions.hasKey(name): conn.subscriptions.del(name)

proc removePublisher(name: string) =
  if name.len == 0: return
  if gStreams.hasKey(name):
    let se = gStreams[name]
    # notify subscribers that stream ended
    for s in se.subscribers:
      if s.conn != nil:
        let info = amfObj({
          "level": newString("status"),
          "code": newString("NetStream.Play.Stop"),
          "description": newString("Stream ended")
        })
        sendAmfCommand(s.conn, s.msgStreamId, @[ newString("onStatus"), newNumber(0.0), newNull(), info ])
    gStreams.del(name)

proc cleanupConn(conn: ConnCtx) =
  if conn == nil: return
  if conn.closed: return
  conn.closed = true
  if conn.closeReason.len == 0:
    conn.closeReason = "cleanup"
  if conn.publishedStreamName.len > 0:
    removePublisher(conn.publishedStreamName)
    conn.publishedStreamName = ""
  # remove conn from any subscriber lists
  for name, sid in conn.subscriptions.pairs:
    removeSubscriber(name, conn)
  conn.subscriptions = initTable[string, int]()

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

# Event callback: ensure ConnCtx is freed when connection closes/errors
proc bev_event_cb(bev: ptr bufferevent, what: cshort, ctx: pointer) {.cdecl.} =
  let cbarg = ctx

  echo "bev_event: conn=", $(cast[int](cbarg)), " what=", what,
       " flags=",(if (what and BEV_EVENT_CONNECTED) != 0: " CONNECTED" else: ""),
       (if (what and BEV_EVENT_EOF) != 0: " EOF" else: ""),
       (if (what and BEV_EVENT_ERROR) != 0: " ERROR" else: ""),
       (if (what and BEV_EVENT_TIMEOUT) != 0: " TIMEOUT" else: "")
  if cbarg != nil and gConnKeepAlive.hasKey(cbarg):
    let conn = gConnKeepAlive[cbarg]
    echo "  ", connSummary(conn), " hsState=", conn.hsState, " closed=", conn.closed

  if (what and BEV_EVENT_CONNECTED) != 0:
    echo "Connection established"

  if (what and BEV_EVENT_EOF) != 0:
    echo "Connection closed (EOF)"
    if cbarg != nil:
      if gConnKeepAlive.hasKey(cbarg):
        let conn = gConnKeepAlive[cbarg]
        conn.closed = true
        conn.closeReason = "EOF"
        cleanupConn(conn)
        conn.bev = nil
        conn.inbuf = nil
        conn.outbuf = nil
        gConnKeepAlive.del(cbarg)
    if bev != nil:
      bufferevent_free(bev)

  if (what and BEV_EVENT_ERROR) != 0:
    echo "Connection error"
    if cbarg != nil:
      if gConnKeepAlive.hasKey(cbarg):
        let conn = gConnKeepAlive[cbarg]
        conn.closed = true
        conn.closeReason = "ERROR"
        cleanupConn(conn)
        conn.bev = nil
        conn.inbuf = nil
        conn.outbuf = nil
        gConnKeepAlive.del(cbarg)
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

proc sendRtmpMessage(conn: ConnCtx; csid: int; msgTypeId: int; msgStreamId: int; payload: seq[byte]; timestamp: int = 0): bool =
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
  # echo "sendRtmpMessage: csid=", csid, " type=", msgTypeId, " stream=", msgStreamId, " payloadLen=", payload.len
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
  1.0

# 
# Read callback
# 
proc bev_read_cb(bev: ptr bufferevent, ctx: pointer) {.cdecl.} =
  var conn = cast[ConnCtx](ctx)
  if conn == nil:
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
      closed: false,
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
          sendAmfCommand(c, 0, @[ newString("_result"), newNumber(1.0), newNull() ])
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
          let info = amfObj({
            "level": newString("status"),
            "code": newString("NetConnection.Connect.Success"),
            "description": newString("Connection succeeded."),
            "objectEncoding": newNumber(0.0)
          })

          sendAmfCommand(c, 0, @[ newString("_result"), newNumber(txn), props, info ])
          return

        if cmd == "releaseStream" or cmd == "FCPublish":
          # Many encoders send these; respond with _result to unblock them.
          sendAmfCommand(c, 0, @[ newString("_result"), newNumber(txn), newNull(), newNull() ])
          return

        if cmd == "createStream":
          let sid = c.nextStreamId
          c.nextStreamId.inc
          # echo "createStream -> assigning sid=", sid, " txn=", txn
          sendAmfCommand(c, 0, @[ newString("_result"), newNumber(txn), newNull(), newNumber(float64(sid)) ])
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
          sendAmfCommand(c, streamId, @[ newString("onStatus"), newNumber(0.0), newNull(), info ])
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
            sendAmfCommand(c, sid, @[ newString("onStatus"), newNumber(0.0), newNull(), nf ])
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
          sendAmfCommand(c, sid, @[ newString("onStatus"), newNumber(0.0), newNull(), resetInfo ])

          let startInfo = amfObj({
            "level": newString("status"),
            "code": newString("NetStream.Play.Start"),
            "description": newString("Started playing."),
            "details": newString(streamName)
          })
          sendAmfCommand(c, sid, @[ newString("onStatus"), newNumber(0.0), newNull(), startInfo ])
          return

        if cmd == "getStreamLength":
          # Some players query stream length before playing; reply with a numeric length (0.0 = unknown)
          sendAmfCommand(c, 0, @[ newString("_result"), newNumber(txn), newNull(), newNumber(0.0) ])
          return

        if cmd == "closeStream":
          # closeStream: reply with _result if txn provided and cleanup subscriptions
          sendAmfCommand(c, msgStreamId, @[ newString("_result"), newNumber(txn), newNull() ])
          removeSubscriptionsByStreamId(c, msgStreamId)
          if c.publishedStreamId == msgStreamId and c.publishedStreamName.len > 0:
            removePublisher(c.publishedStreamName)
            c.publishedStreamName = ""
            c.publishedStreamId = 0
          return

        if cmd == "deleteStream":
          # deleteStream: reply with _result and cleanup
          sendAmfCommand(c, msgStreamId, @[ newString("_result"), newNumber(txn), newNull() ])
          removeSubscriptionsByStreamId(c, msgStreamId)
          if c.publishedStreamId == msgStreamId and c.publishedStreamName.len > 0:
            removePublisher(c.publishedStreamName)
            c.publishedStreamName = ""
            c.publishedStreamId = 0
          return

        if cmd == "FCUnpublish" or cmd == "unpublish":
          # explicit unpublish request from publisher
          if c.publishedStreamName.len > 0:
            removePublisher(c.publishedStreamName)
            c.publishedStreamName = ""
            c.publishedStreamId = 0
          sendAmfCommand(c, msgStreamId, @[ newString("_result"), newNumber(txn), newNull() ])
          return

        if not isDataMsg:
          return

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
          if not matchPub:
            continue
          var payload: seq[byte] = newSeq[byte](payloadLen)
          let src = cast[ptr UncheckedArray[byte]](payloadPtr)
          for i in 0 ..< payloadLen:
            payload[i] = src[i]
          # echo "Forwarding msgTypeId=", msgTypeId, " stream=", name, " len=", payload.len, " subs=", se.subscribers.len
          var dropSubs: seq[SubscriberEntry] = @[]
          for s in se.subscribers:
            if s.conn == nil or s.conn.bev == nil or s.conn.closed:
              dropSubs.add(s)
              continue
            # Use CSID 4 for forwarded messages; preserve subscriber's message stream id
            if not sendRtmpMessage(s.conn, csid = 4, msgTypeId = msgTypeId, msgStreamId = s.msgStreamId, payload = payload, timestamp = int(timestamp)):
              dropSubs.add(s)
              continue
            # echo "  -> forwarded to subscriber conn=", $(cast[int](addr(s.conn))), " msgStreamId=", s.msgStreamId, " ts=", timestamp
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
          
          # if se.subscribers.len == 0:
          #   echo "  (no subscribers for stream=", name, ")"

          # Cache metadata and sequence headers from publisher
          if msgTypeId == 18:
            se.metaPayload = payload
          elif msgTypeId == 9:
            # Video: detect AVC sequence header (AVCPacketType == 0)
            if payload.len >= 2:
              let first = payload[0]
              let codecId = first and 0x0F'u8
              if codecId == 7'u8 and payload[1] == 0'u8:
                se.videoSeqPayload = payload
          elif msgTypeId == 8:
            # Audio: detect AAC sequence header (AACPacketType == 0)
            if payload.len >= 2:
              let soundFormat = (payload[0] shr 4) and 0x0F
              if soundFormat == 10 and payload[1] == 0'u8:
                se.audioSeqPayload = payload
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
  # initially attach with nil cbarg; bev_read_cb will create and reattach ConnCtx on first read
  bufferevent_setcb(bev, bev_read_cb, nil, bev_event_cb, nil)
  discard bufferevent_enable(bev, EV_READ or EV_WRITE)

proc startServer*(port: int = DEFAULT_RTMP_PORT) =
  ## Start RTMP server on specified port (default 1935)
  # todo find a better name for this proc; it's really "setupListenerAndEventLoop" or
  # something, but startServer is more concise for now
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

  let base = event_base_new()
  if base == nil:
    raise newException(RTMPServerError, "Failed to create event base")

  let ev = event_new(base, listenFd.cint, EV_READ or EV_PERSIST, accept_cb, base)
  if ev == nil:
    raise newException(RTMPServerError, "Failed to create event")
  discard event_add(ev, nil)
  discard event_base_dispatch(base)

when isMainModule:
  startServer()
