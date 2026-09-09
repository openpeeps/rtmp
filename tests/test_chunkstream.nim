# Chunk-stream assembler: headers, splits, deltas, csids, partial feeds.

import unittest
import rtmp/server/chunkstream
import helpers

type Msg = object
  typeId: int
  streamId: int
  ts: uint32
  payload: seq[byte]

proc feedAll(ctx: ChunkStreamCtx, data: seq[byte]): int =
  if data.len == 0: feedBytes(ctx, nil, 0)
  else: feedBytes(ctx, cast[ptr byte](addr data[0]), data.len)

type
  Collector = ref object
    ## Heap holder so the message callback closure can capture it.
    msgs: seq[Msg]

proc collector(col: Collector): ChunkMessageCb =
  result = proc(msgTypeId: int, msgStreamId: int, timestamp: uint32,
                payloadPtr: ptr byte, payloadLen: int,
                arg: pointer) {.closure.} =
    var p = newSeq[byte](payloadLen)
    if payloadLen > 0:
      copyMem(addr p[0], payloadPtr, payloadLen)
    col.msgs.add Msg(typeId: msgTypeId, streamId: msgStreamId,
      ts: timestamp, payload: p)

proc newCtx(col: Collector, chunkSize = 128): ChunkStreamCtx =
  result = initChunkStreamCtx(chunkSize)
  result.setOnMessage(collector(col), nil)

suite "chunk fmt0":
  test "single-chunk message":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    let payload = @[byte('h'), byte('e'), byte('l'), byte('l'), byte('o')]
    let frame = chunkMessage(0, 3, 0'u32, 5, 0x14'u8, 1'u32, payload)
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 1
    check msgs.msgs[0].typeId == 0x14
    check msgs.msgs[0].streamId == 1
    check msgs.msgs[0].ts == 0'u32
    check msgs.msgs[0].payload == payload

  test "message split across chunks reassembles":
    var msgs = Collector()
    let ctx = newCtx(msgs, 4)
    var payload = newSeq[byte](10)
    for i in 0 ..< 10: payload[i] = byte(i)
    let frame = chunkMessage(0, 3, 100'u32, 10, 0x08'u8, 1'u32, payload, 4)
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 1
    check msgs.msgs[0].ts == 100'u32
    check msgs.msgs[0].payload == payload

  test "byte-at-a-time feeding with caller staging":
    # feedBytes never buffers partial headers (the server stages via
    # gStaging); the caller must retain unconsumed bytes and re-feed them.
    var msgs = Collector()
    let ctx = newCtx(msgs, 4)
    var payload = newSeq[byte](10)
    for i in 0 ..< 10: payload[i] = byte(0xA0 + i)
    let frame = chunkMessage(0, 6, 7'u32, 10, 0x09'u8, 2'u32, payload, 4)
    var pending: seq[byte] = @[]
    for i in 0 ..< frame.len:
      pending.add frame[i]
      let n = feedAll(ctx, pending)
      pending = if n >= pending.len: @[] else: pending[n .. ^1]
    check pending.len == 0
    check msgs.msgs.len == 1
    check msgs.msgs[0].payload == payload
    check msgs.msgs[0].streamId == 2

  test "partial header is not consumed":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    let payload = @[byte('x')]
    let frame = chunkMessage(0, 3, 0'u32, 1, 0x14'u8, 1'u32, payload)
    check feedAll(ctx, frame[0 ..< 3]) == 0
    check msgs.msgs.len == 0
    # Split AFTER the 12-byte header: header consumed, payload staged.
    check feedAll(ctx, frame[0 ..< 12]) == 12
    check msgs.msgs.len == 0
    check feedAll(ctx, frame[12 .. ^1]) == frame.len - 12
    check msgs.msgs.len == 1
    check msgs.msgs[0].payload == payload

  test "zero-length message fires no callback (known quirk)":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    let frame = chunkMessage(0, 3, 0'u32, 0, 0x14'u8, 1'u32, @[])
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 0

suite "chunk fmt1 fmt2 fmt3":
  test "fmt1 advances timestamp by delta":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    discard feedAll(ctx, chunkMessage(0, 3, 1000'u32, 2, 0x08'u8, 1'u32,
      @[1'u8, 2'u8]))
    discard feedAll(ctx, chunkMessage(1, 3, 20'u32, 2, 0x08'u8, 0'u32,
      @[3'u8, 4'u8]))
    check msgs.msgs.len == 2
    check msgs.msgs[1].ts == 1020'u32
    check msgs.msgs[1].payload == @[3'u8, 4'u8]

  test "fmt2 advances timestamp, keeps length":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    discard feedAll(ctx, chunkMessage(0, 3, 500'u32, 2, 0x09'u8, 1'u32,
      @[1'u8, 2'u8]))
    discard feedAll(ctx, chunkMessage(2, 3, 10'u32, 0, 0'u8, 0'u32,
      @[3'u8, 4'u8]))
    check msgs.msgs.len == 2
    check msgs.msgs[1].ts == 510'u32
    check msgs.msgs[1].typeId == 0x09

  test "fmt3 continuation of long message":
    var msgs = Collector()
    let ctx = newCtx(msgs, 3)
    var payload = newSeq[byte](9)
    for i in 0 ..< 9: payload[i] = byte(i + 1)
    let frame = chunkMessage(0, 3, 33'u32, 9, 0x09'u8, 1'u32, payload, 3)
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 1
    check msgs.msgs[0].payload == payload

suite "chunk extended csid and timestamp":
  test "two-byte csid":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    let frame = chunkMessage(0, 70, 5'u32, 1, 0x14'u8, 1'u32, @[9'u8])
    check frame[0] == 0x00'u8
    check frame[1] == 0x06'u8
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 1
    check msgs.msgs[0].payload == @[9'u8]

  test "three-byte csid":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    let frame = chunkMessage(0, 400, 5'u32, 1, 0x14'u8, 1'u32, @[9'u8])
    check frame[0] == 0x01'u8
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 1

  test "extended timestamp":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    let big = 0x1234567'u32
    let frame = chunkMessage(0, 3, big, 1, 0x08'u8, 1'u32, @[7'u8])
    check frame[1 .. 3] == @[0xFF'u8, 0xFF, 0xFF]
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 1
    check msgs.msgs[0].ts == big

suite "chunk multi-stream":
  test "interleaved csids stay separate":
    var msgs = Collector()
    let ctx = newCtx(msgs, 2)
    let a = chunkMessage(0, 3, 10'u32, 4, 0x08'u8, 1'u32,
      @[1'u8, 2'u8, 3'u8, 4'u8], 2)
    let b = chunkMessage(0, 4, 20'u32, 2, 0x09'u8, 1'u32, @[5'u8, 6'u8], 2)
    # a = 12-byte header + 2 payload + fmt3 basic + 2 payload (17 bytes).
    # Split a on its chunk boundary, run b in between.
    check feedAll(ctx, a[0 ..< 14]) == 14
    check msgs.msgs.len == 0
    check feedAll(ctx, b) == b.len
    check msgs.msgs.len == 1
    check msgs.msgs[0].typeId == 0x09
    check feedAll(ctx, a[14 .. ^1]) == a.len - 14
    check msgs.msgs.len == 2
    check msgs.msgs[1].payload == @[1'u8, 2'u8, 3'u8, 4'u8]

  test "peer chunk size change mid-stream":
    var msgs = Collector()
    let ctx = newCtx(msgs, 128)
    ctx.setPeerChunkSize(2)
    var payload = newSeq[byte](5)
    for i in 0 ..< 5: payload[i] = byte(i)
    let frame = chunkMessage(0, 3, 0'u32, 5, 0x08'u8, 1'u32, payload, 2)
    check feedAll(ctx, frame) == frame.len
    check msgs.msgs.len == 1
    check msgs.msgs[0].payload == payload

  test "nil and empty feeds are free":
    var msgs = Collector()
    let ctx = newCtx(msgs)
    check feedBytes(ctx, nil, 0) == 0
    check feedAll(ctx, @[]) == 0
    check msgs.msgs.len == 0
