# AMF0 codec (server) + AMF0 put-helpers (client).

import unittest
import std/tables
import rtmp/server/actionmessage
import rtmp/rtmpclient

suite "amf0 constructors":
  test "discriminants and fields":
    check newNumber(1.5).typ == AMF0_Number
    check newNumber(1.5).num == 1.5
    check newBoolean(true).b == true
    check newString("hi").s == "hi"
    check newNull().typ == AMF0_Null
    check newUndefined().typ == AMF0_Undefined
    check newObject().obj.len == 0
    check newEcmaArray().obj.len == 0
    check newStrictArray(@[newNumber(1.0)]).arr.len == 1
    check amfObj([("a", newNumber(1.0))]).obj["a"].num == 1.0

suite "amf0 golden vectors":
  test "number 1.0":
    var outp: seq[byte] = @[]
    encodeOneAMF0(newNumber(1.0), outp)
    check outp == @[0x00'u8, 0x3F, 0xF0, 0, 0, 0, 0, 0, 0]

  test "string":
    var outp: seq[byte] = @[]
    encodeOneAMF0(newString("hi"), outp)
    check outp == @[0x02'u8, 0x00, 0x02, byte('h'), byte('i')]

  test "boolean null undefined":
    var outp: seq[byte] = @[]
    encodeOneAMF0(newBoolean(true), outp)
    encodeOneAMF0(newBoolean(false), outp)
    encodeOneAMF0(newNull(), outp)
    encodeOneAMF0(newUndefined(), outp)
    encodeOneAMF0(nil, outp)
    check outp == @[0x01'u8, 0x01, 0x01, 0x00, 0x05, 0x06, 0x06]

  test "strict array [1, 'a']":
    var outp: seq[byte] = @[]
    encodeOneAMF0(newStrictArray(@[newNumber(1.0), newString("a")]), outp)
    check outp == @[0x0A'u8, 0, 0, 0, 2,
      0x00, 0x3F, 0xF0, 0, 0, 0, 0, 0, 0,
      0x02, 0x00, 0x01, byte('a')]

  test "object end marker":
    var outp: seq[byte] = @[]
    encodeOneAMF0(amfObj([("a", newNumber(1.0))]), outp)
    check outp == @[0x03'u8, 0x00, 0x01, byte('a'),
      0x00, 0x3F, 0xF0, 0, 0, 0, 0, 0, 0,
      0x00, 0x00, 0x09]

  test "ecma array count + end marker":
    var outp: seq[byte] = @[]
    encodeOneAMF0(newEcmaArray(), outp)
    check outp == @[0x08'u8, 0, 0, 0, 0, 0x00, 0x00, 0x09]

  test "oversize string falls back to long-string marker":
    var big = newString(70000)
    for i in 0 ..< big.len: big[i] = 'x'
    var outp: seq[byte] = @[]
    encodeOneAMF0(newString(big), outp)
    check outp[0] == 0x0C'u8
    check outp[1 .. 4] == @[0x00'u8, 0x01, 0x11, 0x70] # 70000 BE
    check outp.len == 5 + 70000

suite "amf0 roundtrips":
  test "all scalar types":
    let vals = @[newNumber(-3.25), newBoolean(false), newString(""),
      newString("connect"), newNull(), newUndefined()]
    let enc = encodeAMF0Values(vals)
    let dec = decodeAllAMF0(enc)
    check dec.len == vals.len
    check dec[0].num == -3.25
    check dec[1].b == false
    check dec[2].s == ""
    check dec[3].s == "connect"
    check dec[4].typ == AMF0_Null
    check dec[5].typ == AMF0_Undefined

  test "nested object":
    let inner = amfObj([("n", newNumber(2.0))])
    let outer = amfObj([("app", newString("live")), ("cap", inner)])
    let dec = decodeAllAMF0(encodeAMF0Values(@[outer]))
    check dec.len == 1
    check dec[0].obj["app"].s == "live"
    check dec[0].obj["cap"].obj["n"].num == 2.0

  test "connect command shape":
    let cmd = encodeAMF0Values(@[newString("connect"), newNumber(1.0),
      amfObj([("app", newString("live")), ("tcUrl", newString("rtmp://h/live"))])])
    let dec = decodeAllAMF0(cmd)
    check dec.len == 3
    check dec[0].s == "connect"
    check dec[1].num == 1.0
    check dec[2].obj["app"].s == "live"

  test "decoded long string keeps content (type normalizes to String)":
    # The 0x0C decode branch builds via newString(), so the value comes
    # back typed AMF0_String; AMF0_LongString is never produced by decode.
    # Re-encode is content-correct (short marker here; 0x0C for >64K).
    var raw: seq[byte] = @[0x0C'u8, 0, 0, 0, 2, byte('a'), byte('b')]
    var idx = 0
    let v = decodeOneAMF0(raw, idx)
    check v.typ == AMF0_String
    check v.s == "ab"
    check idx == raw.len
    var outp: seq[byte] = @[]
    encodeOneAMF0(v, outp)
    check outp == @[0x02'u8, 0x00, 0x02, byte('a'), byte('b')]

suite "amf0 decode edges":
  test "empty input decodes to nothing":
    check decodeAllAMF0(@[]).len == 0
    var idx = 0
    check decodeOneAMF0(@[], idx) == nil

  test "unknown marker becomes undefined":
    var idx = 0
    let v = decodeOneAMF0(@[0xFF'u8], idx)
    check v != nil
    check v.typ == AMF0_Undefined

  test "KNOWN HARDENING: truncated number raises (seq path has no bounds check)":
    # ByteReader path returns nil for the same input (see below).
    # Filed as follow-up; do not "fix" by weakening this test.
    var idx = 0
    expect IndexDefect:
      discard decodeOneAMF0(@[0x00'u8, 0x3F], idx)

  test "KNOWN HARDENING: string length overrun raises":
    var idx = 0
    expect IndexDefect:
      discard decodeOneAMF0(@[0x02'u8, 0x00, 0x05, byte('h'), byte('i')], idx)

suite "amf0 ByteReader path":
  test "connect payload decodes":
    let cmd = encodeAMF0Values(@[newString("play"), newNumber(2.0),
      amfObj([("stream", newString("s"))])])
    let dec = decodeAllAMF0Ptr(cast[ptr byte](addr cmd[0]), cmd.len)
    check dec.len == 3
    check dec[0].s == "play"
    check dec[1].num == 2.0
    check dec[2].obj["stream"].s == "s"

  test "truncated number returns nil (safe path)":
    var data = @[0x00'u8, 0x3F]
    let dec = decodeAllAMF0Ptr(cast[ptr byte](addr data[0]), data.len)
    check dec.len == 0

  test "amf0ToStr":
    check amf0ToStr(newNumber(1.0)) == "1.0"
    check amf0ToStr(newBoolean(true)) == "true"
    check amf0ToStr(newString("x")) == "x"
    check amf0ToStr(newNull()) == "null"
    check amf0ToStr(newUndefined()) == "undefined"
    check amf0ToStr(newObject()) == "<complex>"

suite "client amf0 put-helpers":
  test "put string byte-exact":
    var p: seq[byte] = @[]
    amf0PutString("connect", p)
    check p == @[0x02'u8, 0x00, 0x07, byte('c'), byte('o'), byte('n'),
      byte('n'), byte('e'), byte('c'), byte('t')]

  test "put empty string":
    var p: seq[byte] = @[]
    amf0PutString("", p)
    check p == @[0x02'u8, 0x00, 0x00]

  test "put txid numbers":
    var p: seq[byte] = @[]
    amf0PutNumber(1, p)
    amf0PutNumber(2, p)
    amf0PutNumber(3, p)
    amf0PutNumber(99, p)
    check p == @[
      0x00'u8, 0x3F, 0xF0, 0, 0, 0, 0, 0, 0,
      0x00, 0x40, 0x00, 0, 0, 0, 0, 0, 0,
      0x00, 0x40, 0x08, 0, 0, 0, 0, 0, 0,
      0x00, 0, 0, 0, 0, 0, 0, 0, 0]

  test "put double bool null":
    var p: seq[byte] = @[]
    amf0PutDouble(15.0, p)
    amf0PutBool(true, p)
    amf0PutBool(false, p)
    amf0PutNull(p)
    check p == @[0x00'u8, 0x40, 0x2E, 0, 0, 0, 0, 0, 0,
      0x01, 0x01, 0x01, 0x00, 0x05]

  test "read string roundtrip + rejects":
    var p: seq[byte] = @[]
    amf0PutString("live", p)
    var i = 0
    check amf0ReadString(p, i) == "live"
    check i == p.len
    var j = 0
    check amf0ReadString(@[0x05'u8], j) == ""
    var k = 0
    check amf0ReadString(@[0x02'u8, 0x00], k) == ""

  test "read txid roundtrip":
    for tx in [1, 2, 3]:
      var p: seq[byte] = @[]
      amf0PutNumber(tx, p)
      var i = 0
      check amf0ReadNumberAsInt(p, i) == tx
    var i = 0
    check amf0ReadNumberAsInt(@[0x05'u8], i) == 0

  test "u32be":
    check u32be(@[0xDE'u8, 0xAD, 0xBE, 0xEF], 0) == 0xDEADBEEF'u32
    check u32be(@[0x00'u8, 0x00, 0x01, 0x00], 0) == 256'u32

  test "parseBasicHeader sizes":
    check parseBasicHeader(@[0x43'u8], 0) == (1'u8, 3'u32, 1, true)
    check parseBasicHeader(@[0x00'u8, 0x05], 0) == (0'u8, 69'u32, 2, true)
    check parseBasicHeader(@[0xC1'u8, 0x10, 0x01], 0) == (3'u8, 336'u32, 3, true)
    check parseBasicHeader(@[], 0) == (0'u8, 0'u32, 0, false)
    check parseBasicHeader(@[0x00'u8], 0) == (0'u8, 0'u32, 0, false)
    check parseBasicHeader(@[0xC1'u8, 0x10], 0) == (3'u8, 0'u32, 0, false)
