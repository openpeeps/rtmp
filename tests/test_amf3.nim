# AMF3 decoder (returns AMF0Value for interop).

import unittest
import std/tables
import rtmp/server/actionmessage

proc decode(data: seq[byte]): tuple[v: AMF0Value, idx: int] =
  var idx = 0
  (decodeOneAMF3(data, idx), idx)

suite "amf3 scalars":
  test "undefined null booleans":
    check decode(@[0x00'u8]).v.typ == AMF0_Undefined
    check decode(@[0x01'u8]).v.typ == AMF0_Null
    check decode(@[0x02'u8]).v.b == false
    check decode(@[0x03'u8]).v.b == true

  test "empty input is nil":
    check decode(@[]).v == nil

  test "u29 boundaries":
    check decode(@[0x04'u8, 0x7F]).v.num == 127.0
    check decode(@[0x04'u8, 0xFF, 0x7F]).v.num == 16383.0
    check decode(@[0x04'u8, 0xFF, 0xFF, 0x7F]).v.num == 2097151.0
    check decode(@[0x04'u8, 0xFF, 0xFF, 0xFF, 0x7F]).v.num == 536870783.0

  test "u29 consumes exact bytes":
    let (v, idx) = decode(@[0x04'u8, 0xFF, 0x7F, 0xAA])
    check v.num == 16383.0
    check idx == 3

  test "double 1.5":
    let (v, idx) = decode(@[0x05'u8, 0x3F, 0xF8, 0, 0, 0, 0, 0, 0])
    check v.num == 1.5
    check idx == 9

  test "string inline and empty":
    let (a, ia) = decode(@[0x06'u8, 0x05, byte('h'), byte('i')])
    check a.s == "hi"
    check ia == 4
    check decode(@[0x06'u8, 0x01]).v.s == ""
    check decode(@[0x06'u8, 0x00]).v.s == "" # reference, no content

  test "unknown marker becomes undefined":
    check decode(@[0x1F'u8]).v.typ == AMF0_Undefined

  test "KNOWN HARDENING: truncated integer raises (readU29 has no bounds check)":
    var idx = 0
    expect IndexDefect:
      discard decodeOneAMF3(@[0x04'u8], idx)

suite "amf3 containers":
  test "dynamic object with one member":
    # 08 | trait 07 (inline, dynamic, 0 sealed) | class "" | "x" | int 5 | end ""
    let data = @[0x08'u8, 0x07, 0x01, 0x03, byte('x'), 0x04, 0x05, 0x01]
    var idx = 0
    let v = decodeOneAMF3(data, idx)
    check v.typ == AMF0_Object
    check v.obj["x"].num == 5.0
    check idx == data.len

  test "dense array of two":
    # 09 | denseCount (2<<1)|1 | assoc-end "" | int 1 | int 2
    let data = @[0x09'u8, 0x05, 0x01, 0x04, 0x01, 0x04, 0x02]
    let v = decodeAllAMF3(data)
    check v.len == 1
    check v[0].obj["0"].num == 1.0
    check v[0].obj["1"].num == 2.0

  test "byte array is skipped":
    let (v, idx) = decode(@[0x0C'u8, 0x07, byte('a'), byte('b'), byte('c')])
    check v.typ == AMF0_Undefined
    check idx == 5

  test "decodeAll stops at truncation":
    check decodeAllAMF3(@[0x03'u8, 0x02]).len == 2
    check decodeAllAMF3(@[0x03'u8]).len == 1
