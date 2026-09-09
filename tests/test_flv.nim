# FLV container parsing (fd-based readers) + tag mapping.

import unittest
import std/[os, posix]
import rtmp/rtmpclient
import helpers

proc openTmp(data: seq[byte]): tuple[fd: cint, path: string] =
  let path = writeTempBytes("flv", ".flv", data)
  (posix.open(path.cstring, O_RDONLY), path)

template withFlv(data: seq[byte], body: untyped) =
  let (fd {.inject.}, path {.inject.}) = openTmp(data)
  try:
    body
  finally:
    discard posix.close(fd)
    removeFile(path)

suite "flv header":
  test "valid header returns first-tag offset":
    withFlv(flvFile(0x05'u8, @[])):
      check readFlvHeader(fd) == 13

  test "dataOffset is honored":
    var raw = @[byte('F'), byte('L'), byte('V'), 0x01'u8, 0x05'u8,
      0x00'u8, 0x00'u8, 0x00'u8, 0x0D'u8]
    withFlv(raw):
      check readFlvHeader(fd) == 17

  test "bad magic fails":
    withFlv(@[byte('X'), byte('L'), byte('V'), 0x01'u8, 0x05'u8,
        0x00'u8, 0x00'u8, 0x00'u8, 0x09'u8]):
      check readFlvHeader(fd) == -1

  test "truncated header fails":
    withFlv(@[byte('F'), byte('L')]):
      check readFlvHeader(fd) == -1

suite "flv tags":
  test "audio video script headers":
    let tags = @[
      flvTag(0x08'u8, 0'u32, @[0xAF'u8, 0x01, 0x02]),
      flvTag(0x09'u8, 40'u32, @[0x17'u8, 0x01]),
      flvTag(0x12'u8, 0'u32, @[0x02'u8])
    ]
    withFlv(flvFile(0x05'u8, tags)):
      var th: FlvTagHeader
      check readFlvTagHeader(fd, 13, th) == true
      check th.tagType == 0x08'u8
      check th.dataSize == 3
      check th.timestamp == 0'u32
      check th.posPayload == 24
      let second = 13 + 11 + 3 + 4
      check readFlvTagHeader(fd, second, th) == true
      check th.tagType == 0x09'u8
      check th.dataSize == 2
      check th.timestamp == 40'u32
      let third = second + 11 + 2 + 4
      check readFlvTagHeader(fd, third, th) == true
      check th.tagType == 0x12'u8

  test "extended timestamp reconstructs":
    let big = 0x1FFFFFF'u32
    withFlv(flvFile(0x01'u8, @[flvTag(0x09'u8, big, @[0x17'u8])])):
      var th: FlvTagHeader
      check readFlvTagHeader(fd, 13, th) == true
      check th.timestamp == big
      var ts: uint32 = 0
      check peekNextFlvTagTs(fd, 13, ts) == true
      check ts == big

  test "peek matches header read":
    withFlv(flvFile(0x05'u8, @[flvTag(0x08'u8, 1234'u32, @[1'u8, 2'u8])])):
      var ts: uint32 = 0
      check peekNextFlvTagTs(fd, 13, ts) == true
      check ts == 1234'u32

  test "past-eof reads fail cleanly":
    withFlv(flvFile(0x05'u8, @[])):
      var th: FlvTagHeader
      check readFlvTagHeader(fd, 13, th) == false
      var ts: uint32 = 0
      check peekNextFlvTagTs(fd, 9999, ts) == false

suite "flv tag mapping":
  test "known types":
    check flvTagToRtmp(0x08'u8) == (0x08'u8, 4'u8)
    check flvTagToRtmp(0x09'u8) == (0x09'u8, 6'u8)
    check flvTagToRtmp(0x12'u8) == (0x12'u8, 5'u8)

  test "unknown defaults to script csid":
    check flvTagToRtmp(0xFF'u8) == (0x12'u8, 5'u8)
    check flvTagToRtmp(0x00'u8) == (0x12'u8, 5'u8)
