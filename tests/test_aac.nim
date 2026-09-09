# AAC helpers: ASC construction, sample-rate table, ADTS parsing.

import unittest
import std/[os, posix]
import rtmp/rtmpclient
import helpers

proc openTmp(data: seq[byte]): tuple[fd: cint, path: string] =
  let path = writeTempBytes("aac", ".aac", data)
  (posix.open(path.cstring, O_RDONLY), path)

suite "aac asc":
  test "aac-lc 44.1k stereo is 1210":
    check buildAacAsc(2, 4, 2) == @[0x12'u8, 0x10'u8]

  test "aac-lc 48k mono is 1188":
    check buildAacAsc(2, 3, 1) == @[0x11'u8, 0x88'u8]

  test "he-aac 22k stereo":
    # profile 5, idx 7 (22050), ch 2: (5<<11)|(7<<7)|(2<<3) = 0x2B90
    check buildAacAsc(5, 7, 2) == @[0x2B'u8, 0x90'u8]

suite "sample rate table":
  test "all indices":
    let expected = [96000, 88200, 64000, 48000, 44100, 32000, 24000,
      22050, 16000, 12000, 11025, 8000, 7350]
    for i, sr in expected:
      check samplingRateFromIndex(i) == sr

  test "out of range falls back to 44100":
    check samplingRateFromIndex(-1) == 44100
    check samplingRateFromIndex(13) == 44100
    check samplingRateFromIndex(99) == 44100

suite "adts parsing":
  test "frame roundtrip":
    var rawPayload = newSeq[byte](100)
    for i in 0 ..< 100: rawPayload[i] = byte(i)
    let (fd, path) = openTmp(adtsFrame(2, 4, 2, rawPayload))
    try:
      var frameLen, headerLen, profile, sfIdx, ch: int
      check parseAdts(fd, 0, frameLen, headerLen, profile, sfIdx, ch) == true
      check profile == 2
      check sfIdx == 4
      check ch == 2
      check frameLen == 107
      check headerLen == 7
    finally:
      discard posix.close(fd)
      removeFile(path)

  test "second frame offset":
    let f = adtsFrame(2, 4, 2, newSeq[byte](50))
    var blob: seq[byte] = @[]
    blob.add f
    blob.add adtsFrame(2, 4, 2, newSeq[byte](20))
    let (fd, path) = openTmp(blob)
    try:
      var frameLen, headerLen, profile, sfIdx, ch: int
      check parseAdts(fd, 0, frameLen, headerLen, profile, sfIdx, ch) == true
      check frameLen == 57
      check parseAdts(fd, 57, frameLen, headerLen, profile, sfIdx, ch) == true
      check frameLen == 27
    finally:
      discard posix.close(fd)
      removeFile(path)

  test "bad sync fails":
    let (fd, path) = openTmp(@[0x00'u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00])
    try:
      var frameLen, headerLen, profile, sfIdx, ch: int
      check parseAdts(fd, 0, frameLen, headerLen, profile, sfIdx, ch) == false
    finally:
      discard posix.close(fd)
      removeFile(path)

  test "truncated header fails":
    let (fd, path) = openTmp(@[0xFF'u8, 0xF1, 0x50])
    try:
      var frameLen, headerLen, profile, sfIdx, ch: int
      check parseAdts(fd, 0, frameLen, headerLen, profile, sfIdx, ch) == false
    finally:
      discard posix.close(fd)
      removeFile(path)
