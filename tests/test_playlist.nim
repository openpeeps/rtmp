# Playlist cycling and file loading.

import unittest
import std/[algorithm, os]
import rtmp/rtmpplaylist
import helpers

suite "playlist cycling":
  test "video loops":
    var ps = PlaylistState(videoFiles: @["a", "b", "c"],
      audioFiles: newSeq[string]())
    check nextVideo(ps) == "a"
    check nextVideo(ps) == "b"
    check nextVideo(ps) == "c"
    check nextVideo(ps) == "a"

  test "audio loops independently":
    var ps = PlaylistState(videoFiles: newSeq[string](),
      audioFiles: @["x", "y"])
    check nextAudio(ps) == "x"
    check nextAudio(ps) == "y"
    check nextAudio(ps) == "x"

  test "empty returns empty":
    var ps = PlaylistState(videoFiles: newSeq[string](),
      audioFiles: newSeq[string]())
    check nextVideo(ps) == ""
    check nextAudio(ps) == ""

  test "mid-state index wraps":
    var ps = PlaylistState(videoFiles: @["a", "b"],
      audioFiles: newSeq[string](), videoIdx: 5)
    check nextVideo(ps) == "a"
    check nextVideo(ps) == "b"

suite "playlist files":
  test "missing file is empty":
    check loadPlaylist("/nonexistent/rtmp-test-playlist.txt") == newSeq[string]()

  test "blank lines filtered, order shuffled":
    var lines = @["one", "", "two", "", "three"]
    var s = ""
    for l in lines: s.add l & "\n"
    var raw = newSeq[byte](s.len)
    for i, c in s: raw[i] = byte(c)
    let path = writeTempBytes("playlist", ".txt", raw)
    try:
      let got = loadPlaylist(path)
      check sorted(got) == @["one", "three", "two"]
    finally:
      removeFile(path)
