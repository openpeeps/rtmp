# RTMP URL parsing (pure; no sockets opened).

import unittest
import rtmp/rtmpclient

suite "rtmp urls":
  test "basic publish address":
    let u = parseRtmpUrl("rtmp://example.com/live/streamkey")
    check u.scheme == "rtmp"
    check u.host == "example.com"
    check u.port == 1935
    check u.app == "live"
    check u.streamName == "streamkey"
    check u.tcUrl == "rtmp://example.com/live"

  test "rtmps defaults to 443":
    let u = parseRtmpUrl("rtmps://example.com/live/s")
    check u.scheme == "rtmps"
    check u.port == 443
    check u.tcUrl == "rtmps://example.com/live"

  test "explicit port":
    let u = parseRtmpUrl("rtmp://127.0.0.1:1940/live/s")
    check u.host == "127.0.0.1"
    check u.port == 1940
    check u.tcUrl == "rtmp://127.0.0.1/live" # port excluded by design

  test "app without stream":
    let u = parseRtmpUrl("rtmp://example.com/live")
    check u.app == "live"
    check u.streamName == ""

  test "host only":
    let u = parseRtmpUrl("rtmp://example.com")
    check u.app == ""
    check u.streamName == ""

  test "trailing slash keeps stream":
    let u = parseRtmpUrl("rtmp://example.com/live/s/")
    check u.app == "live"
    check u.streamName == "s"

  test "deep path pins first-two-segments rule":
    let u = parseRtmpUrl("rtmp://example.com/app/sub/stream")
    check u.app == "app"
    check u.streamName == "sub"

  test "non-rtmp scheme asserts":
    expect AssertionDefect:
      discard parseRtmpUrl("http://example.com/live/s")
