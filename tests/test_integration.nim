# Loopback integration: real server + publishing client over 127.0.0.1.
#
# Covers the exact replay regression (publisher loops across EOF while the
# connection stays up) plus publish-visibility via the REST monitor.
# Synthetic fixtures only; no ffmpeg/VLC. Runtime ~8s.

import unittest
import std/[json, os]
import std/httpclient as httpclient
import powpow
import rtmp
import helpers

const
  RtmpPort = 19350
  RestPort = 14000

type IntegState = ref object
  videoPath: string
  audioPath: string
  publishedOk: bool
  videoEnds: int
  audioEnds: int
  errors: seq[string]
  monitorSawStream: bool

var gState {.global.}: IntegState

proc runTestServer(ports: tuple[rtmp, rest: int]) {.thread.} =
  # All server state (gMonitor, connections) lives on this thread only;
  # the main thread never touches it, it only speaks HTTP over loopback.
  {.cast(gcsafe).}:
    var srv = newRTMPServer(RtmpServerSettings(
      rtmpPort: Port(ports.rtmp), restApiPort: Port(ports.rest)))
    srv.startServer() # blocks; thread is abandoned at process exit

proc startStreaming(c: RtmpClient) =
  startStreamFlvZeroCopy(c, gState.videoPath, c.msgStreamId,
    startTs = c.ps.globalTs)
  startStreamAacAdtsZeroCopy(c, gState.audioPath, c.msgStreamId, 4'u8,
    startTs = c.ps.globalTs)

suite "loopback publish and replay":
  test "publish, replay across EOF, monitor visibility":
    # Tiny fixtures: 4 FLV tags over 60ms, 6 ADTS frames (~128ms).
    var payload = @[0xAF'u8, 0x01, 0x02, 0x03]
    let videoPath = writeTempBytes("integ-video", ".flv", flvFile(0x05'u8, @[
      flvTag(0x08'u8, 0'u32, payload),
      flvTag(0x09'u8, 0'u32, @[0x17'u8, 0x01]),
      flvTag(0x08'u8, 23'u32, payload),
      flvTag(0x09'u8, 60'u32, @[0x27'u8, 0x01]),
    ]))
    var audioBlob: seq[byte] = @[]
    for i in 0 ..< 6:
      audioBlob.add adtsFrame(2, 4, 2, @[byte(i), 0xAA'u8])
    let audioPath = writeTempBytes("integ-audio", ".aac", audioBlob)

    gState = IntegState(videoPath: videoPath, audioPath: audioPath,
      errors: @[])

    var srvThread: Thread[tuple[rtmp, rest: int]]
    createThread(srvThread, runTestServer, (RtmpPort, RestPort))
    sleep(1000) # let listeners bind

    let client = newRtmpClient("rtmp://127.0.0.1:" & $RtmpPort &
      "/live/livestream")
    client.ps = PlaylistState()

    client.onPublishOk =
      proc(c: RtmpClient) =
        gState.publishedOk = true
        startPacer(c, proc(c2: RtmpClient) = startStreaming(c2))

    client.onStreamEnd =
      proc(c: RtmpClient, st: StreamState, sent: int) =
        if st.msgType == 0x09'u8:
          inc gState.videoEnds
          startStreamFlvZeroCopy(c, gState.videoPath, c.msgStreamId,
            startTs = c.ps.globalTs)
        elif st.msgType == 0x08'u8:
          inc gState.audioEnds
          startStreamAacAdtsZeroCopy(c, gState.audioPath, c.msgStreamId,
            4'u8, startTs = c.ps.globalTs)

    client.onError =
      proc(c: RtmpClient, msg: string) =
        gState.errors.add msg

    # t=3.5s: assert the monitor sees the live stream (blocking GET is
    # fine on loopback inside a timer callback).
    discard client.loop.addTimer(3500) do (id: int) {.closure.}:
      try:
        let body = httpclient.newHttpClient().getContent(
          "http://127.0.0.1:" & $RestPort & "/")
        let j = parseJson(body)
        gState.monitorSawStream =
          j.hasKey("streams") and j["streams"].hasKey("livestream") and
          j["streams"]["livestream"]["publisher"]["id"].getStr.len > 0
      except CatchableError:
        gState.monitorSawStream = false

    # t=7s: stop the client loop; server thread is abandoned at exit.
    discard client.loop.addTimer(7000) do (id: int) {.closure.}:
      client.loop.stop()

    client.loop.run()

    check gState.publishedOk == true
    check gState.errors.len == 0
    check gState.monitorSawStream == true
    # Short fixtures over a 7s window: each track must have ended AND
    # replayed at least once (the exact scenario that used to break).
    check gState.videoEnds >= 2
    check gState.audioEnds >= 2

    removeFile(videoPath)
    removeFile(audioPath)
