<p align="center">
  RTMP Client & Server - Real-Time Messaging Protocol for Nim 👑 
</p>

<p align="center">
  <code>nimble install rtmp</code>
</p>

<p align="center">
  <a href="https://github.com/">API reference</a><br>
  <img src="https://github.com/openpeeps/rtmp/workflows/test/badge.svg" alt="Github Actions">  <img src="https://github.com/openpeeps/rtmp/workflows/docs/badge.svg" alt="Github Actions">
</p>

## 😍 Key Features
- Built on [powpow](https://github.com/openpeeps/powpow) for high-performance, zero-dependency networking (kqueue / epoll / IOCP / io_uring)
- RTMP Client for publishing live streams to RTMP servers
- RTMP Server for accepting RTMP client connections with pub/sub fanout
- Enhanced handshake (HMAC-SHA256) for interoperability with SRS, nginx-rtmp and CDN servers — automatic fallback to plain handshake
- RTMPS support (RTMP over TLS) via `rtmps://` URLs
- AMF0 codec with AMF3 decoding fallback
- Zero-copy file streaming (`sendfile`) for efficient media delivery from disk
- Real-time pacing engine that streams FLV/AAC files at wall-clock rate
- REST API for server monitoring (active streams, publishers, subscribers)
- Flexible callback API for handling RTMP events
- Live ingest support for streaming from livecam/microphone (SOON)

## Requirements
- Nim >= 2.2.0
- [powpow](https://github.com/openpeeps/powpow) — the zero-copy file streaming API used here (`Connection.sendFile` with `keepOpen`) lands in the next powpow release (> 0.2.0). Until then, point your build at powpow's devel branch:
  ```sh
  git clone https://github.com/openpeeps/powpow ../powpow
  # then compile with:
  #   --path:../powpow/src
  ```
- OpenSSL (linked by powpow's TLS layer)

## Examples

### Create a RTMP server
```nim
import pkg/rtmp

var rtmpServer = newRTMPServer(RtmpServerSettings(
  enableRestApi = true,   # Enable REST API for server monitoring
  restApiPort = Port(4000), # Port for REST API
  rtmpPort = Port(1935),    # Port for RTMP connections
))

# start the RTMP server (blocks; runs the event loop)
rtmpServer.startServer()
```

The server accepts incoming publishers and players, fans out published media to all subscribers, caches sequence headers + metadata for late joiners, and handles slow subscribers with pause/resume and keyframe recovery.

### Monitor a running server
With `enableRestApi = true`, the server exposes a JSON snapshot of active streams:
```sh
curl http://localhost:4000/
```
```json
{
  "streams": {
    "live/livestream": {
      "id": "live/livestream",
      "publisher": {"id": "3", "ip": "127.0.0.1", "published_at": 1725000000},
      "subscribers": [{"id": "7", "ip": "127.0.0.1", "subscribed_at": 1725000010}],
      "created_at": 1725000000
    }
  }
}
```

### Create a RTMP streaming client
Use the following code to create an RTMP client that connects to an RTMP server and streams media files from disk in real time. Check the runnable example from the `examples/` folder for a complete working example.
```nim
import pkg/rtmp

let
  rtmpClient = newRtmpClient("rtmp://127.0.0.1/live/livestream")
  flvVideoPath = "./data/8721923-sd_426_226_25fps.flv"
  aacAudioPath = "./data/space_loop_78bpm.aac"

proc startStreaming(c: RtmpClient, ps: PlaylistState) =
  # Start streaming video and audio files with zero-copy.
  startStreamFlvZeroCopy(c, flvVideoPath, c.msgStreamId, startTs = c.ps.globalTs)
  startStreamAacAdtsZeroCopy(c, aacAudioPath, c.msgStreamId, 4'u8, startTs = c.ps.globalTs)

rtmpClient.ps = PlaylistState()
rtmpClient.onPublishOk =
  proc(c: RtmpClient) =
    # Called when the server acknowledges the publish request
    echo "[rtmp] Starting to stream video and audio..."
    startPacer(c, proc(c2: RtmpClient) = startStreaming(c2, c2.ps))

rtmpClient.onStreamEnd =
  proc (c: RtmpClient, st: StreamState, sent: int) =
    # Called when a stream finishes sending all data.
    # Use this to start the next item in a playlist.
    echo "[rtmp] Stream ended, bytes sent=", sent
    if st.msgType == 0x09'u8:
      inc c.ps.videoIdx
      startStreamFlvZeroCopy(c, flvVideoPath, c.msgStreamId, startTs = c.ps.globalTs)
    elif st.msgType == 0x08'u8:
      inc c.ps.audioIdx
      startStreamAacAdtsZeroCopy(c, aacAudioPath, c.msgStreamId, 4'u8, startTs = c.ps.globalTs)

rtmpClient.onStreamError =
  proc(c: RtmpClient, st: StreamState, err: cstring) =
    echo "[rtmp] Stream error: ", err

rtmpClient.onError =
  proc(c: RtmpClient, msg: string) =
    # Fires on transport failures (connection refused / unreachable host /
    # handshake timeout) and on server `_error` responses
    echo "[rtmp] ERROR: ", msg
    c.loop.stop()

echo "[rtmp] Starting event loop"
rtmpClient.loop.run()  # blocks; runs the powpow event loop
```

> [!NOTE]
> The client guards against dead peers: if a server accepts TCP but never completes the RTMP handshake within 10 seconds, `onError` fires with `"handshake timeout"` and the connection is closed — no silent hangs.

### Publish over RTMPS (TLS)
Simply use an `rtmps://` URL — TLS wrapping, SNI and the RTMP handshake are handled automatically:
```nim
let rtmpClient = newRtmpClient("rtmps://ingest.example.com/live/livestream")
```

### Client callbacks reference

| Callback | Signature | Fires when |
|---|---|---|
| `onPublishOk` | `proc(c: RtmpClient)` | Server confirms publish (`NetStream.Publish.Start`) |
| `onStreamStart` | `proc(c: RtmpClient, st: StreamState, sent: int)` | A media file starts streaming |
| `onStreamProgress` | `proc(c: RtmpClient, st: StreamState, sent: int)` | Periodically while streaming |
| `onStreamEnd` | `proc(c: RtmpClient, st: StreamState, sent: int)` | A media file is fully sent |
| `onStreamError` | `proc(c: RtmpClient, st: StreamState, err: cstring)` | File open/read failure during streaming |
| `onError` | `proc(c: RtmpClient, msg: string)` | Transport failure (connection refused / unreachable / handshake timeout) or server `_error` (auth failure, stream not found, ...) |

### Test with SRS
You can use [SRS (Simple Realtime Server)](https://github.com/ossrs/srs) as a local test server:
```sh
docker run --rm -p 1935:1935 -p 8080:8080 ossrs/srs:5
# then point the client at rtmp://127.0.0.1/live/livestream
# watch the stream: ffplay rtmp://127.0.0.1/live/livestream
```

### Play RTMP Stream with VLC
To play an RTMP stream with URL `rtmp://localhost/live/livestream` on VLC player, open the player, go to Media > Open Network Stream, enter the URL and click Play.

## Roadmap

See [COVERAGE.md](./COVERAGE.md) for a detailed audit of what is implemented today.

- [x] Switch networking core from libevent to [powpow](https://github.com/openpeeps/powpow)
- [x] Enhanced handshake (HMAC-SHA256) with plain-handshake fallback
- [x] RTMPS (TLS) client support
- [x] Client transport-error surfacing + 10s handshake watchdog (no silent hangs)
- [ ] Client-side playback: implement `play` / subscribe mode so the client can receive streams
- [ ] Full client-side AMF0 codec (reuse the server's `actionmessage` module) + AMF3 encoding
- [ ] `_error` handling on the server side
- [ ] Remaining control messages: Abort (client send), `receiveAudio` / `receiveVideo`, `seek`, `FCSubscribe`
- [ ] Aggregate messages (type 18) for higher throughput
- [ ] Dynamic bandwidth estimation and adaptive ACK window (replace hardcoded values)
- [ ] Shared Object message support
- [ ] Live ingest from webcam/microphone
  - [ ] H.264 video ([Cisco OpenH264](https://github.com/cisco/openh264)) and AAC ([FDK AAC](https://github.com/mstorsjo/fdk-aac)) encoding or [FFmpeg bindings](https://github.com/mantielero/ffmpeg6.nim)
- [ ] RTMP server TLS (accept `rtmps://` connections)

## Projects using RTMP package
Check out these projects that are using the RTMP package:
- [Groovebox](https://github.com/openpeeps/groovebox) &mdash; Lightweight CLI app for streaming to Icecast and YouTube/Twitch RTMP servers.

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/rtmp/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/rtmp/fork)
- 😎 [Get €20 in cloud credits from Hetzner](https://hetzner.cloud/?ref=Hm0mYGM9NxZ4)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
