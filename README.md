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
- Based on Libevent for High performance networking
- RTMP Client for connecting to RTMP servers
- RTMP Server for accepting RTMP client connections
- Super Lightweight and minimal dependencies (only Libevent)
- Zero-Copy File Streaming support for efficient media delivery
- Flexible API for handling RTMP messages and events
- Live ingest support for streaming from livecam/microphone (SOON)

## Examples
Here you can find some simple examples to get you started with the RTMP package.

### Create a RTMP server
```nim
import pkg/rtmp
var rtmpServer = newRTMPServer(settings = RtmpServerSettings(
  enableRestApi = true, # Enable REST API for server management
  restApiPort = 8080, # Port for REST API
  ...
))

# start the RTMP server
rtmpServer.startServer()
```

### Create a RTMP streaming client
Use the following code to create a RTMP client that connects to an RTMP server and starts streaming media from disk. Check the runnable example from the `examples/` folder for a complete working example.
```nim
import pkg/rtmp

let
  rtmpClient = newRtmpClient("rtmp://127.0.0.1/live/livestream")
  flvVideoPath = "./data/8721923-sd_426_226_25fps.flv"
  aacAudioPath = "./data/space_loop_78bpm.aac"

proc startStreaming(c: RtmpClient, ps: PlaylistState) =
  # Start streaming video and audio files with zero-copy.
  #
  # This is a simple example that streams one video and one
  # audio file in a loop. You can extend this to use playlists
  # and more complex logic.
  startStreamFlvZeroCopy(c, flvVideoPath, c.msgStreamId, startTs = c.ps.globalTs)
  startStreamAacAdtsZeroCopy(c, aacAudioPath, c.msgStreamId, 4'u8, startTs = c.ps.globalTs)

rtmpClient.ps = PlaylistState()
rtmpClient.onPublishOk =
  proc(c: RtmpClient) =
    # This callback is called when the client successfully connects
    # and publishes to the RTMP server. You can use this callback to
    # start streaming
    echo "[rtmp] Starting to stream video and audio..."
    startPacer(c, proc(c2: RtmpClient) = startStreaming(c2, c2.ps))
    
rtmpClient.onStreamEnd =
  proc (c: RtmpClient, st: StreamState, sent: int) =
    # This callback is called when a stream finishes sending all data.
    # You can use this to start the next video/audio in a playlist,
    # or to log stream end events.
    echo "[rtmp] Stream ended, bytes sent=", sent
    if st.msgType == 0x09'u8:
      # Video ended
      echo "[rtmp] Video stream ended. Replaying the video..."
      inc c.ps.videoIdx
      startStreamFlvZeroCopy(c, flvVideoPath, c.msgStreamId, startTs = c.ps.globalTs)
    elif st.msgType == 0x08'u8:
      # Audio ended
      echo "rtmp] Audio stream ended. Restarting audio..."
      inc c.ps.audioIdx
      startStreamAacAdtsZeroCopy(c, aacAudioPath, c.msgStreamId, 4'u8, startTs = c.ps.globalTs)

rtmpClient.onStreamError =
  proc(c: RtmpClient, st: StreamState, err: cstring) =
    echo "[rtmp] Stream error: ", err

echo "[rtmp] Starting event loop"
discard event_base_dispatch(rtmpClient.base)

```

### Play RTMP Stream with VLC
To play an RTMP stream with URL `rtmp://localhost/live/livestream` on VLC player, open the player, go to Media > Open Network Stream, enter the URL and click Play.


## Projects using RTMP package
Check out these projects that are using the RTMP package:
- [Groovebox](https://github.com/openpeeps/groovebox) &mdash; Lightweight CLI app for streaming to Icecast and YouTube/Twitch RTMP servers.

## Todo
- [ ] Add support for RTMPS (RTMP over TLS)
- [ ] Implement live ingest from webcam/microphone
  - [ ] Add support for H.264 (need bindings for [Cisco H.264 codec](https://github.com/cisco/openh264)) video and AAC ([FDK AAC](https://github.com/mstorsjo/fdk-aac)) audio encoding or [Nim bindings for FFmpeg](https://github.com/mantielero/ffmpeg6.nim)

### ❤ Contributions & Support
- 🐛 Found a bug? [Create a new Issue](https://github.com/openpeeps/rtmp/issues)
- 👋 Wanna help? [Fork it!](https://github.com/openpeeps/rtmp/fork)
- 😎 [Get €20 in cloud credits from Hetzner](https://hetzner.cloud/?ref=Hm0mYGM9NxZ4)

### 🎩 License
MIT license. [Made by Humans from OpenPeeps](https://github.com/openpeeps).<br>
Copyright OpenPeeps & Contributors &mdash; All rights reserved.
