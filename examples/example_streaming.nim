import pkg/rtmp

# This is a simple example of how to create an RTMP streaming client
# using the `rtmp` package.
#
# The client will connect to an RTMP server and stream video and audio
# files in a loop. You can customize the client settings and streaming logic as needed.
# For more details, check the documentation https://openpeeps.github.io/rtmp/rtmp/rtmpclient.html

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
# this is hardcoded playlist state and should not be part
# of the rtmp client, lol. todo remove this and use a generic
# type for client state.

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

