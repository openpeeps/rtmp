import pkg/rtmp

# This example demonstrates publishing over RTMPS (TLS) and
# handling server-side errors via the onError callback.
#
# Point it at any RTMPS ingest endpoint.

let
  rtmpClient = newRtmpClient("rtmps://ingest.example.com/live/livestream")
  flvVideoPath = "./data/8721923-sd_426_226_25fps.flv"

rtmpClient.onPublishOk =
  proc(c: RtmpClient) =
    echo "[rtmps] Publish accepted, starting video stream..."
    startPacer(c) do (c2: RtmpClient):
      startStreamFlvZeroCopy(c2, flvVideoPath, c2.msgStreamId)

rtmpClient.onStreamEnd =
  proc(c: RtmpClient, st: StreamState, sent: int) =
    echo "[rtmps] Stream ended, bytes sent=", sent

rtmpClient.onError =
  proc(c: RtmpClient, msg: string) =
    # Fires when the server returns _error, e.g. auth failure
    echo "[rtmps] Server error: ", msg
    c.loop.stop()

rtmpClient.onStreamError =
  proc(c: RtmpClient, st: StreamState, err: cstring) =
    echo "[rtmps] Stream error: ", err

echo "[rtmps] Starting event loop"
rtmpClient.loop.run()
