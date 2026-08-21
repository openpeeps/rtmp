import times
import rtmp.rtmpserver

when isMainModule:
  echo "Starting example RTMP server at ", times.getTime().format("yyyy-MM-dd HH:mm:ss")
  # startServer blocks and runs the powpow event loop; Ctrl-C to stop.
  startServer()
