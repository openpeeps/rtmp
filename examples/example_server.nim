import pkg/rtmp

# This is a simple example of how to create an RTMP server
# using the `rtmp` package.
# 
# The server will listen for incoming RTMP connections on the
# default port (1935). You can customize the server settings by passing a
# `RtmpServerSettings` object to `newRTMPServer`.
#
# For more details, check the documentation https://openpeeps.github.io/rtmp/rtmp/rtmpserver.html

# Create a new RTMP server with default settings
var rtmpServer = newRTMPServer()

# Start the RTMP server and listen for incoming connections
rtmpServer.startServer()