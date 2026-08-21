# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

import std/[tables]

## This module defines the RTMP server monitor data types
## for tracking active streams, publishers, and subscribers.

type
  RtmpClient* = object of RootObj
    ## Base type for RTMP clients (publishers and subscribers)
    id*: string
      ## Unique identifier for the client, typically derived from the connection info
    ip*: string
      ## IP address of the client

  RtmpSubscriber* = object of RtmpClient
    ## Represents a connected RTMP subscriber (stream consumer)
    subscribed_at*: int64
      ## Timestamp of when the subscriber started receiving the stream (Unix time in seconds)
  
  RtmpPublisher* = object of RtmpClient
    ## Represents a connected RTMP publisher (stream source)
    published_at*: int64
      ## Timestamp when publisher started streaming (Unix time in seconds)

  RtmpStream* = object
    id*: string
      ## Unique identifier for the stream
    publisher*: RtmpPublisher
      ## The publisher client that is sending the stream data
    subscribers*: seq[RtmpSubscriber]
      ## List of subscriber clients that are receiving the stream data
    created_at*: int64
      ## Timestamp when the stream was created (Unix time in seconds)

  RtmpMonitor* = ref object
    streams*: Table[string, RtmpStream]
      ## A table mapping stream IDs to their corresponding
      ## stream information, including publisher and subscribers

var gMonitor*: RtmpMonitor = RtmpMonitor()
  ## Global instance of the RTMP monitor that tracks all
  ## active streams and clients
