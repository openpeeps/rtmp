# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

import std/[httpcore, tables, times, json, jsonutils]
import pkg/libevent/bindings/[event, bufferevent, buffer, http, listener]

## This module implements a simple RTMP server monitor
## that listens for incoming RTMP connections and provides a
## REST API for monitoring the server status.

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
      ## Timestamp of when the publisher started streaming (Unix time in seconds)

  RtmpStream* = object
    id*: string
      ## Unique identifier for the stream, typically derived from the RTMP URL or stream key
    publisher*: RtmpPublisher
      ## The publisher client that is sending the stream data
    subscribers*: seq[RtmpSubscriber]
      ## List of subscriber clients that are receiving the stream data
    created_at*: int64
      ## Timestamp of when the stream was created (Unix time in seconds)

  RtmpMonitor* = ref object
    streams*: Table[string, RtmpStream]
      ## A table mapping stream IDs to their corresponding
      ## stream information, including publisher and subscribers

var gMonitor*: RtmpMonitor = RtmpMonitor()
  ## Global instance of the RTMP monitor that tracks all
  ## active streams and clients

proc evbufAddString(buf: ptr Evbuffer; s: string) =
  if buf == nil or s.len == 0: return
  discard evbuffer_add(buf, unsafeAddr s[0], csize_t(s.len))

template respond(str: string, code: HttpCode = HttpCode(200)) =
  let buf = evhttp_request_get_output_buffer(req)
  assert buf != nil # should never be nil
  assert evbuffer_add(buf, str.cstring, str.len.csize_t) == 0
  evhttp_send_reply(req, code.cint, "", buf)
  return

proc apiRequestCb*(req: ptr evhttp_request; arg: pointer) {.cdecl.} =
  ## Callback for handling incoming HTTP requests to the monitor API
  if req == nil: return

  let uriC = evhttp_request_get_uri(req)
  let uri = if uriC == nil: "" else: $uriC

  var
    code = HTTP_OK
    reason = "OK"
    body: string
  if uri != "/":
    code = HTTP_NOTFOUND
    reason = "Not Found"
    body = """{"ok":false,"error":"not_found"}"""
  else:
    body = $(gMonitor.toJson())
    

  let output = evhttp_request_get_output_buffer(req)
  if output == nil:
    evhttp_send_error(req, HTTP_INTERNAL, "Internal Server Error")
    return

  let headers = evhttp_request_get_output_headers(req)
  discard evhttp_add_header(headers, "Content-Type", "application/json")
  discard evhttp_add_header(headers, "Connection", "close")
  evbufAddString(output, body)
  evhttp_send_reply(req, cint(code), reason, output)
