# Monitor JSON shape (same serialization the REST endpoint serves).

import unittest
import std/[json, jsonutils, tables]
import rtmp/server/rtmpmonitor

suite "monitor json":
  test "populated stream serializes":
    var mon = RtmpMonitor(streams: initTable[string, RtmpStream]())
    mon.streams["live"] = RtmpStream(
      id: "live",
      publisher: RtmpPublisher(id: "p1", ip: "1.2.3.4", published_at: 7),
      subscribers: @[RtmpSubscriber(id: "s1", ip: "5.6.7.8",
        subscribed_at: 9)],
      created_at: 5)
    let j = parseJson($toJson(mon))
    check j["streams"]["live"]["id"].getStr == "live"
    check j["streams"]["live"]["publisher"]["id"].getStr == "p1"
    check j["streams"]["live"]["publisher"]["ip"].getStr == "1.2.3.4"
    check j["streams"]["live"]["publisher"]["published_at"].getBiggestInt == 7
    check j["streams"]["live"]["subscribers"].len == 1
    check j["streams"]["live"]["subscribers"][0]["id"].getStr == "s1"
    check j["streams"]["live"]["created_at"].getBiggestInt == 5

  test "empty monitor":
    var mon = RtmpMonitor(streams: initTable[string, RtmpStream]())
    let j = parseJson($toJson(mon))
    check j["streams"].len == 0

  test "subscriber removal reflects":
    var mon = RtmpMonitor(streams: initTable[string, RtmpStream]())
    mon.streams["live"] = RtmpStream(id: "live",
      publisher: RtmpPublisher(id: "p", ip: "h", published_at: 1),
      subscribers: @[RtmpSubscriber(id: "a", ip: "h", subscribed_at: 1),
        RtmpSubscriber(id: "b", ip: "h", subscribed_at: 2)],
      created_at: 1)
    mon.streams["live"].subscribers.setLen(1)
    let j = parseJson($toJson(mon))
    check j["streams"]["live"]["subscribers"].len == 1
    check j["streams"]["live"]["subscribers"][0]["id"].getStr == "a"
