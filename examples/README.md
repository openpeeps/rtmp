### Nim RTMP Client and Server Examples
Check the source code in this folder for runnable examples of using the RTMP package to create RTMP clients and servers. These examples demonstrate how to set up a basic RTMP server, connect to it with a client, and stream media files using zero-copy techniques for efficient delivery.

> [!NOTE]
> Requires Nim >= 2.2.0. The networking core is [powpow](https://github.com/openpeeps/powpow) — no external C dependencies besides OpenSSL.

| Example | Description |
|---|---|
| `example_server.nim` | Minimal RTMP server with REST monitoring API |
| `example_streaming.nim` | Client that publishes an FLV video + AAC audio track in a loop |
| `example_rtmps.nim` | Client publishing over RTMPS (TLS) with `_error` handling |
| `simple_server.nim` | One-liner server bootstrap |

Run any example from the repo root:
```sh
nim c -r examples/example_rtmps.nims examples/example_streaming.nim
```

### Use SRS as a test RTMP server
You can use the SRS (Simple Realtime Server) as a test RTMP server to connect your RTMP client to:
```sh
docker run --rm -p 1935:1935 -p 8080:8080 ossrs/srs:5
```
SRS is a popular open-source RTMP server that supports various streaming protocols and features. You can find the SRS project here: https://github.com/ossrs/srs

Watch a published stream:
```sh
ffplay rtmp://127.0.0.1/live/livestream
```

### Credits
- Music from my personal collection
- Footage by [Cottonbro Studio](https://www.pexels.com/@cottonbro/)
