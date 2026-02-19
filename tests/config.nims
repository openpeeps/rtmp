switch("path", "$projectDir/../src")

when defined(macosx):
  --passL:"/opt/local/lib/libssl.a"
  --passL:"/opt/local/lib/libcrypto.a"
  --passL:"/opt/local/lib/libevent.a"
  --passC:"-I /opt/local/include"
elif defined(linux):
  --passL:"/usr/lib/x86_64-linux-gnu/libssl.so"
  --passL:"/usr/lib/x86_64-linux-gnu/libcrypto.so"
  --passL:"/usr/lib/x86_64-linux-gnu/libevent.so"
  --passC:"-I /usr/include"