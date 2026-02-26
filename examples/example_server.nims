when defined(macosx):
  --passL:"/opt/local/lib/libssl.a"
  --passL:"/opt/local/lib/libcrypto.a"
  --passL:"/opt/local/lib/libevent.a"
  --passC:"-I /opt/local/include"
elif defined(linux):
  --passL:"/usr/lib/x86_64-linux-gnu/libssl.a"
  --passL:"/usr/lib/x86_64-linux-gnu/libcrypto.a"
  --passL:"/usr/local/lib/libevent.a"
  --passC:"-I /usr/include"