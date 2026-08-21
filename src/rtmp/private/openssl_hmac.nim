# RTMP enhanced handshake — OpenSSL HMAC-SHA256 bindings
#
# Minimal OpenSSL declarations needed for HMAC-SHA256.
# Linked at compile time, matching powpow's tlsapi.nim approach
# (-lssl -lcrypto; duplicate flags are harmless).

when not defined(windows):
  {.passL: "-lcrypto".}

type
  EvpMd* = pointer
  HMACCtx* = pointer

proc EVP_sha256*(): EvpMd {.importc: "EVP_sha256".}

proc HMAC*(md: EvpMd; key: pointer; keyLen: cint;
           data: pointer; dataLen: csize_t;
           output: pointer; outputLen: ptr cuint): pointer {.
  importc: "HMAC".}
