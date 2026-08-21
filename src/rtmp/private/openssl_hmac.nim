# RTMP enhanced handshake — OpenSSL HMAC-SHA256 bindings
#
# These are the minimal OpenSSL declarations needed for HMAC-SHA256.
# powpow already links -lssl -lcrypto via tlsapi.nim.

type
  EvpMd* = pointer
  EvpMdCtx* = pointer
  HMACCtx* = pointer

proc EVP_sha256*(): EvpMd {.importc: "EVP_sha256", dynlib: "libssl.so|libssl.dylib|libssl.so.3|libssl.so.1.1".}

proc HMAC*(md: EvpMd; key: pointer; keyLen: cint;
           data: pointer; dataLen: csize_t;
           output: pointer; outputLen: ptr cuint): pointer {.
  importc: "HMAC", dynlib: "libcrypto.so|libcrypto.dylib|libcrypto.so.3|libcrypto.so.1.1".}

proc HMAC_CTX_new*(): HMACCtx {.
  importc: "HMAC_CTX_new", dynlib: "libcrypto.so|libcrypto.dylib|libcrypto.so.3|libcrypto.so.1.1".}

proc HMAC_CTX_free*(ctx: HMACCtx) {.
  importc: "HMAC_CTX_free", dynlib: "libcrypto.so|libcrypto.dylib|libcrypto.so.3|libcrypto.so.1.1".}

proc HMAC_Init_ex*(ctx: HMACCtx; key: pointer; keyLen: cint;
                   md: EvpMd; impl: pointer): cint {.
  importc: "HMAC_Init_ex", dynlib: "libcrypto.so|libcrypto.dylib|libcrypto.so.3|libcrypto.so.1.1".}

proc HMAC_Update*(ctx: HMACCtx; data: pointer; dataLen: csize_t): cint {.
  importc: "HMAC_Update", dynlib: "libcrypto.so|libcrypto.dylib|libcrypto.so.3|libcrypto.so.1.1".}

proc HMAC_Final*(ctx: HMACCtx; output: pointer; outputLen: ptr cuint): cint {.
  importc: "HMAC_Final", dynlib: "libcrypto.so|libcrypto.dylib|libcrypto.so.3|libcrypto.so.1.1".}
