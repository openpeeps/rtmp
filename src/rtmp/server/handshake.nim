# RTMP Enhanced Handshake — HMAC-SHA256 digest support
#
# Implements the enhanced RTMP handshake with HMAC-SHA256 digests
# for interoperability with modern RTMP servers (SRS, nginx-rtmp, etc.).
#
# The handshake uses Scheme 0 (digest at front of C1/S1).

import std/[times, random]
import ../private/openssl_hmac

const
  RTMP_HANDSHAKE_SIZE* = 1536

  # Genuine Adobe Flash Media Server 001 (36-byte open + 32 random = 68)
  GenuineFMSKey*: array[68, byte] = [
    0x47, 0x65, 0x6e, 0x75, 0x69, 0x6e, 0x65, 0x20,
    0x41, 0x64, 0x6f, 0x62, 0x65, 0x20, 0x46, 0x6c,
    0x61, 0x73, 0x68, 0x20, 0x4d, 0x65, 0x64, 0x69,
    0x61, 0x20, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72,
    0x20, 0x30, 0x30, 0x31,
    0xf0, 0xee, 0xc2, 0x4a, 0x80, 0x68, 0xbe, 0xe8,
    0x2e, 0x00, 0xd0, 0xd1, 0x02, 0x9e, 0x7e, 0x57,
    0x6e, 0xec, 0x5d, 0x2d, 0x29, 0x80, 0x6f, 0xab,
    0x93, 0xb8, 0xe6, 0x36, 0xcf, 0xeb, 0x31, 0xae
  ]

  GenuineFMSKeyOpenLen = 36  # for C1/S1 digest creation
  GenuineFMSKeyFullLen = 68  # for C2/S2 signatures

  # Genuine Adobe Flash Player 001 (30-byte open + 32 random = 62)
  GenuineFPKey*: array[62, byte] = [
    0x47, 0x65, 0x6e, 0x75, 0x69, 0x6e, 0x65, 0x20,
    0x41, 0x64, 0x6f, 0x62, 0x65, 0x20, 0x46, 0x6c,
    0x61, 0x73, 0x68, 0x20, 0x50, 0x6c, 0x61, 0x79,
    0x65, 0x72, 0x20, 0x30, 0x30, 0x31,
    0xF0, 0xEE, 0xC2, 0x4A, 0x80, 0x68, 0xBE, 0xE8,
    0x2E, 0x00, 0xD0, 0xD1, 0x02, 0x9E, 0x7E, 0x57,
    0x6E, 0xEC, 0x5D, 0x2D, 0x29, 0x80, 0x6F, 0xAB,
    0x93, 0xB8, 0xE6, 0x36, 0xCF, 0xEB, 0x31, 0xAE
  ]

  GenuineFPKeyOpenLen = 30  # for C1/S1 digest creation
  GenuineFPKeyFullLen = 62  # for C2/S2 signatures

  # Enhanced handshake version (placed in C1 bytes 4-7)
  EnhancedVersion = 0x09007C02'u32

  # Digest is 32 bytes (SHA-256 output)
  DigestLen = 32

  # Scheme 0: digest area at bytes 8..775, key area at bytes 776..1535
  Scheme0DigestBase = 8
  Scheme0KeyBase = 776
  Scheme0DigestAreaLen = 764
  Scheme0KeyAreaLen = 760

  # Pointer bytes for digest offset (bytes 8-11 in Scheme 0)
  Scheme0DigestPtrOffset = 8
  # Pointer bytes for key offset (bytes 1532-1535 in Scheme 0)
  Scheme0KeyPtrOffset = 1532

proc hmacSha256(key: pointer, keyLen: int,
                data: pointer, dataLen: int): array[32, byte] =
  ## Compute HMAC-SHA256 using OpenSSL.
  var outputLen: cuint = 0
  let res = HMAC(EVP_sha256(), key, keyLen.cint, data, dataLen.csize_t,
                 addr result[0], addr outputLen)
  if res == nil:
    zeroMem(addr result[0], 32)

proc computeDigestPos(data: ptr UncheckedArray[byte], ptrOffset: int, base: int): int =
  ## Compute digest position from pointer bytes (Scheme 0).
  let b0 = int(data[ptrOffset])
  let b1 = int(data[ptrOffset + 1])
  let b2 = int(data[ptrOffset + 2])
  let b3 = int(data[ptrOffset + 3])
  (b0 + b1 + b2 + b3) mod 728 + base

proc computeDigestOverPacket(packet: ptr UncheckedArray[byte], digestPos: int,
                             key: pointer, keyLen: int): array[32, byte] =
  ## HMAC-SHA256 over the 1536-byte packet with the 32-byte gap at digestPos.
  ## msg = packet[0..digestPos-1] ++ packet[digestPos+32..1535]
  ## total = 1504 bytes
  var buf = newSeq[byte](RTMP_HANDSHAKE_SIZE - DigestLen)
  # Copy before gap
  if digestPos > 0:
    copyMem(addr buf[0], packet, digestPos)
  # Copy after gap
  let afterGap = RTMP_HANDSHAKE_SIZE - digestPos - DigestLen
  if afterGap > 0:
    copyMem(addr buf[digestPos], cast[pointer](cast[uint](packet) + (digestPos + DigestLen).uint), afterGap)
  result = hmacSha256(key, keyLen, addr buf[0], buf.len)

proc createC1Enhanced*(c1: ptr UncheckedArray[byte]) =
  ## Create enhanced C1 with HMAC-SHA256 digest using FP key.
  ## C1 layout (Scheme 0):
  ##   bytes 0-3: timestamp
  ##   bytes 4-7: version (non-zero = enhanced)
  ##   bytes 8-11: pointer to digest offset
  ##   bytes 12-775: digest area (764 bytes, digest at computed offset)
  ##   bytes 776-1531: key area (760 bytes)
  ##   bytes 1532-1535: pointer to DH key offset (unused, zero)

  # Set timestamp (bytes 0-3)
  let ts = uint32(epochTime())
  c1[0] = byte((ts shr 24) and 0xFF)
  c1[1] = byte((ts shr 16) and 0xFF)
  c1[2] = byte((ts shr 8) and 0xFF)
  c1[3] = byte(ts and 0xFF)

  # Set enhanced version (bytes 4-7)
  c1[4] = byte((EnhancedVersion shr 24) and 0xFF)
  c1[5] = byte((EnhancedVersion shr 16) and 0xFF)
  c1[6] = byte((EnhancedVersion shr 8) and 0xFF)
  c1[7] = byte(EnhancedVersion and 0xFF)

  # Fill random data in digest and key areas
  for i in Scheme0DigestBase ..< RTMP_HANDSHAKE_SIZE:
    c1[i] = byte(rand(255))

  # Set digest pointer (bytes 8-11) — we'll compute a deterministic offset
  let digestOffset = 0  # place digest at base of digest area
  c1[Scheme0DigestPtrOffset] = byte(digestOffset and 0xFF)
  c1[Scheme0DigestPtrOffset + 1] = byte((digestOffset shr 8) and 0xFF)
  c1[Scheme0DigestPtrOffset + 2] = byte((digestOffset shr 16) and 0xFF)
  c1[Scheme0DigestPtrOffset + 3] = byte((digestOffset shr 24) and 0xFF)

  # Zero DH key pointer (bytes 1532-1535)
  c1[Scheme0KeyPtrOffset] = 0
  c1[Scheme0KeyPtrOffset + 1] = 0
  c1[Scheme0KeyPtrOffset + 2] = 0
  c1[Scheme0KeyPtrOffset + 3] = 0

  # Compute digest position
  let digestPos = computeDigestPos(c1, Scheme0DigestPtrOffset, Scheme0DigestBase)

  # Compute HMAC-SHA256 over C1 with FP key
  let digest = computeDigestOverPacket(c1, digestPos, unsafeAddr GenuineFPKey[0], GenuineFPKeyOpenLen)

  # Place digest
  copyMem(addr c1[digestPos], unsafeAddr digest[0], DigestLen)

proc validateC1Digest*(c1: ptr UncheckedArray[byte]): bool =
  ## Validate C1 digest using FP key (player key).
  ## Returns true if the digest matches (enhanced handshake detected).
  let digestPos = computeDigestPos(c1, Scheme0DigestPtrOffset, Scheme0DigestBase)
  let expected = computeDigestOverPacket(c1, digestPos, unsafeAddr GenuineFPKey[0], GenuineFPKeyOpenLen)
  for i in 0 ..< DigestLen:
    if c1[digestPos + i] != expected[i]:
      return false
  true

proc createS1Enhanced*(s1: ptr UncheckedArray[byte]) =
  ## Create enhanced S1 with HMAC-SHA256 digest using FMS key.
  let ts = uint32(epochTime())
  s1[0] = byte((ts shr 24) and 0xFF)
  s1[1] = byte((ts shr 16) and 0xFF)
  s1[2] = byte((ts shr 8) and 0xFF)
  s1[3] = byte(ts and 0xFF)

  # Version = 0 (server doesn't set version field in S1 per spec)
  s1[4] = 0; s1[5] = 0; s1[6] = 0; s1[7] = 0

  # Fill random data
  for i in Scheme0DigestBase ..< RTMP_HANDSHAKE_SIZE:
    s1[i] = byte(rand(255))

  # Set digest pointer
  let digestOffset = 0
  s1[Scheme0DigestPtrOffset] = byte(digestOffset and 0xFF)
  s1[Scheme0DigestPtrOffset + 1] = byte((digestOffset shr 8) and 0xFF)
  s1[Scheme0DigestPtrOffset + 2] = byte((digestOffset shr 16) and 0xFF)
  s1[Scheme0DigestPtrOffset + 3] = byte((digestOffset shr 24) and 0xFF)

  # Zero DH key pointer
  s1[Scheme0KeyPtrOffset] = 0
  s1[Scheme0KeyPtrOffset + 1] = 0
  s1[Scheme0KeyPtrOffset + 2] = 0
  s1[Scheme0KeyPtrOffset + 3] = 0

  # Compute and place digest using FMS key
  let digestPos = computeDigestPos(s1, Scheme0DigestPtrOffset, Scheme0DigestBase)
  let digest = computeDigestOverPacket(s1, digestPos, unsafeAddr GenuineFMSKey[0], GenuineFMSKeyOpenLen)
  copyMem(addr s1[digestPos], unsafeAddr digest[0], DigestLen)

proc validateS1Digest*(s1: ptr UncheckedArray[byte]): bool =
  ## Validate S1 digest using FMS key (server key).
  let digestPos = computeDigestPos(s1, Scheme0DigestPtrOffset, Scheme0DigestBase)
  let expected = computeDigestOverPacket(s1, digestPos, unsafeAddr GenuineFMSKey[0], GenuineFMSKeyOpenLen)
  for i in 0 ..< DigestLen:
    if s1[digestPos + i] != expected[i]:
      return false
  true

proc signC2*(c2: ptr UncheckedArray[byte], s1: ptr UncheckedArray[byte]) =
  ## Create C2 signature: HMAC-SHA256 of S1 using full FMS key (68 bytes).
  ## C2 = 1536 bytes: first 1504 random, last 32 = HMAC signature.
  # Fill with random data
  for i in 0 ..< RTMP_HANDSHAKE_SIZE:
    c2[i] = byte(rand(255))
  # Compute HMAC-SHA256 over S1 with full FMS key
  let sig = hmacSha256(unsafeAddr GenuineFMSKey[0], GenuineFMSKeyFullLen,
                       s1, RTMP_HANDSHAKE_SIZE)
  # Place signature at end
  copyMem(addr c2[RTMP_HANDSHAKE_SIZE - DigestLen], unsafeAddr sig[0], DigestLen)

proc signS2*(s2: ptr UncheckedArray[byte], c1: ptr UncheckedArray[byte]) =
  ## Create S2 signature: HMAC-SHA256 of C1 using full FP key (62 bytes).
  ## S2 = 1536 bytes: first 1504 random, last 32 = HMAC signature.
  for i in 0 ..< RTMP_HANDSHAKE_SIZE:
    s2[i] = byte(rand(255))
  let sig = hmacSha256(unsafeAddr GenuineFPKey[0], GenuineFPKeyFullLen,
                       c1, RTMP_HANDSHAKE_SIZE)
  copyMem(addr s2[RTMP_HANDSHAKE_SIZE - DigestLen], unsafeAddr sig[0], DigestLen)

proc validateS2*(s2: ptr UncheckedArray[byte], c1: ptr UncheckedArray[byte]): bool =
  ## Validate S2 signature using full FP key.
  let expected = hmacSha256(unsafeAddr GenuineFPKey[0], GenuineFPKeyFullLen,
                            c1, RTMP_HANDSHAKE_SIZE)
  for i in 0 ..< DigestLen:
    if s2[RTMP_HANDSHAKE_SIZE - DigestLen + i] != expected[i]:
      return false
  true

proc validateC2*(c2: ptr UncheckedArray[byte], s1: ptr UncheckedArray[byte]): bool =
  ## Validate C2 signature using full FMS key.
  let expected = hmacSha256(unsafeAddr GenuineFMSKey[0], GenuineFMSKeyFullLen,
                            s1, RTMP_HANDSHAKE_SIZE)
  for i in 0 ..< DigestLen:
    if c2[RTMP_HANDSHAKE_SIZE - DigestLen + i] != expected[i]:
      return false
  true

proc isEnhancedC1*(c1: ptr UncheckedArray[byte]): bool =
  ## Check if C1 indicates enhanced handshake (version field non-zero).
  let version = (uint32(c1[4]) shl 24) or (uint32(c1[5]) shl 16) or
                 (uint32(c1[6]) shl 8) or uint32(c1[7])
  version != 0

proc isEnhancedS1*(s1: ptr UncheckedArray[byte]): bool =
  ## Check if S1 indicates enhanced handshake (version field non-zero).
  let version = (uint32(s1[4]) shl 24) or (uint32(s1[5]) shl 16) or
                 (uint32(s1[6]) shl 8) or uint32(s1[7])
  version != 0
