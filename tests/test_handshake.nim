# Enhanced RTMP handshake: digests, signatures, layout.
# Links libcrypto via the existing passL (present on macOS and CI linux).

import unittest
import std/random
import rtmp/server/handshake

randomize()

proc asPtr(b: var array[RTMP_HANDSHAKE_SIZE, byte]): ptr UncheckedArray[byte] =
  cast[ptr UncheckedArray[byte]](addr b[0])

suite "handshake layout":
  test "size constant":
    check RTMP_HANDSHAKE_SIZE == 1536

  test "c1 carries enhanced version":
    var c1: array[RTMP_HANDSHAKE_SIZE, byte]
    createC1Enhanced(asPtr(c1))
    check c1[4] == 0x09'u8
    check c1[5] == 0x00'u8
    check c1[6] == 0x7C'u8
    check c1[7] == 0x02'u8
    check isEnhancedC1(asPtr(c1)) == true

  test "plain c1 is not enhanced":
    var c1: array[RTMP_HANDSHAKE_SIZE, byte] # zeroed
    check isEnhancedC1(asPtr(c1)) == false

  test "s1 leaves version zero per spec":
    var s1: array[RTMP_HANDSHAKE_SIZE, byte]
    createS1Enhanced(asPtr(s1))
    check isEnhancedS1(asPtr(s1)) == false
    s1[4] = 1
    check isEnhancedS1(asPtr(s1)) == true

  test "genuine keys":
    check GenuineFMSKey.len == 68
    check GenuineFPKey.len == 62
    var prefix = newString(36)
    for i in 0 ..< 36: prefix[i] = char(GenuineFMSKey[i])
    check prefix == "Genuine Adobe Flash Media Server 001"
    var pprefix = newString(30)
    for i in 0 ..< 30: pprefix[i] = char(GenuineFPKey[i])
    check pprefix == "Genuine Adobe Flash Player 001"

suite "handshake digests":
  test "KNOWN BUG: own c1 never self-validates (digest overwrites its pointer)":
    # createC1Enhanced hardcodes digestOffset=0, so digestPos=8 and the
    # 32-byte digest lands on bytes 8..39 — destroying the pointer bytes
    # 8..11 it was computed from. Validation recomputes a different pos
    # and fails. Impact is limited (both our server and SRS/nginx fall
    # back to the plain handshake), but the enhanced path is dead for our
    # own packets. See FINDINGS.md H-1. Flip to == true when fixed.
    var c1: array[RTMP_HANDSHAKE_SIZE, byte]
    createC1Enhanced(asPtr(c1))
    check validateC1Digest(asPtr(c1)) == false

  test "c1 tamper fails (digest area and body)":
    var c1: array[RTMP_HANDSHAKE_SIZE, byte]
    createC1Enhanced(asPtr(c1))
    c1[20] = c1[20] xor 0xFF
    check validateC1Digest(asPtr(c1)) == false
    createC1Enhanced(asPtr(c1))
    c1[1000] = c1[1000] xor 0xFF
    check validateC1Digest(asPtr(c1)) == false

  test "KNOWN BUG: own s1 never self-validates (same pointer overlap)":
    # Same construction flaw as C1 (FINDINGS.md H-1). Flip to == true
    # together with the C1 test when fixed.
    var s1: array[RTMP_HANDSHAKE_SIZE, byte]
    createS1Enhanced(asPtr(s1))
    check validateS1Digest(asPtr(s1)) == false

  test "player and server keys do not cross-validate":
    var c1: array[RTMP_HANDSHAKE_SIZE, byte]
    createC1Enhanced(asPtr(c1))
    check validateS1Digest(asPtr(c1)) == false
    var s1: array[RTMP_HANDSHAKE_SIZE, byte]
    createS1Enhanced(asPtr(s1))
    check validateC1Digest(asPtr(s1)) == false

suite "handshake signatures":
  test "c2 signs s1":
    var s1: array[RTMP_HANDSHAKE_SIZE, byte]
    createS1Enhanced(asPtr(s1))
    var c2: array[RTMP_HANDSHAKE_SIZE, byte]
    signC2(asPtr(c2), asPtr(s1))
    check validateC2(asPtr(c2), asPtr(s1)) == true

  test "c2 tamper and wrong-peer fail":
    var s1: array[RTMP_HANDSHAKE_SIZE, byte]
    createS1Enhanced(asPtr(s1))
    var c2: array[RTMP_HANDSHAKE_SIZE, byte]
    signC2(asPtr(c2), asPtr(s1))
    c2[RTMP_HANDSHAKE_SIZE - 1] = c2[RTMP_HANDSHAKE_SIZE - 1] xor 0xFF
    check validateC2(asPtr(c2), asPtr(s1)) == false
    var other: array[RTMP_HANDSHAKE_SIZE, byte]
    createS1Enhanced(asPtr(other))
    signC2(asPtr(c2), asPtr(s1))
    check validateC2(asPtr(c2), asPtr(other)) == false

  test "s2 signs c1":
    var c1: array[RTMP_HANDSHAKE_SIZE, byte]
    createC1Enhanced(asPtr(c1))
    var s2: array[RTMP_HANDSHAKE_SIZE, byte]
    signS2(asPtr(s2), asPtr(c1))
    check validateS2(asPtr(s2), asPtr(c1)) == true
    s2[0] = s2[0] xor 0xFF # body tamper outside signature still fails:
    # signature is over c1, so s2 body tamper does NOT fail —
    check validateS2(asPtr(s2), asPtr(c1)) == true
    s2[RTMP_HANDSHAKE_SIZE - 1] = s2[RTMP_HANDSHAKE_SIZE - 1] xor 0xFF
    check validateS2(asPtr(s2), asPtr(c1)) == false
