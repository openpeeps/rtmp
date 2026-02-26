# Real-Time Messaging Protocol (RTMP) Client & Server for Nim lang
#
# (c) 2026 George Lemon | MIT License
#          Made by Humans from OpenPeeps
#          https://github.com/openpeeps/rtmp

## This module provides cross-platform memory management utilities, including
## a portable shim for malloc_trim() to release unused memory back to the OS.
## On macOS, it uses malloc_zone_pressure_relief() from libmalloc. On Linux,
## it uses glibc's malloc_trim(). On unsupported platforms, the functions are no-ops.
## 
## This is a hacky module from the Supranim Web Framework:
## https://github.com/supranim/supranim

when defined(macosx):
  type
    MallocZoneT* = object ## Opaque; libmalloc internal
    MallocZone*  = ptr MallocZoneT

  proc malloc_default_zone*(): MallocZone
    {.cdecl, importc: "malloc_default_zone", header: "<malloc/malloc.h>".}

  # size_t malloc_zone_pressure_relief(malloc_zone_t *, size_t goal);
  # Returns number of bytes released (0 if none).
  proc malloc_zone_pressure_relief*(zone: MallocZone; goal: csize_t): csize_t
    {.cdecl, importc: "malloc_zone_pressure_relief", header: "<malloc/malloc.h>".}

  ## Portable shim: behave *like* malloc_trim(pad). Returns true if any
  ## bytes were released to the OS.
  proc malloc_trim*(pad: csize_t = 0): bool =
    let released = malloc_zone_pressure_relief(malloc_default_zone(), pad)
    result = released > 0

  proc releaseUnusedMemory*(): bool {.discardable.} =
    malloc_trim(0)

elif defined(linux):
  # int malloc_trim(size_t pad); nonzero on success
  proc glibc_malloc_trim(pad: csize_t): cint
    {.cdecl, importc: "malloc_trim", header: "<malloc.h>".}

  proc malloc_trim*(pad: csize_t = 0): bool =
    glibc_malloc_trim(pad) != 0

  proc releaseUnusedMemory*(): bool {.discardable.} =
    malloc_trim(0)

else:
  proc malloc_trim*(pad: csize_t = 0): bool = false
  proc releaseUnusedMemory*(): bool = false

template freemem*(x: untyped) =
  {.gcsafe.}:
    discard releaseUnusedMemory()
