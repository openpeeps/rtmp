import std/[strutils, sequtils, random, os]

type
  PlaylistState* = ref object
    ## Lists of video and audio files
    videoFiles*: seq[string]
      ## Lists of video file paths
    audioFiles*: seq[string]
      ## Lists of video and audio file paths
    videoIdx*: int
      ## Indexes for current video and audio files
    audioIdx*: int
      ## Indexes for current video and audio files
    currentAudioPath*: string
      ## Path of the current audio file being streamed
    currentAudioDone*: bool
      ## Indicates if the current audio file has finished streaming
    globalTs*: uint32
      ## Global timestamp for synchronization

proc loadPlaylist*(filePath: string): seq[string] =
  # Load playlist from file (one entry per line), shuffle entries
  if not fileExists(filePath): return @[]
  result = readFile(filePath).splitLines().filterIt(it.len > 0)
  randomize()
  result.shuffle()

proc nextVideo*(ps: PlaylistState): string =
  # Get next video file from playlist (looping)
  if ps.videoFiles.len == 0: return ""
  if ps.videoIdx >= ps.videoFiles.len: ps.videoIdx = 0 # loop
  result = ps.videoFiles[ps.videoIdx]
  inc(ps.videoIdx)

proc nextAudio*(ps: PlaylistState): string =
  # Get next audio file from playlist (looping)
  if ps.audioFiles.len == 0: return ""
  if ps.audioIdx >= ps.audioFiles.len: ps.audioIdx = 0 # loop
  result = ps.audioFiles[ps.audioIdx]
  inc(ps.audioIdx)
