#pragma once

// Compresses one finished hourly file, X.jsonl -> X.jsonl.zst, without ever
// risking the data:
//
//   1. stream X.jsonl into X.jsonl.zst.tmp  (".tmp" = unfinished, ignore me)
//   2. fsync the .tmp                      (physically on disk)
//   3. decompress the .tmp and compare it byte for byte with X.jsonl
//   4. rename .tmp -> X.jsonl.zst          (all at once: complete, or absent)
//   5. delete X.jsonl                      (only now that a proven copy exists)
//
// A crash at any point leaves something the next run can finish: just the
// original (start again; a stale .tmp is overwritten), or the original
// plus a finished .zst (check they match, then delete the original). So
// for anyone reading the data: a file ending in .zst is always complete.
//
// Every .zst also carries zstd's own checksum of its content, so if a disk
// error ever damages it, reading it fails loudly instead of quietly
// returning wrong data.

#include <filesystem>
#include <string>

// Returns true once X.jsonl.zst exists, is verified, and X.jsonl is gone.
// Returns false with the reason in `error` otherwise; the original is
// never deleted on any failure path.
// level: zstd compression level, 1 (fastest) to 22 (smallest).
bool compress_file(const std::filesystem::path& source, int level, std::string& error);
