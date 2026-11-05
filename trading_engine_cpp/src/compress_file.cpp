#include "compress_file.h"

#include <cerrno>
#include <cstring>  // std::memcmp, std::strerror
#include <memory>
#include <system_error>
#include <vector>

#include <fcntl.h>     // open
#include <sys/stat.h>  // fstat
#include <unistd.h>    // read, write, fsync, close

#include <zstd.h>

namespace fs = std::filesystem;

namespace {

// Closes the file when it goes out of scope, so no early "return false"
// below can leak an open file.
struct Fd {
    int fd = -1;
    explicit Fd(int f) : fd(f) {}
    ~Fd() {
        if (fd >= 0) {
            ::close(fd);
        }
    }
    Fd(const Fd&) = delete;
    Fd& operator=(const Fd&) = delete;
};

// zstd's compressor/decompressor state must be freed with zstd's own
// functions; unique_ptr with these deleters does that automatically.
struct CCtxFree { void operator()(ZSTD_CCtx* c) const { ZSTD_freeCCtx(c); } };
struct DCtxFree { void operator()(ZSTD_DCtx* d) const { ZSTD_freeDCtx(d); } };

std::string os_error(const std::string& what, const fs::path& path) {
    return what + " " + path.string() + ": " + std::strerror(errno);
}

// Read up to `size` bytes, stopping early only at the end of the file.
// (read() may return less than asked for even mid-file, so loop.)
bool read_full(int fd, char* data, size_t size, size_t& got, const fs::path& path, std::string& error) {
    got = 0;
    while (got < size) {
        ssize_t n = ::read(fd, data + got, size - got);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            error = os_error("cannot read", path);
            return false;
        }
        if (n == 0) {
            break;  // end of file
        }
        got += static_cast<size_t>(n);
    }
    return true;
}

// write() may take only part of the data, so keep going until it's all out.
bool write_all(int fd, const char* data, size_t size, const fs::path& path, std::string& error) {
    size_t written = 0;
    while (written < size) {
        ssize_t n = ::write(fd, data + written, size - written);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            error = os_error("cannot write", path);
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return true;
}

// A rename or delete changes the FOLDER, not the file - so the folder has
// to be fsynced too, or a power cut could undo the name change.
bool fsync_dir(const fs::path& dir, std::string& error) {
    Fd d(::open(dir.c_str(), O_RDONLY | O_CLOEXEC));
    if (d.fd < 0) {
        error = os_error("cannot open folder", dir);
        return false;
    }
    if (::fsync(d.fd) != 0) {
        error = os_error("cannot fsync folder", dir);
        return false;
    }
    return true;
}

// Step 1 + 2: stream `source` into `dest` as zstd, then fsync `dest`.
bool compress_to(const fs::path& source, const fs::path& dest, int level, std::string& error) {
    Fd in(::open(source.c_str(), O_RDONLY | O_CLOEXEC));
    if (in.fd < 0) {
        error = os_error("cannot open", source);
        return false;
    }
    struct stat info{};
    if (::fstat(in.fd, &info) != 0) {
        error = os_error("cannot check size of", source);
        return false;
    }

    std::unique_ptr<ZSTD_CCtx, CCtxFree> cctx(ZSTD_createCCtx());
    if (!cctx) {
        error = "cannot create zstd compressor (out of memory?)";
        return false;
    }
    // checksumFlag: store a fingerprint of the content inside the file,
    //   which every later read checks automatically.
    // pledgedSrcSize: tell zstd the exact size up front. It is stored in
    //   the file (so readers know the size before decompressing), and if
    //   the file grows or shrinks while we read it - i.e. something is
    //   still writing to it - compression fails instead of producing a
    //   copy of a moving target.
    size_t rc = ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_compressionLevel, level);
    if (!ZSTD_isError(rc)) rc = ZSTD_CCtx_setParameter(cctx.get(), ZSTD_c_checksumFlag, 1);
    if (!ZSTD_isError(rc)) rc = ZSTD_CCtx_setPledgedSrcSize(cctx.get(), static_cast<unsigned long long>(info.st_size));
    if (ZSTD_isError(rc)) {
        error = std::string("zstd setup failed (level ") + std::to_string(level) + "): " + ZSTD_getErrorName(rc);
        return false;
    }

    // O_TRUNC: a .tmp left by an earlier crash is simply overwritten.
    Fd out(::open(dest.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644));
    if (out.fd < 0) {
        error = os_error("cannot create", dest);
        return false;
    }

    // zstd's recommended buffer sizes (about 128 KB each), so memory use
    // stays the same however big the file is.
    std::vector<char> in_buf(ZSTD_CStreamInSize());
    std::vector<char> out_buf(ZSTD_CStreamOutSize());

    while (true) {
        size_t got = 0;
        if (!read_full(in.fd, in_buf.data(), in_buf.size(), got, source, error)) {
            return false;
        }
        // A short read means we reached the end: tell zstd to finish the
        // file (write its last block and the checksum).
        bool last = got < in_buf.size();
        ZSTD_EndDirective mode = last ? ZSTD_e_end : ZSTD_e_continue;
        ZSTD_inBuffer input{in_buf.data(), got, 0};

        // zstd may need several rounds to take in one piece of input (or
        // to write out everything at the end), so loop until it's done.
        bool finished = false;
        while (!finished) {
            ZSTD_outBuffer output{out_buf.data(), out_buf.size(), 0};
            size_t remaining = ZSTD_compressStream2(cctx.get(), &output, &input, mode);
            if (ZSTD_isError(remaining)) {
                error = std::string("compression failed for ") + source.string() + ": " + ZSTD_getErrorName(remaining);
                return false;
            }
            if (!write_all(out.fd, out_buf.data(), output.pos, dest, error)) {
                return false;
            }
            finished = last ? (remaining == 0) : (input.pos == input.size);
        }
        if (last) {
            break;
        }
    }

    if (::fsync(out.fd) != 0) {
        error = os_error("cannot fsync", dest);
        return false;
    }
    return true;
}

// Step 3: does `compressed` decompress to exactly the bytes in `original`?
// Reads both side by side in pieces, so it works for any file size. zstd
// also checks the stored checksum when it reaches the end of the file.
bool matches(const fs::path& original, const fs::path& compressed, std::string& error) {
    Fd orig(::open(original.c_str(), O_RDONLY | O_CLOEXEC));
    if (orig.fd < 0) {
        error = os_error("cannot open", original);
        return false;
    }
    Fd comp(::open(compressed.c_str(), O_RDONLY | O_CLOEXEC));
    if (comp.fd < 0) {
        error = os_error("cannot open", compressed);
        return false;
    }

    std::unique_ptr<ZSTD_DCtx, DCtxFree> dctx(ZSTD_createDCtx());
    if (!dctx) {
        error = "cannot create zstd decompressor (out of memory?)";
        return false;
    }

    std::vector<char> in_buf(ZSTD_DStreamInSize());
    std::vector<char> out_buf(ZSTD_DStreamOutSize());
    std::vector<char> orig_buf(ZSTD_DStreamOutSize());

    // 0 once zstd has seen a complete, checksum-verified end of file;
    // anything else afterwards means the compressed file is cut short.
    size_t last_ret = 1;
    bool any_input = false;

    while (true) {
        size_t got = 0;
        if (!read_full(comp.fd, in_buf.data(), in_buf.size(), got, compressed, error)) {
            return false;
        }
        if (got == 0) {
            break;
        }
        any_input = true;

        // Keep going while there is input left, and also while the output
        // buffer came back completely full - zstd may then still be
        // holding more decompressed data for us.
        ZSTD_inBuffer input{in_buf.data(), got, 0};
        bool output_full = false;
        while (input.pos < input.size || output_full) {
            ZSTD_outBuffer output{out_buf.data(), out_buf.size(), 0};
            size_t ret = ZSTD_decompressStream(dctx.get(), &output, &input);
            if (ZSTD_isError(ret)) {
                error = std::string("cannot decompress ") + compressed.string() + ": " + ZSTD_getErrorName(ret);
                return false;
            }
            last_ret = ret;
            output_full = output.pos == output.size;

            // Compare what came out with the same stretch of the original.
            size_t orig_got = 0;
            if (!read_full(orig.fd, orig_buf.data(), output.pos, orig_got, original, error)) {
                return false;
            }
            if (orig_got != output.pos || std::memcmp(orig_buf.data(), out_buf.data(), output.pos) != 0) {
                error = compressed.string() + " does not match " + original.string();
                return false;
            }
        }
    }

    if (!any_input || last_ret != 0) {
        error = compressed.string() + " is incomplete (cut short)";
        return false;
    }

    // Everything decompressed matched - but the original must not have
    // anything left over beyond that.
    char extra = 0;
    size_t extra_got = 0;
    if (!read_full(orig.fd, &extra, 1, extra_got, original, error)) {
        return false;
    }
    if (extra_got != 0) {
        error = compressed.string() + " does not match " + original.string() + " (original is longer)";
        return false;
    }
    return true;
}

}  // namespace

bool compress_file(const fs::path& source, int level, std::string& error) {
    fs::path finished = source;
    finished += ".zst";
    fs::path tmp = finished;
    tmp += ".tmp";
    fs::path dir = source.parent_path().empty() ? fs::path(".") : source.parent_path();

    // zstd doesn't reject an out-of-range level - it quietly clamps it to
    // the nearest valid one - so a typo like 190 would silently run at 22.
    // Check it ourselves, before touching any files.
    if (level < 1 || level > ZSTD_maxCLevel()) {
        error = "compression level " + std::to_string(level) + " is outside 1-" + std::to_string(ZSTD_maxCLevel());
        return false;
    }

    std::error_code ec;
    if (!fs::is_regular_file(source, ec)) {
        error = "no such file: " + source.string();
        return false;
    }

    if (fs::exists(finished, ec)) {
        // An earlier run got as far as the rename, then stopped before
        // deleting the original. Only finish the job if the two really are
        // the same data; anything else needs a human to look.
        if (!matches(source, finished, error)) {
            error = "found " + finished.string() + " that does not match the original; left both untouched (" + error + ")";
            return false;
        }
    } else {
        if (!compress_to(source, tmp, level, error) || !matches(source, tmp, error)) {
            fs::remove(tmp, ec);  // best effort; a leftover .tmp is overwritten next time anyway
            return false;
        }
        // rename is all-at-once: readers see either no .zst or a complete one.
        fs::rename(tmp, finished, ec);
        if (ec) {
            error = "cannot rename " + tmp.string() + ": " + ec.message();
            return false;
        }
        if (!fsync_dir(dir, error)) {
            return false;
        }
    }

    fs::remove(source, ec);
    if (ec) {
        error = "cannot delete " + source.string() + ": " + ec.message();
        return false;
    }
    return fsync_dir(dir, error);
}
