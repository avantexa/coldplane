/*
 * seekpos ABI shim for cross-compiling go-duckdb on Windows.
 *
 * go-duckdb v1.8.2 ships a pre-built libduckdb.a that was compiled with
 * MSYS2 MINGW64 GCC 12.2.0 targeting UCRT.  In UCRT builds, mbstate_t is
 * a struct (_Mbstatet), so std::fpos<mbstate_t> mangles as fpos<_Mbstatet>.
 *
 * Debian's mingw-w64 cross-compiler targets MSVCRT, where mbstate_t is int,
 * so its libstdc++ provides seekpos with fpos<int> — a different symbol.
 *
 * This file provides the UCRT-mangled seekpos with the default libstdc++
 * implementation (return pos_type(-1) = "seek not supported").  The symbol
 * is referenced by DuckDB's httplib::DataSink::data_sink_streambuf vtable
 * but is never actually called at runtime.
 *
 * Windows x64 calling convention for this member function:
 *   RCX = hidden return pointer  (fpos<_Mbstatet> is 16 bytes > 8)
 *   RDX = this
 *   R8  = pointer to fpos<_Mbstatet> pos  (16 bytes, passed by pointer)
 *   R9  = openmode (int)
 */

#include <stdint.h>
#include <string.h>

/* Use GCC asm label to emit the exact mangled symbol name. */
void *__seekpos_ucrt_shim(void *ret, void *self, void *pos, int mode)
    __asm__("_ZNSt15basic_streambufIcSt11char_traitsIcEE7seekposESt4fposI9_MbstatetESt13_Ios_Openmode");

void *__seekpos_ucrt_shim(void *ret, void *self, void *pos, int mode) {
    /* fpos<_Mbstatet> layout: 8-byte streamoff + 8-byte _Mbstatet */
    int64_t neg_one = -1;
    memcpy(ret, &neg_one, 8);   /* streamoff = -1  */
    memset((char *)ret + 8, 0, 8); /* _Mbstatet = {0} */
    return ret;
}
