// ****************************************************************************
// This file originally from:
//     http://web.mit.edu/mmadinot/Desktop/mmadinot/MacData/afs/athena.mit.edu/software/julia_v0.2.0/julia/src/support/MurmurHash3.h
// ****************************************************************************

// MurmurHash3 was written by Austin Appleby, and is placed in the public
// domain. The author hereby disclaims copyright to this source code.

#ifndef MURMURHASH3_H
#define MURMURHASH3_H

//-----------------------------------------------------------------------------
// Platform-specific functions and macros
#include <stdint.h>
#include "c.h"

//-----------------------------------------------------------------------------

void MurmurHash3_x64_128 (const void *key, const Size len, const uint64_t seed, void *out);

//-----------------------------------------------------------------------------

#endif // MURMURHASH3_H
