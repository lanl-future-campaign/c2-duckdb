/*
 * Copyright (c) 2021 Triad National Security, LLC, as operator of Los Alamos
 * National Laboratory with the U.S. Department of Energy/National Nuclear
 * Security Administration. All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * with the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * 1. Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 *    this list of conditions and the following disclaimer in the documentation
 *    and/or other materials provided with the distribution.
 * 3. Neither the name of TRIAD, Los Alamos National Laboratory, LANL, the
 *    U.S. Government, nor the names of its contributors may be used to endorse
 *    or promote products derived from this software without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS "AS IS" AND ANY EXPRESS OR
 * IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDERS OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
 * (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
 * LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
#pragma once

#include <fstream>
#include <iostream>
#include <sstream>
#include <stdint.h>
#include <string>

namespace {
// Linux kernel-maintained disk io stats
struct iostats {
	// number of read I/Os processed
	long long read_ops;
	// number of read I/Os merged with in-queue I/O
	long long read_merges;
	// number of sectors read
	long long read_sectors;
	// total wait time for read requests
	long long read_ticks;
	// number of write I/Os processed
	long long write_ops;
	// number of write I/Os merged with in-queue I/O
	long long write_merges;
	// number of sectors written
	long long write_sectors;
	// total wait time for write requests
	long long write_ticks;
};
} // namespace

static inline void GetDiskStats(const char *path, struct iostats *result) {
	std::string scratch_space;
	std::ifstream f(path);
	std::getline(f, scratch_space);
	std::istringstream input(scratch_space);
	input >> result->read_ops;
	input >> result->read_merges;
	input >> result->read_sectors;
	input >> result->read_ticks;
	input >> result->write_ops;
	input >> result->write_merges;
	input >> result->write_sectors;
	input >> result->write_ticks;
}