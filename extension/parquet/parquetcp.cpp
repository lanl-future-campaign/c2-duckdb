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
#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/string_util.hpp"
#else
#include "parquet-amalgamation.hpp"
#endif

#include <fcntl.h>
#include <fstream>
#include <stdio.h>
#include <string>
#include <unistd.h>
#include <vector>

namespace {

class ParquetFooterHandle {
public:
	ParquetFooterHandle(const std::string &path, int fd, int64_t offset, int64_t size)
	    : path(path), fd(fd), offset(offset), size(size) {
	}
	~ParquetFooterHandle() {
		close(fd);
	}

	std::string path;
	int fd;
	int64_t offset;
	int64_t size;
};

class ParquetFooterReader {
public:
	ParquetFooterReader() : default_footer_size(1 << 10) {
	}
	~ParquetFooterReader() {};

	std::unique_ptr<ParquetFooterHandle> Open(const std::string &path) {
		std::vector<std::string> parameters = duckdb::StringUtil::Split(path, ":");
		int64_t offset;
		int64_t size;
		if (parameters.size() != 3 || (offset = atoll(parameters[1].c_str())) < 0 ||
		    (size = atoll(parameters[2].c_str())) < 0) {
			throw duckdb::IOException("Invalid file name %s", path);
		}
		int fd = open(parameters[0].c_str(), O_RDONLY);
		if (fd == -1) {
			throw duckdb::IOException("Cannot open file %s: %s", parameters[0], strerror(errno));
		}
		return duckdb::make_unique<ParquetFooterHandle>(path, fd, offset, size);
	}

	int64_t Read(ParquetFooterHandle &handle, void *buffer, size_t buffer_size) {
		int64_t footer_size = default_footer_size;
		if (buffer_size < footer_size) {
			throw duckdb::IOException("Insufficient buffer size: required=%lld, supplied=%lld", footer_size,
			                          buffer_size);
		}
		int64_t bytes_read = pread(handle.fd, buffer, footer_size, handle.offset + handle.size - footer_size);
		if (bytes_read == -1) {
			throw duckdb::IOException("Could not read from file %s: %s", handle.path, strerror(errno));
		}
		if (bytes_read != footer_size) {
			throw duckdb::IOException("Could not read all bytes from file %s: wanted=%lld read=%lld", handle.path,
			                          footer_size, bytes_read);
		}
		return footer_size;
	}

private:
	int64_t default_footer_size;
};

class DestinationHandle {
public:
	DestinationHandle(const std::string &path, int fd, int64_t offset, int64_t size)
	    : path(path), fd(fd), offset(offset), size(size), pos(0) {
	}
	~DestinationHandle() {
		close(fd);
	}

	std::string path;
	int fd;
	int64_t offset;
	int64_t size;
	int64_t pos; // Current writer position
};

class Replicator {
public:
	explicit Replicator(const std::vector<std::string> &dests)
	    : footer_reader(new ParquetFooterReader()), cur_dest(nullptr), nxt(0), dests(dests.data()), n(dests.size()) {
	}
	~Replicator() {
		CloseCurrentDestination();
	}

	void CloseCurrentDestination() {
		if (cur_dest) {
			fprintf(stderr, "%s:0:%llu\n", dests[nxt - 1].c_str(), static_cast<unsigned long long>(cur_dest->pos));
			delete cur_dest;
			cur_dest = nullptr;
		}
	}

	void OpenNextDestination() {
		if (nxt >= n) {
			throw duckdb::IOException("Insufficient destination space");
		}
		const std::string &path = dests[nxt];
		std::vector<std::string> parameters = duckdb::StringUtil::Split(path, ":");
		int64_t offset;
		int64_t size;
		if (parameters.size() != 3 || (offset = atoll(parameters[1].c_str())) < 0 ||
		    (size = atoll(parameters[2].c_str())) < 0) {
			throw duckdb::IOException("Invalid file name %s", path);
		}
		int fd = open(parameters[0].c_str(), O_WRONLY | O_CREAT, 0644);
		if (fd == -1) {
			throw duckdb::IOException("Cannot open file %s: %s", parameters[0], strerror(errno));
		}
		cur_dest = new DestinationHandle(path, fd, offset, size);
		nxt++;
	}

	void Process(const std::string &src) {
		std::unique_ptr<ParquetFooterHandle> handle = footer_reader->Open(src);
		int64_t footer_size = footer_reader->Read(*handle, buf, sizeof(buf));
		if (!cur_dest) {
			OpenNextDestination();
		} else if (cur_dest->pos + footer_size > cur_dest->size) {
			CloseCurrentDestination();
			OpenNextDestination();
		}
		int64_t bytes_written = pwrite(cur_dest->fd, buf, footer_size, cur_dest->offset + cur_dest->pos);
		if (bytes_written == -1) {
			throw duckdb::IOException("Could not write data to file %s: %s", cur_dest->path, strerror(errno));
		}
		if (bytes_written != footer_size) {
			throw duckdb::IOException("Could not write all bytes to file %s: wanted=%lld read=%lld", cur_dest->path,
			                          footer_size, bytes_written);
		}
		cur_dest->pos += bytes_written;
	}

	void Finish() {
		CloseCurrentDestination();
	}

private:
	char buf[4096];
	std::unique_ptr<ParquetFooterReader> footer_reader;
	DestinationHandle *cur_dest;
	size_t nxt; // Index of the next destination location to be used
	// Constant after construction
	const std::string *const dests; // Available destinations
	// Number of available destinations
	const size_t n;
};

} // namespace

/*
 * Example: ./parquetcp --from=container:offset:size --to=container:offset:size
 */
int main(int argc, char *argv[]) {
	std::vector<std::string> sources, dests;
	for (int i = 1; i < argc; i++) {
		if (strncmp(argv[i], "--from=", strlen("--from=")) == 0) {
			sources.push_back(argv[i] + strlen("--from="));
		} else if (strncmp(argv[i], "--to=", strlen("--to=")) == 0) {
			dests.push_back(argv[i] + strlen("--to="));
		} else {
			fprintf(stderr, "Invalid argument: use --from and --to to specify input and output locations\n");
			return -1;
		}
	}
	try {
		auto replicator = duckdb::make_unique<Replicator>(dests);
		for (const auto &src : sources) {
			if (src[0] != '^') {
				replicator->Process(src);
			} else {
				std::ifstream in(&src[1]);
				std::string input;
				while (in >> input) {
					replicator->Process(input);
				}
			}
		}
		replicator->Finish();
	} catch (const std::exception &e) {
		fprintf(stderr, "[ERROR] %s\n", e.what());
		return -1;
	}
	return 0;
}
