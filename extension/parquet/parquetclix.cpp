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
#include "duckdb/common/printer.hpp"
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "parquet_reader.hpp"
#else
#include "parquet-amalgamation.hpp"
#endif

#include "iostats.h"
#include "pthread-helper.h"
#include "time.h"

#include <algorithm>
#include <errno.h>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <map>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <unordered_map>

namespace {

struct DeviceFileWrapper : public duckdb::FileHandle {
	DeviceFileWrapper(duckdb::FileSystem &file_system, const std::string &path, int fd, int64_t offset, int64_t size,
	                  int64_t id)
	    : FileHandle(file_system, path), fd(fd), offset(offset), size(size), id(id), reads(0), bytes_read(0) {
	}
	~DeviceFileWrapper() override {
		Close();
	}

	int fd;
	int64_t offset;
	int64_t size;
	int64_t id;
	uint64_t reads;      // Number of read invocations
	uint64_t bytes_read; // Number of bytes read from the file

	void Close() override {
		if (fd != -1) {
			close(fd);
		}
	};
};

template <uint64_t FLAGS_footer_size = 600, bool FLAGS_disable_cache = false, bool FLAGS_fadv_random = true>
class DeviceFileSystem : public duckdb::FileSystem {
public:
	std::unique_ptr<duckdb::FileHandle> OpenFile(const std::string &path, uint8_t flags,
	                                             duckdb::FileLockType lock = DEFAULT_LOCK,
	                                             duckdb::FileCompressionType compression = DEFAULT_COMPRESSION,
	                                             duckdb::FileOpener *opener = nullptr) override {
		std::vector<std::string> parameters = duckdb::StringUtil::Split(path, ":");
		int64_t offset;
		int64_t size;
		if (parameters.size() < 3 || (offset = atoll(parameters[1].c_str())) < 0 ||
		    (size = atoll(parameters[2].c_str())) < 0) {
			throw duckdb::IOException("Invalid file name %s", path);
		}
		int fd = open(parameters[0].c_str(), O_RDONLY);
#ifdef __linux__
		if (FLAGS_fadv_random) {
			int r = posix_fadvise(fd, 0, 0, POSIX_FADV_RANDOM);
			if (r != 0) {
				throw duckdb::IOException("Fail to invoke posix_fadvise");
			}
		}
#endif
		if (fd == -1) {
			throw duckdb::IOException("Cannot open file %s: %s", parameters[0], strerror(errno));
		}
		return duckdb::make_unique<DeviceFileWrapper>(*this, path, fd, offset, size,
		                                              parameters.size() > 3 ? atoll(parameters[3].c_str()) : -1);
	}

	int64_t GetFileSize(duckdb::FileHandle &handle) override {
		return static_cast<DeviceFileWrapper &>(handle).size;
	}

	void Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		if (!FLAGS_disable_cache && static_cast<DeviceFileWrapper &>(handle).id >= 0) {
			uint64_t footer_cache_offset = static_cast<DeviceFileWrapper &>(handle).id * FLAGS_footer_size;
			if (location < static_cast<DeviceFileWrapper &>(handle).size - FLAGS_footer_size) {
				// Non-footer reads
			} else if (footer_cache.size() < footer_cache_offset + FLAGS_footer_size) {
				// No cache available
			} else {
				memcpy(buffer,
				       &footer_cache[0] + footer_cache_offset + location -
				           static_cast<DeviceFileWrapper &>(handle).size + FLAGS_footer_size,
				       nr_bytes);
				return;
			}
		}
		int64_t bytes_read = pread(static_cast<DeviceFileWrapper &>(handle).fd, buffer, nr_bytes,
		                           static_cast<DeviceFileWrapper &>(handle).offset + location);
		if (bytes_read == -1) {
			throw duckdb::IOException("Could not read from file %s: %s", handle.path, strerror(errno));
		}
		if (bytes_read != nr_bytes) {
			throw duckdb::IOException("Could not read all bytes from file %s: wanted=%lld read=%lld", handle.path,
			                          nr_bytes, bytes_read);
		}
		static_cast<DeviceFileWrapper &>(handle).bytes_read += bytes_read;
		static_cast<DeviceFileWrapper &>(handle).reads++;
	}

	bool CanSeek() override {
		return true;
	}

	bool OnDiskFile(duckdb::FileHandle &handle) override {
		return true;
	}

	std::string GetName() const override {
		return "ReadonlyDeviceFileSystem";
	}

	void LoadCache(const std::string *srcs, size_t n) {
		for (int i = 0; i < n; i++) {
			if (!srcs[i].empty()) {
				LoadCacheFromSource(srcs[i]);
			}
		}
	}

private:
	void LoadCacheFromSource(const std::string &path) {
		std::vector<std::string> parameters = duckdb::StringUtil::Split(path, ":");
		int64_t offset;
		int64_t size;
		if (parameters.size() != 3 || (offset = atoll(parameters[1].c_str())) < 0 ||
		    (size = atoll(parameters[2].c_str())) < 0) {
			throw duckdb::IOException("Invalid cache source file name: %s", path);
		}
		int fd = open(parameters[0].c_str(), O_RDONLY);
		if (fd == -1) {
			throw duckdb::IOException("Cannot open file %s: %s", parameters[0], strerror(errno));
		}
		void *const tmp = malloc(size);
		int64_t bytes_read = pread(fd, tmp, size, offset);
		if (bytes_read == size) {
			footer_cache.append((char *)tmp, bytes_read);
			fprintf(stderr, "Loaded %s: %llu bytes\n", path.c_str(), static_cast<unsigned long long>(bytes_read));
		}
		free(tmp);
		close(fd);
		if (bytes_read == -1) {
			throw duckdb::IOException("Could not read from file %s: %s", parameters[0], strerror(errno));
		}
		if (bytes_read != size) {
			throw duckdb::IOException("Could not read all bytes from file %s: wanted=%lld read=%lld", parameters[0],
			                          size, bytes_read);
		}
	}

	std::string footer_cache;
};

struct ScanState {
	double math_sum;
	uint64_t meta_reads;
	uint64_t meta_bytes_read;
	uint64_t reads;
	uint64_t bytes_read;
	uint64_t bytes_printed;
	uint64_t hits;
	uint64_t n;
};

struct SharedState {
	duckdb::FileSystem *fs;
	std::vector<duckdb::column_t> *column_ids;
	std::vector<duckdb::LogicalType> *return_types;
	duckdb::TableFilterSet *filters;
	ScanState *scan;
	bool print_binary;
	bool print;
	bool sum;
};

class JobScheduler {
public:
	JobScheduler(int j, const SharedState &shared);
	~JobScheduler();
	void AddTask(const std::string &filename);
	void Wait();

private:
	SharedState shared_;
	static void RunJob(void *);
	ThreadPool *const pool_;
	// State below protected by cv_;
	port::Mutex mu_;
	port::CondVar cv_;
	int bg_scheduled_;
	int bg_completed_;
};

JobScheduler::JobScheduler(int j, const SharedState &shared)
    : shared_(shared), pool_(new ThreadPool(j)), cv_(&mu_), bg_scheduled_(0), bg_completed_(0) {
}

struct Task {
	JobScheduler *parent;
	SharedState *shared;
	std::string filename;
};

void JobScheduler::AddTask(const std::string &filename) {
	Task *const t = new Task;
	t->filename = filename;
	t->shared = &shared_;
	t->parent = this;
	MutexLock ml(&mu_);
	bg_scheduled_++;
	pool_->Schedule(RunJob, t);
}

void Run(const std::string &filename, ScanState *const scan, SharedState *const shared) {
	std::unique_ptr<duckdb::FileHandle> file = shared->fs->OpenFile(filename, duckdb::FileFlags::FILE_FLAGS_READ);
	// Obtain a ref to the file before giving it to the reader so that we can collect its statistics later
	duckdb::FileHandle *const root_file = file.get();
	duckdb::Allocator allocator;
	duckdb::ParquetReader reader(allocator, std::move(file));
	std::vector<idx_t> groups;
	for (idx_t i = 0; i < reader.NumRowGroups(); i++) {
		groups.push_back(i);
	}
	duckdb::ParquetReaderScanState state;
	reader.InitializeScan(state, *shared->column_ids, groups, shared->filters);
	duckdb::DataChunk output;
	output.Initialize(*shared->return_types);
	duckdb::BufferedSerializer ser(1024 * 1024);
	uint64_t hits = 0;
	float sum = 0;
	do {
		output.Reset();
		reader.Scan(state, output);
		const uint64_t n = output.size();
		if (n) {
			hits += n;
			if (shared->sum) {
				auto &col = output.data[output.ColumnCount() - 1];
				duckdb::VectorData vdata;
				col.Orrify(n, vdata);
				float *data = (float *)vdata.data;
				for (uint64_t i = 0; i < n; i++) {
					sum += data[i];
				}
			} else if (shared->print) {
				if (shared->print_binary) {
					// output.Serialize(ser);
					for (idx_t i = 0; i < output.ColumnCount(); i++) {
						output.data[i].Serialize(n, ser);
					}
					fwrite(ser.blob.data.get(), ser.blob.size, 1, stdout);
					scan->bytes_printed += ser.blob.size;
					fflush(stdout);
					ser.Reset();
				} else {
					std::string out = output.ToString();
					duckdb::Printer::Print(out);
					scan->bytes_printed += out.size();
				}
			}
		} else {
			break;
		}
	} while (1);
	scan->n = groups.size();
	scan->meta_reads = static_cast<DeviceFileWrapper *>(root_file)->reads;
	scan->meta_bytes_read = static_cast<DeviceFileWrapper *>(root_file)->bytes_read;
	scan->reads = static_cast<DeviceFileWrapper *>(state.file_handle.get())->reads;
	scan->bytes_read = static_cast<DeviceFileWrapper *>(state.file_handle.get())->bytes_read;
	scan->hits = hits;
	scan->math_sum += sum;
}

void TryRun(const std::string &filename, ScanState *scan, SharedState *shared) {
	try {
		Run(filename, scan, shared);
	} catch (const std::exception &e) {
		fprintf(stderr, "ERROR scanning %s: %s\n", filename.c_str(), e.what());
	}
}

void JobScheduler::RunJob(void *arg) {
	Task *const t = static_cast<Task *>(arg);
	JobScheduler *const p = t->parent;
	ScanState scan;
	memset(&scan, 0, sizeof(scan));
	TryRun(t->filename, &scan, t->shared);
	{
		MutexLock ml(&p->mu_);
		t->shared->scan->meta_reads += scan.meta_reads;
		t->shared->scan->meta_bytes_read += scan.meta_bytes_read;
		t->shared->scan->reads += scan.reads;
		t->shared->scan->bytes_read += scan.bytes_read;
		t->shared->scan->bytes_printed += scan.bytes_printed;
		t->shared->scan->hits += scan.hits;
		t->shared->scan->math_sum += scan.math_sum;
		t->shared->scan->n += scan.n;
		p->bg_completed_++;
		p->cv_.SignalAll();
	}
	delete t;
}

void JobScheduler::Wait() {
	MutexLock ml(&mu_);
	while (bg_completed_ < bg_scheduled_) {
		cv_.Wait();
	}
}

JobScheduler::~JobScheduler() {
	{
		MutexLock ml(&mu_);
		while (bg_completed_ < bg_scheduled_) {
			cv_.Wait();
		}
	}
	delete pool_;
}

} // namespace

/*
 * Example: ./parquetclix metadata_file 'ID,x,y,z' ke '>' 0.01 [contain:offset:size] ...
 */
int main(int argc, char *argv[]) {
	if (argc < 2) {
		fprintf(stderr, "Too few arguments: no metadata input file\n");
		return -1;
	}
	std::string filename(argv[1]);
	if (argc < 3) {
		fprintf(stderr, "Too few arguments: no column specification\n");
		return -1;
	}
	bool read_all_columns = false;
	if (std::string(argv[2]) == "*") {
		read_all_columns = true;
	}
	std::vector<std::string> column_names = duckdb::StringUtil::Split(argv[2], ",");

	duckdb::Allocator allocator;
	DeviceFileSystem<> fs;
	std::unique_ptr<duckdb::FileHandle> file = fs.OpenFile(filename, duckdb::FileFlags::FILE_FLAGS_READ);
	duckdb::ParquetReader reader(allocator, std::move(file));
	//-----------------
	// 0: ID  uint64_t
	// 1: x   float
	// 2: y   float
	// 3: z   float
	// 4: ke  float
	//------------------
	std::vector<duckdb::column_t> column_ids;
	std::vector<duckdb::LogicalType> return_types;
	std::unordered_map<std::string, int> name_map; // e.g., ID->0, ke->1
	for (idx_t i = 0; i < reader.names.size(); ++i) {
		const std::string &colname = reader.names[i];
		if (read_all_columns || std::find(column_names.begin(), column_names.end(), colname) != column_names.end()) {
			name_map[colname] = column_ids.size();
			column_ids.push_back(i);
			return_types.push_back(reader.return_types[i]);
		}
	}
	if (column_ids.empty()) {
		fprintf(stderr, "No columns found that match the query\n");
	}

	duckdb::TableFilterSet filters;
	if (argc >= 6) {
		auto entry = name_map.find(argv[3]);
		if (entry == name_map.end()) {
			fprintf(stderr, "Column name not found: %s\n", argv[3]);
			return -1;
		}
		int i = entry->second;
		duckdb::ExpressionType exp;
		switch (argv[4][0]) {
		case '>':
			exp = duckdb::ExpressionType::COMPARE_GREATERTHAN;
			break;
		case '=':
			exp = duckdb::ExpressionType::COMPARE_EQUAL;
			break;
		case '<':
			exp = duckdb::ExpressionType::COMPARE_LESSTHAN;
			break;
		default:
			fprintf(stderr, "Unknown expression: %s\n", argv[4]);
			return -1;
		}
		auto filter = duckdb::make_unique<duckdb::ConstantFilter>(exp, duckdb::Value(argv[5]).CastAs(return_types[i]));
		filters.filters[i] = std::move(filter);
	}
	// Print in raw binary format without converting to human friendly strings
	int print_binary = 1;
	{
		const char *env = getenv("Env_print_binary");
		if (env && env[0]) {
			print_binary = atoi(env);
		}
	}
	int print = 1;
	{
		const char *env = getenv("Env_print");
		if (env && env[0]) {
			print = atoi(env);
		}
	}
	int sum = 0;
	{
		const char *env = getenv("Env_sum");
		if (env && env[0]) {
			sum = atoi(env);
		}
	}
	ScanState scan;
	memset(&scan, 0, sizeof(scan));
	SharedState shared;
	shared.fs = &fs;
	shared.column_ids = &column_ids;
	shared.return_types = &return_types;
	shared.filters = &filters;
	shared.scan = &scan;
	shared.print_binary = print_binary;
	shared.print = print;
	shared.sum = sum;
	int j = 32;
	{
		const char *env = getenv("Env_jobs");
		if (env && env[0]) {
			j = atoi(env);
			if (j < 1) {
				j = 1;
			}
		}
	}
	std::map<std::string, struct iostats> diskstats;
	{
		const char *env = getenv("Env_mon_disks");
		if (env && env[0]) {
#ifdef __linux__
			std::vector<std::string> disks = duckdb::StringUtil::Split(env, ",");
			for (const auto &disk : disks) {
				char path[50];
				snprintf(path, sizeof(path), "/sys/block/%s/stat", disk.c_str());
				struct iostats stats;
				memset(&stats, 0, sizeof(stats));
				GetDiskStats(path, &stats);
				diskstats.emplace(disk, stats);
			}
#else
			fprintf(stderr, "WARN: disk stats mon not enabled\n");
#endif
		}
	}
	std::vector<std::string> cachefiles;
	{
		const char *env = getenv("Env_cache_files");
		if (env && env[0]) {
			cachefiles = duckdb::StringUtil::Split(env, ",");
		}
	}
	JobScheduler scheduler(j, shared);
	const uint64_t start = CurrentMicros();
	{
		if (!cachefiles.empty()) {
			fs.LoadCache(cachefiles.data(), cachefiles.size());
		}
		unsigned long long fileid = 0;
		char tmp[100];
		for (int i = 6; i < argc; i++) {
			if (argv[i][0] != '^') {
				snprintf(tmp, sizeof(tmp), "%s:%llu", argv[i], fileid++);
				scheduler.AddTask(tmp);
			} else {
				std::ifstream in(argv[i] + 1);
				std::string input;
				while (in >> input) {
					snprintf(tmp, sizeof(tmp), "%s:%llu", input.c_str(), fileid++);
					scheduler.AddTask(tmp);
				}
			}
		}
		scheduler.Wait();
	}
	const uint64_t end = CurrentMicros();
	if (argc >= 6) {
		fprintf(stderr, "Predicate: %s%s%s\n", argv[3], argv[4], argv[5]);
	}
	fprintf(stderr, "Query time: %.2f s\n", double(end - start) / 1000000);
	if (sum)
		fprintf(stderr, "Exec sum(ke): %d\n", sum);
	if (print) {
		fprintf(stderr, "Print results: %d\n", print);
		fprintf(stderr, "Print in binary form: %d\n", print_binary);
	}
	fprintf(stderr, "Threads: %d\n", j);
	fprintf(stderr, "Total meta reads: %llu\n", static_cast<unsigned long long>(scan.meta_reads));
	fprintf(stderr, "Total meta bytes read: %llu\n", static_cast<unsigned long long>(scan.meta_bytes_read));
	fprintf(stderr, "Total reads: %llu\n", static_cast<unsigned long long>(scan.reads));
	fprintf(stderr, "Total bytes read: %llu\n", static_cast<unsigned long long>(scan.bytes_read));
	fprintf(stderr, "Total bytes printed to stdout: %llu\n", static_cast<unsigned long long>(scan.bytes_printed));
	fprintf(stderr, "Total hits: %llu\n", static_cast<unsigned long long>(scan.hits));
	fprintf(stderr, "Total row groups scanned: %llu\n", static_cast<unsigned long long>(scan.n));
	fprintf(stderr, "Math sum: %.3f\n", scan.math_sum);
	{
#ifdef __linux__
		long long total_ops = 0, total_sectors = 0, total_ticks = 0, diff = 0;
		for (const auto &it : diskstats) {
			char path[50];
			snprintf(path, sizeof(path), "/sys/block/%s/stat", it.first.c_str());
			struct iostats stats;
			memset(&stats, 0, sizeof(stats));
			GetDiskStats(path, &stats);
			diff = stats.read_ops - it.second.read_ops;
			fprintf(stderr, "%s_read_ops: %lld\n", path, diff);
			total_ops += diff;
			diff = stats.read_sectors - it.second.read_sectors;
			fprintf(stderr, "%s_read_sectors: %lld\n", path, diff);
			total_sectors += diff;
			diff = stats.read_ticks - it.second.read_ticks;
			fprintf(stderr, "%s_read_ticks: %lld ms\n", path, diff);
			total_ticks += diff;
		}
		fprintf(stderr, "Total_read_ops: %lld\n", total_ops);
		fprintf(stderr, "Total_read_sectors: %lld\n", total_sectors);
		fprintf(stderr, "Total_read_ticks: %lld ms\n", total_ticks);
#endif
	}
	return 0;
}
