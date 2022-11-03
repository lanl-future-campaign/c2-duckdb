#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/serializer/buffered_serializer.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "parquet_reader.hpp"
#else
#include "parquet-amalgamation.hpp"
#endif

#include "pthread-helper.h"

#include <algorithm>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <unordered_map>

namespace {

struct DeviceFileWrapper : public duckdb::FileHandle {
	DeviceFileWrapper(duckdb::FileSystem &file_system, const std::string &path, int fd, int64_t offset, int64_t size)
	    : FileHandle(file_system, path), fd(fd), offset(offset), size(size), reads(0), bytes_read(0) {
	}
	~DeviceFileWrapper() override {
		Close();
	}

	int fd;
	int64_t offset;
	int64_t size;
	uint64_t reads;      // Number of read invocations
	uint64_t bytes_read; // Number of bytes read from the file

	void Close() override {
		if (fd != -1) {
			close(fd);
		}
	};
};

class DeviceFileSystem : public duckdb::FileSystem {
public:
	std::unique_ptr<duckdb::FileHandle> OpenFile(const std::string &path, uint8_t flags,
	                                             duckdb::FileLockType lock = DEFAULT_LOCK,
	                                             duckdb::FileCompressionType compression = DEFAULT_COMPRESSION,
	                                             duckdb::FileOpener *opener = nullptr) override {
		std::vector<std::string> parameters = duckdb::StringUtil::Split(path, ":");
		int64_t offset;
		int64_t size;
		if (parameters.size() != 3 || (offset = atoll(parameters[1].c_str())) < 0 ||
		    (size = atoll(parameters[2].c_str())) < 0) {
			throw duckdb::IOException("Invalid file name \"%s\"", path);
		}
		int fd = open(parameters[0].c_str(), O_RDONLY);
		if (fd == -1) {
			throw duckdb::IOException("Cannot open file \"%s\": %s", path, strerror(errno));
		}
		return duckdb::make_unique<DeviceFileWrapper>(*this, path, fd, offset, size);
	}

	int64_t GetFileSize(duckdb::FileHandle &handle) override {
		return static_cast<DeviceFileWrapper &>(handle).size;
	}

	void Read(duckdb::FileHandle &handle, void *buffer, int64_t nr_bytes, idx_t location) override {
		int fd = static_cast<DeviceFileWrapper &>(handle).fd;
		int64_t offset = static_cast<DeviceFileWrapper &>(handle).offset;
		int64_t bytes_read = pread(fd, buffer, nr_bytes, location + offset);
		if (bytes_read == -1) {
			throw duckdb::IOException("Could not read from file \"%s\": %s", handle.path, strerror(errno));
		}
		if (bytes_read != nr_bytes) {
			throw duckdb::IOException("Could not read all bytes from file \"%s\": "
			                          "wanted=%lld read=%lld",
			                          handle.path, nr_bytes, bytes_read);
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
		return "ReadonlyFileSystem";
	}
};

struct ScanState {
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
	bool print;
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
	do {
		output.Reset();
		reader.Scan(state, output);
		hits += output.size();
		if (shared->print && output.size() > 0) {
			if (1) {
				// output.Serialize(ser);
				for (idx_t i = 0; i < output.ColumnCount(); i++) {
					output.data[i].Serialize(output.size(), ser);
				}
				fwrite(ser.blob.data.get(), ser.blob.size, 1, stdout);
				scan->bytes_printed += ser.blob.size;
				fflush(stdout);
				ser.Reset();
			} else {
				output.Print();
			}
		}
	} while (output.size() > 0);
	scan->n += groups.size();
	scan->reads += static_cast<DeviceFileWrapper *>(state.file_handle.get())->reads;
	scan->bytes_read += static_cast<DeviceFileWrapper *>(state.file_handle.get())->bytes_read;
	scan->hits += hits;
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
		t->shared->scan->reads += scan.reads;
		t->shared->scan->bytes_read += scan.bytes_read;
		t->shared->scan->bytes_printed += scan.bytes_printed;
		t->shared->scan->hits += scan.hits;
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
	DeviceFileSystem fs;
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

	std::vector<idx_t> groups;
	for (idx_t i = 0; i < reader.NumRowGroups(); i++) {
		groups.push_back(i);
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

	ScanState scan;
	memset(&scan, 0, sizeof(scan));
	int print = 1;
	{
		const char *env = getenv("Env_print");
		if (env && env[0]) {
			print = atoi(env);
		}
	}
	SharedState shared;
	shared.fs = &fs;
	shared.column_ids = &column_ids;
	shared.return_types = &return_types;
	shared.filters = &filters;
	shared.scan = &scan;
	shared.print = print;
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
	JobScheduler scheduler(j, shared);
	for (int i = 6; i < argc; i++) {
		if (argv[i][0] != '^') {
			scheduler.AddTask(argv[i]);
		} else {
			std::ifstream in(argv[i] + 1);
			std::string input;
			while (in >> input) {
				scheduler.AddTask(input);
			}
		}
	}
	scheduler.Wait();
	if (argc >= 6) {
		fprintf(stderr, "Predicate: %s%s%s\n", argv[3], argv[4], argv[5]);
	}
	fprintf(stderr, "Print results: %d\n", print);
	fprintf(stderr, "Threads: %d\n", j);
	fprintf(stderr, "Total reads: %llu\n", static_cast<unsigned long long>(scan.reads));
	fprintf(stderr, "Total bytes read: %llu\n", static_cast<unsigned long long>(scan.bytes_read));
	fprintf(stderr, "Total bytes printed to stdout: %llu\n", static_cast<unsigned long long>(scan.bytes_printed));
	fprintf(stderr, "Total hits: %llu\n", static_cast<unsigned long long>(scan.hits));
	fprintf(stderr, "Total row groups scanned: %llu\n", static_cast<unsigned long long>(scan.n));
}
