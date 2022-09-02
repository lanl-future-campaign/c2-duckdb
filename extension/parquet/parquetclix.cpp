#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "parquet_reader.hpp"
#else
#include "parquet-amalgamation.hpp"
#endif

#include <algorithm>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>
#include <unordered_map>

namespace {

struct DeviceFileWrapper : public duckdb::FileHandle {
	DeviceFileWrapper(duckdb::FileSystem &file_system, const std::string &path, int fd, int64_t offset, int64_t size)
	    : FileHandle(file_system, path), fd(fd), offset(offset), size(size) {
	}
	~DeviceFileWrapper() override {
		Close();
	}

	int fd;
	int64_t offset;
	int64_t size;

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

} // namespace

int main(int argc, char *argv[]) {
	std::string filename(argv[1]);

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
	if (argc >= 4) {
		std::vector<std::string> splits = duckdb::StringUtil::Split(argv[3], "=");
		auto entry = name_map.find(splits[0]);
		if (entry == name_map.end()) {
			fprintf(stderr, "Column name not found: %s\n", splits[0].c_str());
			//
		}
		int i = entry->second;
		auto filter = duckdb::make_unique<duckdb::ConstantFilter>(duckdb::ExpressionType::COMPARE_EQUAL,
		                                                          duckdb::Value(splits[1]).CastAs(return_types[i]));
		filters.filters[i] = std::move(filter);
	}

	duckdb::ParquetReaderScanState state;
	reader.InitializeScan(state, column_ids, groups, &filters);
	duckdb::DataChunk output;
	output.Initialize(return_types);
	do {
		output.Reset();
		reader.Scan(state, output);
		output.Print();
	} while (output.size() > 0);
}
