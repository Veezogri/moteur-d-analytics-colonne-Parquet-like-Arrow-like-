// Columnar Analytics Engine
// Author: RIAL Fares
// File format I/O implementation

#include "format.h"
#include "encoding.h"
#include <fstream>
#include <iostream>
#include <stdexcept>
#include <cstring>
#include <algorithm>
#include <limits>

namespace columnar {

// Schema utilities
size_t Schema::columnIndex(const std::string& name) const {
    for (size_t i = 0; i < columns.size(); i++) {
        if (columns[i].name == name) {
            return i;
        }
    }
    throw std::runtime_error("Column not found: " + name);
}

bool Schema::hasColumn(const std::string& name) const {
    return std::any_of(columns.begin(), columns.end(),
                      [&name](const ColumnSchema& col) { return col.name == name; });
}

// Write helpers
static void writeUInt32(std::ofstream& out, uint32_t value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

static void writeUInt64(std::ofstream& out, uint64_t value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

static void writeInt32(std::ofstream& out, int32_t value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

static void writeInt64(std::ofstream& out, int64_t value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

static void writeUInt16(std::ofstream& out, uint16_t value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

static void writeUInt8(std::ofstream& out, uint8_t value) {
    out.write(reinterpret_cast<const char*>(&value), sizeof(value));
}

// Read helpers
static uint32_t readUInt32(std::ifstream& in) {
    uint32_t value;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

static uint64_t readUInt64(std::ifstream& in) {
    uint64_t value;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

static int32_t readInt32(std::ifstream& in) {
    int32_t value;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

static int64_t readInt64(std::ifstream& in) {
    int64_t value;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

static uint16_t readUInt16(std::ifstream& in) {
    uint16_t value;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

static uint8_t readUInt8(std::ifstream& in) {
    uint8_t value;
    in.read(reinterpret_cast<char*>(&value), sizeof(value));
    return value;
}

// FileWriter implementation
struct FileWriter::Impl {
    std::ofstream file;
    Schema schema;
    std::vector<RowGroupMeta> row_groups;
    std::vector<std::vector<uint8_t>> pending_columns;
    std::vector<PageStats> pending_stats;
    uint32_t pending_rows = 0;
    uint32_t total_rows = 0;

    Impl(const std::string& path, Schema s) : schema(std::move(s)) {
        file.open(path, std::ios::binary | std::ios::trunc);
        if (!file) {
            throw std::runtime_error("Failed to open file for writing: " + path);
        }

        writeUInt32(file, FILE_MAGIC);
        writeUInt16(file, FORMAT_VERSION_MAJOR);
        writeUInt16(file, FORMAT_VERSION_MINOR);

        pending_columns.resize(schema.columns.size());
        pending_stats.resize(schema.columns.size());
    }

    PageStats computeStatsInt32(const std::vector<int32_t>& values) {
        PageStats stats;
        stats.null_count = 0;
        stats.distinct_count_estimate = 0;

        if (!values.empty()) {
            int32_t min_val = *std::min_element(values.begin(), values.end());
            int32_t max_val = *std::max_element(values.begin(), values.end());
            stats.min_int = min_val;
            stats.max_int = max_val;
        }

        return stats;
    }

    PageStats computeStatsInt64(const std::vector<int64_t>& values) {
        PageStats stats;
        stats.null_count = 0;
        stats.distinct_count_estimate = 0;

        if (!values.empty()) {
            int64_t min_val = *std::min_element(values.begin(), values.end());
            int64_t max_val = *std::max_element(values.begin(), values.end());
            stats.min_int = min_val;
            stats.max_int = max_val;
        }

        return stats;
    }

    void writePageHeader(const PageHeader& header) {
        writeUInt32(file, header.uncompressed_size);
        writeUInt32(file, header.compressed_size);
        writeUInt32(file, header.num_values);
        writeUInt8(file, static_cast<uint8_t>(header.encoding));

        bool has_stats = header.stats.min_int.has_value() || header.stats.max_int.has_value();
        writeUInt8(file, has_stats ? 1 : 0);

        if (has_stats) {
            writeUInt8(file, header.stats.min_int.has_value() ? 1 : 0);
            if (header.stats.min_int.has_value()) {
                writeInt64(file, header.stats.min_int.value());
            }

            writeUInt8(file, header.stats.max_int.has_value() ? 1 : 0);
            if (header.stats.max_int.has_value()) {
                writeInt64(file, header.stats.max_int.value());
            }

            writeUInt32(file, header.stats.null_count);
        }
    }

    void writeMetadata(const FileMetadata& metadata) {
        writeUInt32(file, static_cast<uint32_t>(metadata.schema.columns.size()));

        for (const auto& col : metadata.schema.columns) {
            writeUInt32(file, static_cast<uint32_t>(col.name.size()));
            file.write(col.name.data(), col.name.size());
            writeUInt8(file, static_cast<uint8_t>(col.type));
            writeUInt8(file, static_cast<uint8_t>(col.encoding));
        }

        writeUInt32(file, static_cast<uint32_t>(metadata.row_groups.size()));

        for (const auto& rg : metadata.row_groups) {
            writeUInt32(file, rg.num_rows);
            writeUInt32(file, static_cast<uint32_t>(rg.column_chunks.size()));

            for (const auto& cc : rg.column_chunks) {
                writeUInt64(file, cc.file_offset);
                writeUInt64(file, cc.total_size);
                writeUInt32(file, static_cast<uint32_t>(cc.page_headers.size()));

                for (const auto& ph : cc.page_headers) {
                    writePageHeader(ph);
                }
            }
        }

        writeUInt32(file, metadata.total_rows);
    }
};

FileWriter::FileWriter(const std::string& path, Schema schema)
    : impl_(std::make_unique<Impl>(path, std::move(schema))) {}

FileWriter::~FileWriter() {
    if (impl_) {
        try {
            close();
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to close file in destructor: " << e.what() << std::endl;
        } catch (...) {
            std::cerr << "Warning: Unknown error closing file in destructor" << std::endl;
        }
    }
}

void FileWriter::writeInt32Column(size_t col_idx, const std::vector<int32_t>& values) {
    if (col_idx >= impl_->schema.columns.size()) {
        throw std::runtime_error("Invalid column index");
    }

    if (impl_->schema.columns[col_idx].type != ColumnType::INT32) {
        throw std::runtime_error("Column type mismatch");
    }

    if (impl_->pending_rows > 0 && values.size() != impl_->pending_rows) {
        throw std::runtime_error("All columns must have same number of rows");
    }

    impl_->pending_rows = static_cast<uint32_t>(values.size());

    EncodingType enc = impl_->schema.columns[col_idx].encoding;
    std::vector<uint8_t> encoded;

    switch (enc) {
    case EncodingType::PLAIN:
        encoded.resize(values.size() * sizeof(int32_t));
        std::memcpy(encoded.data(), values.data(), encoded.size());
        break;
    case EncodingType::RLE:
        encoded = RLEEncoder::encodeInt32(values);
        break;
    case EncodingType::DELTA:
        encoded = DeltaEncoder::encodeInt32(values);
        break;
    default:
        throw std::runtime_error("Unsupported encoding for INT32");
    }

    impl_->pending_columns[col_idx] = std::move(encoded);
    impl_->pending_stats[col_idx] = impl_->computeStatsInt32(values);
}

void FileWriter::writeInt64Column(size_t col_idx, const std::vector<int64_t>& values) {
    if (col_idx >= impl_->schema.columns.size()) {
        throw std::runtime_error("Invalid column index");
    }

    if (impl_->schema.columns[col_idx].type != ColumnType::INT64) {
        throw std::runtime_error("Column type mismatch");
    }

    if (impl_->pending_rows > 0 && values.size() != impl_->pending_rows) {
        throw std::runtime_error("All columns must have same number of rows");
    }

    impl_->pending_rows = static_cast<uint32_t>(values.size());

    EncodingType enc = impl_->schema.columns[col_idx].encoding;
    std::vector<uint8_t> encoded;

    switch (enc) {
    case EncodingType::PLAIN:
        encoded.resize(values.size() * sizeof(int64_t));
        std::memcpy(encoded.data(), values.data(), encoded.size());
        break;
    case EncodingType::RLE:
        encoded = RLEEncoder::encodeInt64(values);
        break;
    case EncodingType::DELTA:
        encoded = DeltaEncoder::encodeInt64(values);
        break;
    default:
        throw std::runtime_error("Unsupported encoding for INT64");
    }

    impl_->pending_columns[col_idx] = std::move(encoded);
    impl_->pending_stats[col_idx] = impl_->computeStatsInt64(values);
}

void FileWriter::writeStringColumn(size_t col_idx, const std::vector<std::string>& values) {
    if (col_idx >= impl_->schema.columns.size()) {
        throw std::runtime_error("Invalid column index");
    }

    if (impl_->schema.columns[col_idx].type != ColumnType::STRING) {
        throw std::runtime_error("Column type mismatch");
    }

    if (impl_->pending_rows > 0 && values.size() != impl_->pending_rows) {
        throw std::runtime_error("All columns must have same number of rows");
    }

    impl_->pending_rows = static_cast<uint32_t>(values.size());

    EncodingType enc = impl_->schema.columns[col_idx].encoding;
    std::vector<uint8_t> encoded;

    switch (enc) {
    case EncodingType::PLAIN: {
        std::vector<uint32_t> offsets;
        offsets.reserve(values.size() + 1);
        uint32_t current_offset = 0;

        for (const auto& str : values) {
            offsets.push_back(current_offset);
            current_offset += static_cast<uint32_t>(str.size());
        }
        offsets.push_back(current_offset);

        encoded.resize(offsets.size() * sizeof(uint32_t) + current_offset);
        std::memcpy(encoded.data(), offsets.data(), offsets.size() * sizeof(uint32_t));

        size_t data_offset = offsets.size() * sizeof(uint32_t);
        for (const auto& str : values) {
            std::memcpy(encoded.data() + data_offset, str.data(), str.size());
            data_offset += str.size();
        }
        break;
    }
    case EncodingType::DICTIONARY: {
        DictionaryEncoder encoder;
        encoded = encoder.encode(values);
        break;
    }
    default:
        throw std::runtime_error("Unsupported encoding for STRING");
    }

    impl_->pending_columns[col_idx] = std::move(encoded);
    impl_->pending_stats[col_idx] = PageStats{};
}

void FileWriter::flushRowGroup() {
    if (impl_->pending_rows == 0) {
        return;
    }

    RowGroupMeta rg_meta;
    rg_meta.num_rows = impl_->pending_rows;
    rg_meta.column_chunks.resize(impl_->schema.columns.size());

    for (size_t col_idx = 0; col_idx < impl_->schema.columns.size(); col_idx++) {
        ColumnChunkMeta cc_meta;
        cc_meta.file_offset = static_cast<uint64_t>(impl_->file.tellp());

        const auto& encoded = impl_->pending_columns[col_idx];
        const auto& stats = impl_->pending_stats[col_idx];

        PageHeader ph;
        ph.uncompressed_size = static_cast<uint32_t>(encoded.size());
        ph.compressed_size = static_cast<uint32_t>(encoded.size());
        ph.num_values = impl_->pending_rows;
        ph.encoding = impl_->schema.columns[col_idx].encoding;
        ph.stats = stats;

        impl_->writePageHeader(ph);
        impl_->file.write(reinterpret_cast<const char*>(encoded.data()), encoded.size());

        cc_meta.total_size = static_cast<uint64_t>(impl_->file.tellp()) - cc_meta.file_offset;
        cc_meta.page_headers.push_back(ph);

        rg_meta.column_chunks[col_idx] = cc_meta;
    }

    impl_->row_groups.push_back(rg_meta);
    impl_->total_rows += impl_->pending_rows;

    impl_->pending_columns.clear();
    impl_->pending_columns.resize(impl_->schema.columns.size());
    impl_->pending_stats.clear();
    impl_->pending_stats.resize(impl_->schema.columns.size());
    impl_->pending_rows = 0;
}

void FileWriter::close() {
    if (!impl_->file.is_open()) {
        return;
    }

    flushRowGroup();

    FileMetadata metadata;
    metadata.schema = impl_->schema;
    metadata.row_groups = impl_->row_groups;
    metadata.total_rows = impl_->total_rows;

    uint64_t metadata_offset = static_cast<uint64_t>(impl_->file.tellp());
    impl_->writeMetadata(metadata);

    writeUInt32(impl_->file, FOOTER_MAGIC);
    writeUInt64(impl_->file, metadata_offset);

    impl_->file.close();
}

// FileReader implementation
struct FileReader::Impl {
    std::ifstream file;
    FileMetadata metadata;

    explicit Impl(const std::string& path) {
        file.open(path, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Failed to open file for reading: " + path);
        }

        file.seekg(-12, std::ios::end);
        uint32_t footer_magic = readUInt32(file);
        if (footer_magic != FOOTER_MAGIC) {
            throw std::runtime_error("Invalid footer magic");
        }

        uint64_t metadata_offset = readUInt64(file);

        file.seekg(0);
        uint32_t file_magic = readUInt32(file);
        if (file_magic != FILE_MAGIC) {
            throw std::runtime_error("Invalid file magic");
        }

        uint16_t major = readUInt16(file);
        uint16_t minor = readUInt16(file);

        if (major != FORMAT_VERSION_MAJOR) {
            throw std::runtime_error("Unsupported file version");
        }

        file.seekg(metadata_offset);
        readMetadata();
    }

    PageHeader readPageHeader() {
        PageHeader ph;
        ph.uncompressed_size = readUInt32(file);
        ph.compressed_size = readUInt32(file);
        ph.num_values = readUInt32(file);
        ph.encoding = static_cast<EncodingType>(readUInt8(file));

        uint8_t has_stats = readUInt8(file);
        if (has_stats) {
            uint8_t has_min = readUInt8(file);
            if (has_min) {
                ph.stats.min_int = readInt64(file);
            }

            uint8_t has_max = readUInt8(file);
            if (has_max) {
                ph.stats.max_int = readInt64(file);
            }

            ph.stats.null_count = readUInt32(file);
        } else {
            ph.stats.null_count = 0;
        }

        ph.stats.distinct_count_estimate = 0;
        return ph;
    }

    void readMetadata() {
        uint32_t num_columns = readUInt32(file);
        if (num_columns > 10000) {
            throw std::runtime_error("Invalid metadata: too many columns");
        }
        metadata.schema.columns.resize(num_columns);

        for (size_t i = 0; i < num_columns; i++) {
            uint32_t name_len = readUInt32(file);
            if (name_len > 1024) {
                throw std::runtime_error("Invalid metadata: column name too long");
            }
            std::string name(name_len, '\0');
            file.read(&name[0], name_len);

            metadata.schema.columns[i].name = std::move(name);
            metadata.schema.columns[i].type = static_cast<ColumnType>(readUInt8(file));
            metadata.schema.columns[i].encoding = static_cast<EncodingType>(readUInt8(file));
        }

        uint32_t num_row_groups = readUInt32(file);
        if (num_row_groups > 100000) {
            throw std::runtime_error("Invalid metadata: too many row groups");
        }
        metadata.row_groups.resize(num_row_groups);

        for (size_t i = 0; i < num_row_groups; i++) {
            metadata.row_groups[i].num_rows = readUInt32(file);
            uint32_t num_cc = readUInt32(file);
            if (num_cc != num_columns) {
                throw std::runtime_error("Invalid metadata: column count mismatch");
            }
            metadata.row_groups[i].column_chunks.resize(num_cc);

            for (size_t j = 0; j < num_cc; j++) {
                auto& cc = metadata.row_groups[i].column_chunks[j];
                cc.file_offset = readUInt64(file);
                cc.total_size = readUInt64(file);
                uint32_t num_pages = readUInt32(file);
                if (num_pages > 10000) {
                    throw std::runtime_error("Invalid metadata: too many pages");
                }
                cc.page_headers.resize(num_pages);

                for (size_t k = 0; k < num_pages; k++) {
                    cc.page_headers[k] = readPageHeader();
                }
            }
        }

        metadata.total_rows = readUInt32(file);
    }

    std::vector<uint8_t> readPageData(const ColumnChunkMeta& cc_meta, size_t page_idx) {
        if (page_idx >= cc_meta.page_headers.size()) {
            throw std::runtime_error("Invalid page index");
        }

        uint64_t page_offset = cc_meta.file_offset;
        for (size_t i = 0; i < page_idx; i++) {
            size_t header_size = 14;
            if (cc_meta.page_headers[i].stats.min_int.has_value() ||
                cc_meta.page_headers[i].stats.max_int.has_value()) {
                header_size += 22;
            }
            page_offset += header_size + cc_meta.page_headers[i].compressed_size;
        }

        size_t header_size = 14;
        if (cc_meta.page_headers[page_idx].stats.min_int.has_value() ||
            cc_meta.page_headers[page_idx].stats.max_int.has_value()) {
            header_size += 22;
        }
        page_offset += header_size;

        file.seekg(page_offset);
        std::vector<uint8_t> data(cc_meta.page_headers[page_idx].compressed_size);
        file.read(reinterpret_cast<char*>(data.data()), data.size());

        return data;
    }
};

FileReader::FileReader(const std::string& path)
    : impl_(std::make_unique<Impl>(path)) {}

FileReader::~FileReader() = default;

const Schema& FileReader::schema() const {
    return impl_->metadata.schema;
}

const FileMetadata& FileReader::metadata() const {
    return impl_->metadata;
}

std::vector<int32_t> FileReader::readInt32Column(size_t row_group_idx, size_t col_idx) {
    if (row_group_idx >= impl_->metadata.row_groups.size()) {
        throw std::runtime_error("Invalid row group index");
    }

    const auto& rg = impl_->metadata.row_groups[row_group_idx];
    if (col_idx >= rg.column_chunks.size()) {
        throw std::runtime_error("Invalid column index");
    }

    const auto& cc = rg.column_chunks[col_idx];
    auto data = impl_->readPageData(cc, 0);

    const auto& ph = cc.page_headers[0];

    switch (ph.encoding) {
    case EncodingType::PLAIN: {
        std::vector<int32_t> result(ph.num_values);
        std::memcpy(result.data(), data.data(), result.size() * sizeof(int32_t));
        return result;
    }
    case EncodingType::RLE:
        return RLEEncoder::decodeInt32(data.data(), data.size(), ph.num_values);
    case EncodingType::DELTA:
        return DeltaEncoder::decodeInt32(data.data(), data.size(), ph.num_values);
    default:
        throw std::runtime_error("Unsupported encoding");
    }
}

std::vector<int64_t> FileReader::readInt64Column(size_t row_group_idx, size_t col_idx) {
    if (row_group_idx >= impl_->metadata.row_groups.size()) {
        throw std::runtime_error("Invalid row group index");
    }

    const auto& rg = impl_->metadata.row_groups[row_group_idx];
    if (col_idx >= rg.column_chunks.size()) {
        throw std::runtime_error("Invalid column index");
    }

    const auto& cc = rg.column_chunks[col_idx];
    auto data = impl_->readPageData(cc, 0);

    const auto& ph = cc.page_headers[0];

    switch (ph.encoding) {
    case EncodingType::PLAIN: {
        std::vector<int64_t> result(ph.num_values);
        std::memcpy(result.data(), data.data(), result.size() * sizeof(int64_t));
        return result;
    }
    case EncodingType::RLE:
        return RLEEncoder::decodeInt64(data.data(), data.size(), ph.num_values);
    case EncodingType::DELTA:
        return DeltaEncoder::decodeInt64(data.data(), data.size(), ph.num_values);
    default:
        throw std::runtime_error("Unsupported encoding");
    }
}

std::vector<std::string> FileReader::readStringColumn(size_t row_group_idx, size_t col_idx) {
    if (row_group_idx >= impl_->metadata.row_groups.size()) {
        throw std::runtime_error("Invalid row group index");
    }

    const auto& rg = impl_->metadata.row_groups[row_group_idx];
    if (col_idx >= rg.column_chunks.size()) {
        throw std::runtime_error("Invalid column index");
    }

    const auto& cc = rg.column_chunks[col_idx];
    auto data = impl_->readPageData(cc, 0);

    const auto& ph = cc.page_headers[0];

    switch (ph.encoding) {
    case EncodingType::PLAIN: {
        std::vector<std::string> result;
        result.reserve(ph.num_values);

        const uint32_t* offsets = reinterpret_cast<const uint32_t*>(data.data());
        const char* string_data = reinterpret_cast<const char*>(data.data()) +
                                  (ph.num_values + 1) * sizeof(uint32_t);

        for (uint32_t i = 0; i < ph.num_values; i++) {
            uint32_t start = offsets[i];
            uint32_t end = offsets[i + 1];
            result.emplace_back(string_data + start, end - start);
        }

        return result;
    }
    case EncodingType::DICTIONARY:
        return DictionaryEncoder::decode(data.data(), data.size(), ph.num_values);
    default:
        throw std::runtime_error("Unsupported encoding");
    }
}

} // namespace columnar
