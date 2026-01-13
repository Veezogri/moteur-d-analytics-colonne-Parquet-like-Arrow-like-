// Columnar Analytics Engine
// Author: RIAL Fares
// Vectorized execution engine implementation

#include "execution.h"
#include <algorithm>
#include <unordered_map>
#include <stdexcept>

namespace columnar {

// Batch utilities
size_t Batch::columnIndex(const std::string& name) const {
    for (size_t i = 0; i < column_names.size(); i++) {
        if (column_names[i] == name) {
            return i;
        }
    }
    throw std::runtime_error("Column not found in batch: " + name);
}

// Predicate evaluation
bool Predicate::evaluate(int32_t col_value) const {
    int64_t val = static_cast<int64_t>(col_value);
    switch (op) {
    case CompareOp::EQ: return val == value;
    case CompareOp::NE: return val != value;
    case CompareOp::LT: return val < value;
    case CompareOp::LE: return val <= value;
    case CompareOp::GT: return val > value;
    case CompareOp::GE: return val >= value;
    }
    return false;
}

bool Predicate::evaluate(int64_t col_value) const {
    switch (op) {
    case CompareOp::EQ: return col_value == value;
    case CompareOp::NE: return col_value != value;
    case CompareOp::LT: return col_value < value;
    case CompareOp::LE: return col_value <= value;
    case CompareOp::GT: return col_value > value;
    case CompareOp::GE: return col_value >= value;
    }
    return false;
}

bool Predicate::canSkipPage(const PageStats& stats) const {
    if (!stats.min_int.has_value() || !stats.max_int.has_value()) {
        return false;
    }

    int64_t min_val = stats.min_int.value();
    int64_t max_val = stats.max_int.value();

    switch (op) {
    case CompareOp::EQ:
        return value < min_val || value > max_val;
    case CompareOp::NE:
        return false;
    case CompareOp::LT:
        return min_val >= value;
    case CompareOp::LE:
        return min_val > value;
    case CompareOp::GT:
        return max_val <= value;
    case CompareOp::GE:
        return max_val < value;
    }
    return false;
}

// Scanner implementation
Scanner::Scanner(std::shared_ptr<FileReader> reader,
                 std::vector<std::string> columns,
                 size_t batch_size)
    : reader_(std::move(reader))
    , selected_columns_(std::move(columns))
    , batch_size_(batch_size)
    , current_row_group_(0)
    , current_offset_(0) {

    for (const auto& col : selected_columns_) {
        column_indices_.push_back(reader_->schema().columnIndex(col));
    }
}

void Scanner::addFilter(Predicate pred) {
    filters_.push_back(pred);
}

bool Scanner::hasNext() {
    return current_row_group_ < reader_->metadata().row_groups.size();
}

Batch Scanner::next() {
    if (!hasNext()) {
        throw std::runtime_error("No more batches");
    }

    const auto& rg = reader_->metadata().row_groups[current_row_group_];

    bool can_skip = false;
    for (const auto& filter : filters_) {
        size_t filter_col_idx = reader_->schema().columnIndex(filter.column);
        const auto& cc = rg.column_chunks[filter_col_idx];

        if (!cc.page_headers.empty()) {
            if (filter.canSkipPage(cc.page_headers[0].stats)) {
                can_skip = true;
                break;
            }
        }
    }

    if (can_skip) {
        current_row_group_++;
        current_offset_ = 0;
        return next();
    }

    Batch batch;
    batch.column_names = selected_columns_;
    batch.num_rows = rg.num_rows;

    for (size_t i = 0; i < selected_columns_.size(); i++) {
        size_t col_idx = column_indices_[i];
        const auto& col_schema = reader_->schema().columns[col_idx];

        switch (col_schema.type) {
        case ColumnType::INT32: {
            auto values = reader_->readInt32Column(current_row_group_, col_idx);
            batch.columns.push_back(std::move(values));
            break;
        }
        case ColumnType::INT64: {
            auto values = reader_->readInt64Column(current_row_group_, col_idx);
            batch.columns.push_back(std::move(values));
            break;
        }
        case ColumnType::STRING: {
            auto values = reader_->readStringColumn(current_row_group_, col_idx);
            batch.columns.push_back(std::move(values));
            break;
        }
        }
    }

    if (!filters_.empty()) {
        std::vector<size_t> keep_indices;
        keep_indices.reserve(batch.num_rows);

        for (size_t row = 0; row < batch.num_rows; row++) {
            bool pass = true;

            for (const auto& filter : filters_) {
                size_t filter_col_batch_idx = batch.columnIndex(filter.column);
                const auto& col = batch.columns[filter_col_batch_idx];

                if (std::holds_alternative<std::vector<int32_t>>(col)) {
                    const auto& vals = std::get<std::vector<int32_t>>(col);
                    if (!filter.evaluate(vals[row])) {
                        pass = false;
                        break;
                    }
                } else if (std::holds_alternative<std::vector<int64_t>>(col)) {
                    const auto& vals = std::get<std::vector<int64_t>>(col);
                    if (!filter.evaluate(vals[row])) {
                        pass = false;
                        break;
                    }
                }
            }

            if (pass) {
                keep_indices.push_back(row);
            }
        }

        Batch filtered_batch;
        filtered_batch.column_names = batch.column_names;
        filtered_batch.num_rows = keep_indices.size();

        for (auto& col : batch.columns) {
            if (std::holds_alternative<std::vector<int32_t>>(col)) {
                const auto& vals = std::get<std::vector<int32_t>>(col);
                std::vector<int32_t> filtered_vals;
                filtered_vals.reserve(keep_indices.size());
                for (size_t idx : keep_indices) {
                    filtered_vals.push_back(vals[idx]);
                }
                filtered_batch.columns.push_back(std::move(filtered_vals));
            } else if (std::holds_alternative<std::vector<int64_t>>(col)) {
                const auto& vals = std::get<std::vector<int64_t>>(col);
                std::vector<int64_t> filtered_vals;
                filtered_vals.reserve(keep_indices.size());
                for (size_t idx : keep_indices) {
                    filtered_vals.push_back(vals[idx]);
                }
                filtered_batch.columns.push_back(std::move(filtered_vals));
            } else if (std::holds_alternative<std::vector<std::string>>(col)) {
                const auto& vals = std::get<std::vector<std::string>>(col);
                std::vector<std::string> filtered_vals;
                filtered_vals.reserve(keep_indices.size());
                for (size_t idx : keep_indices) {
                    filtered_vals.push_back(vals[idx]);
                }
                filtered_batch.columns.push_back(std::move(filtered_vals));
            }
        }

        batch = std::move(filtered_batch);
    }

    current_row_group_++;
    current_offset_ = 0;

    return batch;
}

// Query executor
QueryExecutor::QueryExecutor(std::shared_ptr<FileReader> reader)
    : reader_(std::move(reader)) {}

void QueryExecutor::setProjection(std::vector<std::string> columns) {
    projection_ = std::move(columns);
}

void QueryExecutor::addFilter(Predicate pred) {
    filters_.push_back(pred);
}

void QueryExecutor::setAggregation(AggFunc func, std::string column) {
    aggregation_ = std::make_pair(func, std::move(column));
}

void QueryExecutor::setGroupBy(std::string column) {
    group_by_column_ = std::move(column);
}

std::vector<Batch> QueryExecutor::executeQuery() {
    std::vector<std::string> scan_columns = projection_.empty() ?
        [this]() {
            std::vector<std::string> cols;
            for (const auto& c : reader_->schema().columns) {
                cols.push_back(c.name);
            }
            return cols;
        }() : projection_;

    Scanner scanner(reader_, scan_columns);

    for (const auto& filter : filters_) {
        scanner.addFilter(filter);
    }

    std::vector<Batch> results;
    while (scanner.hasNext()) {
        results.push_back(scanner.next());
    }

    return results;
}

AggResult QueryExecutor::executeAggregate() {
    if (!aggregation_.has_value()) {
        throw std::runtime_error("No aggregation specified");
    }

    const auto& [func, col_name] = aggregation_.value();

    std::vector<std::string> scan_columns;
    if (func != AggFunc::COUNT) {
        scan_columns.push_back(col_name);
    } else {
        if (!reader_->schema().columns.empty()) {
            scan_columns.push_back(reader_->schema().columns[0].name);
        }
    }

    Scanner scanner(reader_, scan_columns);
    for (const auto& filter : filters_) {
        scanner.addFilter(filter);
    }

    AggResult result{};
    result.count = 0;
    result.sum = 0;

    while (scanner.hasNext()) {
        Batch batch = scanner.next();
        result.count += batch.num_rows;

        if (func == AggFunc::COUNT) {
            continue;
        }

        size_t col_idx = batch.columnIndex(col_name);
        const auto& col = batch.columns[col_idx];

        if (std::holds_alternative<std::vector<int32_t>>(col)) {
            const auto& vals = std::get<std::vector<int32_t>>(col);
            for (int32_t val : vals) {
                result.sum += val;
                if (!result.min.has_value() || val < result.min.value()) {
                    result.min = val;
                }
                if (!result.max.has_value() || val > result.max.value()) {
                    result.max = val;
                }
            }
        } else if (std::holds_alternative<std::vector<int64_t>>(col)) {
            const auto& vals = std::get<std::vector<int64_t>>(col);
            for (int64_t val : vals) {
                result.sum += val;
                if (!result.min.has_value() || val < result.min.value()) {
                    result.min = val;
                }
                if (!result.max.has_value() || val > result.max.value()) {
                    result.max = val;
                }
            }
        }
    }

    return result;
}

std::vector<std::pair<std::string, AggResult>> QueryExecutor::executeGroupBy() {
    if (!group_by_column_.has_value()) {
        throw std::runtime_error("No GROUP BY column specified");
    }

    if (!aggregation_.has_value()) {
        throw std::runtime_error("No aggregation specified for GROUP BY");
    }

    const auto& group_col = group_by_column_.value();
    const auto& [func, agg_col] = aggregation_.value();

    std::vector<std::string> scan_columns{group_col};
    if (func != AggFunc::COUNT) {
        scan_columns.push_back(agg_col);
    }

    Scanner scanner(reader_, scan_columns);
    for (const auto& filter : filters_) {
        scanner.addFilter(filter);
    }

    std::unordered_map<std::string, AggResult> groups;

    while (scanner.hasNext()) {
        Batch batch = scanner.next();

        size_t group_col_idx = batch.columnIndex(group_col);
        const auto& group_vals = std::get<std::vector<std::string>>(batch.columns[group_col_idx]);

        for (size_t row = 0; row < batch.num_rows; row++) {
            const std::string& group_key = group_vals[row];
            auto& agg = groups[group_key];
            agg.count++;

            if (func != AggFunc::COUNT) {
                size_t agg_col_idx = batch.columnIndex(agg_col);
                const auto& agg_vals_col = batch.columns[agg_col_idx];

                int64_t val = 0;
                if (std::holds_alternative<std::vector<int32_t>>(agg_vals_col)) {
                    val = std::get<std::vector<int32_t>>(agg_vals_col)[row];
                } else if (std::holds_alternative<std::vector<int64_t>>(agg_vals_col)) {
                    val = std::get<std::vector<int64_t>>(agg_vals_col)[row];
                }

                agg.sum += val;
                if (!agg.min.has_value() || val < agg.min.value()) {
                    agg.min = val;
                }
                if (!agg.max.has_value() || val > agg.max.value()) {
                    agg.max = val;
                }
            }
        }
    }

    std::vector<std::pair<std::string, AggResult>> results(groups.begin(), groups.end());
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    return results;
}

} // namespace columnar
