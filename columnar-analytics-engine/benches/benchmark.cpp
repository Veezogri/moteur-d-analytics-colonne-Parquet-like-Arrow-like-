// Columnar Analytics Engine
// Author: RIAL Fares
// Reproducible benchmark harness

#include "format.h"
#include "execution.h"
#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <filesystem>
#include <algorithm>

using namespace columnar;

struct BenchmarkResult {
    std::string name;
    double elapsed_ms;
    size_t rows_processed;
    size_t bytes_processed;
    double throughput_mbps;
    double rows_per_sec;
};

class Timer {
public:
    void start() {
        start_ = std::chrono::high_resolution_clock::now();
    }

    double elapsed_ms() const {
        auto end = std::chrono::high_resolution_clock::now();
        return std::chrono::duration<double, std::milli>(end - start_).count();
    }

private:
    std::chrono::high_resolution_clock::time_point start_;
};

void generateBenchmarkDataset(const std::string& path, size_t num_rows, unsigned int seed) {
    std::cout << "Generating dataset: " << num_rows << " rows (seed=" << seed << ")\n";

    std::mt19937 rng(seed);
    std::uniform_int_distribution<int32_t> category_dist(1, 10);
    std::uniform_int_distribution<int64_t> value_dist(0, 100000);
    std::uniform_int_distribution<int> region_dist(0, 7);

    std::vector<std::string> regions = {
        "north", "south", "east", "west",
        "northeast", "northwest", "southeast", "southwest"
    };

    Schema schema;
    schema.columns = {
        {"id", ColumnType::INT64, EncodingType::PLAIN},
        {"value", ColumnType::INT64, EncodingType::DELTA},
        {"score", ColumnType::INT32, EncodingType::RLE},
        {"region", ColumnType::STRING, EncodingType::DICTIONARY}
    };

    FileWriter writer(path, schema);

    const size_t chunk_size = 50000;
    size_t remaining = num_rows;

    while (remaining > 0) {
        size_t current_chunk = std::min(remaining, chunk_size);

        std::vector<int64_t> ids(current_chunk);
        std::vector<int64_t> values(current_chunk);
        std::vector<int32_t> scores(current_chunk);
        std::vector<std::string> region_vals(current_chunk);

        for (size_t i = 0; i < current_chunk; i++) {
            ids[i] = static_cast<int64_t>(num_rows - remaining + i);
            values[i] = value_dist(rng);
            scores[i] = category_dist(rng);
            region_vals[i] = regions[region_dist(rng)];
        }

        writer.writeInt64Column(0, ids);
        writer.writeInt64Column(1, values);
        writer.writeInt32Column(2, scores);
        writer.writeStringColumn(3, region_vals);

        writer.flushRowGroup();

        remaining -= current_chunk;
    }

    writer.close();
    std::cout << "Dataset generated: " << path << "\n\n";
}

BenchmarkResult runFullScan(const std::string& path) {
    Timer timer;
    timer.start();

    auto reader = std::make_shared<FileReader>(path);
    QueryExecutor executor(reader);

    auto batches = executor.executeQuery();

    size_t total_rows = 0;
    for (const auto& batch : batches) {
        total_rows += batch.num_rows;
    }

    double elapsed = timer.elapsed_ms();

    size_t file_size = std::filesystem::file_size(path);

    BenchmarkResult result;
    result.name = "Full Scan";
    result.elapsed_ms = elapsed;
    result.rows_processed = total_rows;
    result.bytes_processed = file_size;
    result.throughput_mbps = (file_size / (1024.0 * 1024.0)) / (elapsed / 1000.0);
    result.rows_per_sec = total_rows / (elapsed / 1000.0);

    return result;
}

BenchmarkResult runFilteredScan(const std::string& path) {
    Timer timer;
    timer.start();

    auto reader = std::make_shared<FileReader>(path);
    QueryExecutor executor(reader);

    executor.addFilter(Predicate{"value", CompareOp::GT, 50000});
    auto batches = executor.executeQuery();

    size_t total_rows = 0;
    for (const auto& batch : batches) {
        total_rows += batch.num_rows;
    }

    double elapsed = timer.elapsed_ms();
    size_t file_size = std::filesystem::file_size(path);

    BenchmarkResult result;
    result.name = "Filtered Scan (value > 50000)";
    result.elapsed_ms = elapsed;
    result.rows_processed = total_rows;
    result.bytes_processed = file_size;
    result.throughput_mbps = (file_size / (1024.0 * 1024.0)) / (elapsed / 1000.0);
    result.rows_per_sec = total_rows / (elapsed / 1000.0);

    return result;
}

BenchmarkResult runAggregation(const std::string& path) {
    Timer timer;
    timer.start();

    auto reader = std::make_shared<FileReader>(path);
    QueryExecutor executor(reader);

    executor.setAggregation(AggFunc::SUM, "value");
    auto result = executor.executeAggregate();

    double elapsed = timer.elapsed_ms();
    size_t file_size = std::filesystem::file_size(path);

    BenchmarkResult bench_result;
    bench_result.name = "Aggregation (SUM)";
    bench_result.elapsed_ms = elapsed;
    bench_result.rows_processed = result.count;
    bench_result.bytes_processed = file_size;
    bench_result.throughput_mbps = (file_size / (1024.0 * 1024.0)) / (elapsed / 1000.0);
    bench_result.rows_per_sec = result.count / (elapsed / 1000.0);

    return bench_result;
}

BenchmarkResult runGroupBy(const std::string& path) {
    Timer timer;
    timer.start();

    auto reader = std::make_shared<FileReader>(path);
    QueryExecutor executor(reader);

    executor.setGroupBy("region");
    executor.setAggregation(AggFunc::SUM, "value");
    auto results = executor.executeGroupBy();

    size_t total_rows = 0;
    for (const auto& [key, agg] : results) {
        total_rows += agg.count;
    }

    double elapsed = timer.elapsed_ms();
    size_t file_size = std::filesystem::file_size(path);

    BenchmarkResult result;
    result.name = "Group By (region)";
    result.elapsed_ms = elapsed;
    result.rows_processed = total_rows;
    result.bytes_processed = file_size;
    result.throughput_mbps = (file_size / (1024.0 * 1024.0)) / (elapsed / 1000.0);
    result.rows_per_sec = total_rows / (elapsed / 1000.0);

    return result;
}

void printResults(const std::vector<BenchmarkResult>& results) {
    std::cout << "\n=== Benchmark Results ===\n\n";
    std::cout << std::left << std::setw(30) << "Benchmark"
              << std::right << std::setw(12) << "Time (ms)"
              << std::setw(15) << "Rows"
              << std::setw(15) << "Throughput"
              << std::setw(15) << "Rows/sec"
              << "\n";
    std::cout << std::string(87, '-') << "\n";

    for (const auto& result : results) {
        std::cout << std::left << std::setw(30) << result.name
                  << std::right << std::setw(12) << std::fixed << std::setprecision(2)
                  << result.elapsed_ms
                  << std::setw(15) << result.rows_processed
                  << std::setw(12) << std::fixed << std::setprecision(2)
                  << result.throughput_mbps << " MB/s"
                  << std::setw(12) << std::fixed << std::setprecision(0)
                  << result.rows_per_sec << " r/s"
                  << "\n";
    }
    std::cout << "\n";
}

void exportCSV(const std::vector<BenchmarkResult>& results, const std::string& path) {
    std::ofstream out(path);
    out << "benchmark,elapsed_ms,rows_processed,bytes_processed,throughput_mbps,rows_per_sec\n";

    for (const auto& result : results) {
        out << result.name << ","
            << result.elapsed_ms << ","
            << result.rows_processed << ","
            << result.bytes_processed << ","
            << result.throughput_mbps << ","
            << result.rows_per_sec << "\n";
    }

    out.close();
    std::cout << "Results exported to: " << path << "\n";
}

void exportJSON(const std::vector<BenchmarkResult>& results, const std::string& path) {
    std::ofstream out(path);
    out << "{\n";
    out << "  \"benchmarks\": [\n";

    for (size_t i = 0; i < results.size(); i++) {
        const auto& result = results[i];
        out << "    {\n";
        out << "      \"name\": \"" << result.name << "\",\n";
        out << "      \"elapsed_ms\": " << result.elapsed_ms << ",\n";
        out << "      \"rows_processed\": " << result.rows_processed << ",\n";
        out << "      \"bytes_processed\": " << result.bytes_processed << ",\n";
        out << "      \"throughput_mbps\": " << result.throughput_mbps << ",\n";
        out << "      \"rows_per_sec\": " << result.rows_per_sec << "\n";
        out << "    }";
        if (i < results.size() - 1) {
            out << ",";
        }
        out << "\n";
    }

    out << "  ]\n";
    out << "}\n";

    out.close();
    std::cout << "Results exported to: " << path << "\n";
}

int main(int argc, char* argv[]) {
    size_t num_rows = 1000000;
    unsigned int seed = 42;
    std::string dataset_path = "benchmark_data.col";

    if (argc >= 2) {
        num_rows = std::stoull(argv[1]);
    }
    if (argc >= 3) {
        seed = std::stoul(argv[2]);
    }

    std::cout << "Columnar Analytics Engine - Benchmark Suite\n";
    std::cout << "Author: RIAL Fares\n\n";

    generateBenchmarkDataset(dataset_path, num_rows, seed);

    std::cout << "Running benchmarks...\n\n";

    std::vector<BenchmarkResult> results;

    std::cout << "[1/4] Running full scan...\n";
    results.push_back(runFullScan(dataset_path));

    std::cout << "[2/4] Running filtered scan...\n";
    results.push_back(runFilteredScan(dataset_path));

    std::cout << "[3/4] Running aggregation...\n";
    results.push_back(runAggregation(dataset_path));

    std::cout << "[4/4] Running group by...\n";
    results.push_back(runGroupBy(dataset_path));

    printResults(results);

    exportCSV(results, "benchmark_results.csv");
    exportJSON(results, "benchmark_results.json");

    std::cout << "\nBenchmark complete.\n";

    return 0;
}
