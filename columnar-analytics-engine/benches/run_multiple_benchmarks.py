#!/usr/bin/env python3
# Columnar Analytics Engine
# Author: RIAL Fares
# Run benchmarks with multiple dataset sizes

import subprocess
import json
import matplotlib.pyplot as plt
from pathlib import Path
import sys

def run_benchmark_for_size(benchmark_exe, num_rows):
    """Run benchmark and return results"""
    print(f"\nRunning benchmark with {num_rows:,} rows...")

    # The benchmark executable generates its own data internally
    # We just run it and parse the JSON output
    result = subprocess.run([str(benchmark_exe)],
                          capture_output=True,
                          text=True,
                          cwd=benchmark_exe.parent)

    if result.returncode != 0:
        print(f"Error running benchmark: {result.stderr}")
        return None

    # Read the generated JSON
    json_path = benchmark_exe.parent / 'benchmark_results.json'
    if not json_path.exists():
        print(f"Error: {json_path} not found")
        return None

    with open(json_path, 'r') as f:
        data = json.load(f)

    return data

def plot_scalability(all_results, output_dir):
    """Plot how performance scales with dataset size"""
    sizes = sorted(all_results.keys())

    # Extract data for each benchmark type
    benchmark_types = all_results[sizes[0]]['benchmarks']

    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Columnar Analytics Engine - Scalability Analysis\nAuthor: RIAL Fares',
                 fontsize=16, fontweight='bold')

    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']

    for idx, benchmark in enumerate(benchmark_types):
        bench_name = benchmark['name']

        # Extract metrics across all sizes
        throughputs = []
        latencies = []
        rows_per_sec = []

        for size in sizes:
            bench_data = all_results[size]['benchmarks'][idx]
            throughputs.append(bench_data['throughput_mbps'])
            latencies.append(bench_data['elapsed_ms'])
            rows_per_sec.append(bench_data['rows_per_sec'] / 1e6)

        # Plot throughput
        ax = axes[0, 0]
        ax.plot(sizes, throughputs, marker='o', label=bench_name,
                color=colors[idx], linewidth=2, markersize=8)
        ax.set_xlabel('Dataset Size (rows)', fontsize=11)
        ax.set_ylabel('Throughput (MB/s)', fontsize=11)
        ax.set_title('Throughput vs Dataset Size', fontsize=12, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Plot latency
        ax = axes[0, 1]
        ax.plot(sizes, latencies, marker='o', label=bench_name,
                color=colors[idx], linewidth=2, markersize=8)
        ax.set_xlabel('Dataset Size (rows)', fontsize=11)
        ax.set_ylabel('Latency (ms)', fontsize=11)
        ax.set_title('Latency vs Dataset Size', fontsize=12, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)

        # Plot rows per second
        ax = axes[1, 0]
        ax.plot(sizes, rows_per_sec, marker='o', label=bench_name,
                color=colors[idx], linewidth=2, markersize=8)
        ax.set_xlabel('Dataset Size (rows)', fontsize=11)
        ax.set_ylabel('Processing Speed (M rows/s)', fontsize=11)
        ax.set_title('Processing Speed vs Dataset Size', fontsize=12, fontweight='bold')
        ax.legend()
        ax.grid(True, alpha=0.3)

    # Summary table in the last subplot
    ax = axes[1, 1]
    ax.axis('off')

    table_data = []
    table_data.append(['Dataset Size', 'Full Scan\n(MB/s)', 'Filtered\n(MB/s)',
                      'Aggregation\n(MB/s)', 'Group By\n(MB/s)'])

    for size in sizes:
        row = [f'{size:,}']
        for bench in all_results[size]['benchmarks']:
            row.append(f"{bench['throughput_mbps']:.1f}")
        table_data.append(row)

    table = ax.table(cellText=table_data, cellLoc='center', loc='center',
                    colWidths=[0.2, 0.2, 0.2, 0.2, 0.2])
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 2)

    # Style header row
    for i in range(5):
        table[(0, i)].set_facecolor('#2E86AB')
        table[(0, i)].set_text_props(weight='bold', color='white')

    # Alternate row colors
    for i in range(1, len(table_data)):
        color = '#f0f0f0' if i % 2 == 0 else 'white'
        for j in range(5):
            table[(i, j)].set_facecolor(color)

    plt.tight_layout()
    plt.savefig(output_dir / 'scalability_analysis.png', dpi=300, bbox_inches='tight')
    print(f"\nSaved: {output_dir / 'scalability_analysis.png'}")
    plt.close()

def main():
    # Find benchmark executable
    script_dir = Path(__file__).parent

    # Check in build directory
    benchmark_exe = script_dir.parent / 'build' / 'benches' / 'benchmark.exe'
    if not benchmark_exe.exists():
        benchmark_exe = script_dir / 'benchmark.exe'

    if not benchmark_exe.exists():
        print("Error: benchmark.exe not found")
        print("Build the project first: cmake --build build")
        sys.exit(1)

    print("Columnar Analytics Engine - Multiple Benchmark Runner")
    print("Author: RIAL Fares")
    print("=" * 60)

    # Note: The current benchmark.cpp uses a hardcoded dataset size of 1M rows
    # For now, we'll just run it once and visualize
    # To support multiple sizes, the benchmark.cpp would need to accept a parameter

    print("\nNote: Current benchmark uses 1M rows (hardcoded)")
    print("Running benchmark...")

    result = run_benchmark_for_size(benchmark_exe, 1000000)

    if result:
        print("\nGenerating visualizations...")

        # For now, store single result
        all_results = {1000000: result}

        output_dir = benchmark_exe.parent

        # Generate individual plots
        from visualize_results import (plot_throughput, plot_rows_per_sec,
                                      plot_latency, plot_combined_dashboard)

        plot_throughput(result, output_dir)
        plot_rows_per_sec(result, output_dir)
        plot_latency(result, output_dir)
        plot_combined_dashboard(result, output_dir)

        print("\nVisualization complete!")
        print(f"Results saved in: {output_dir}")

if __name__ == '__main__':
    main()
