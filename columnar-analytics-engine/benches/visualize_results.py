#!/usr/bin/env python3
# Columnar Analytics Engine
# Author: RIAL Fares
# Benchmark visualization script

import json
import matplotlib.pyplot as plt
import sys
from pathlib import Path

def load_results(json_path):
    with open(json_path, 'r') as f:
        return json.load(f)

def plot_throughput(data, output_dir):
    benchmarks = data['benchmarks']
    names = [b['name'] for b in benchmarks]
    throughput = [b['throughput_mbps'] for b in benchmarks]

    plt.figure(figsize=(12, 6))
    bars = plt.bar(names, throughput, color=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])
    plt.ylabel('Throughput (MB/s)', fontsize=12)
    plt.title('Columnar Analytics Engine - Throughput Comparison', fontsize=14, fontweight='bold')
    plt.xticks(rotation=15, ha='right')
    plt.grid(axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.savefig(output_dir / 'throughput.png', dpi=300)
    print(f"Saved: {output_dir / 'throughput.png'}")
    plt.close()

def plot_rows_per_sec(data, output_dir):
    benchmarks = data['benchmarks']
    names = [b['name'] for b in benchmarks]
    rows_per_sec = [b['rows_per_sec'] / 1e6 for b in benchmarks]  # Convert to millions

    plt.figure(figsize=(12, 6))
    bars = plt.bar(names, rows_per_sec, color=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])
    plt.ylabel('Rows/sec (Millions)', fontsize=12)
    plt.title('Columnar Analytics Engine - Processing Speed', fontsize=14, fontweight='bold')
    plt.xticks(rotation=15, ha='right')
    plt.grid(axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}M',
                ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.savefig(output_dir / 'rows_per_sec.png', dpi=300)
    print(f"Saved: {output_dir / 'rows_per_sec.png'}")
    plt.close()

def plot_latency(data, output_dir):
    benchmarks = data['benchmarks']
    names = [b['name'] for b in benchmarks]
    latency = [b['elapsed_ms'] for b in benchmarks]

    plt.figure(figsize=(12, 6))
    bars = plt.bar(names, latency, color=['#2E86AB', '#A23B72', '#F18F01', '#C73E1D'])
    plt.ylabel('Latency (ms)', fontsize=12)
    plt.title('Columnar Analytics Engine - Query Latency', fontsize=14, fontweight='bold')
    plt.xticks(rotation=15, ha='right')
    plt.grid(axis='y', alpha=0.3)

    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}ms',
                ha='center', va='bottom', fontsize=10)

    plt.tight_layout()
    plt.savefig(output_dir / 'latency.png', dpi=300)
    print(f"Saved: {output_dir / 'latency.png'}")
    plt.close()

def plot_combined_dashboard(data, output_dir):
    benchmarks = data['benchmarks']
    names = [b['name'].replace(' (value > 50000)', '\n(filtered)') for b in benchmarks]
    throughput = [b['throughput_mbps'] for b in benchmarks]
    rows_per_sec = [b['rows_per_sec'] / 1e6 for b in benchmarks]
    latency = [b['elapsed_ms'] for b in benchmarks]

    fig, axes = plt.subplots(1, 3, figsize=(18, 6))
    colors = ['#2E86AB', '#A23B72', '#F18F01', '#C73E1D']

    # Throughput
    axes[0].bar(range(len(names)), throughput, color=colors)
    axes[0].set_ylabel('Throughput (MB/s)', fontsize=11)
    axes[0].set_title('Throughput', fontsize=12, fontweight='bold')
    axes[0].set_xticks(range(len(names)))
    axes[0].set_xticklabels(names, rotation=15, ha='right', fontsize=9)
    axes[0].grid(axis='y', alpha=0.3)

    # Rows per second
    axes[1].bar(range(len(names)), rows_per_sec, color=colors)
    axes[1].set_ylabel('Rows/sec (Millions)', fontsize=11)
    axes[1].set_title('Processing Speed', fontsize=12, fontweight='bold')
    axes[1].set_xticks(range(len(names)))
    axes[1].set_xticklabels(names, rotation=15, ha='right', fontsize=9)
    axes[1].grid(axis='y', alpha=0.3)

    # Latency
    axes[2].bar(range(len(names)), latency, color=colors)
    axes[2].set_ylabel('Latency (ms)', fontsize=11)
    axes[2].set_title('Query Latency', fontsize=12, fontweight='bold')
    axes[2].set_xticks(range(len(names)))
    axes[2].set_xticklabels(names, rotation=15, ha='right', fontsize=9)
    axes[2].grid(axis='y', alpha=0.3)

    fig.suptitle('Columnar Analytics Engine - Benchmark Dashboard\nAuthor: RIAL Fares',
                 fontsize=14, fontweight='bold', y=1.00)
    plt.tight_layout()
    plt.savefig(output_dir / 'dashboard.png', dpi=300, bbox_inches='tight')
    print(f"Saved: {output_dir / 'dashboard.png'}")
    plt.close()

def main():
    if len(sys.argv) > 1:
        json_path = Path(sys.argv[1])
    else:
        json_path = Path(__file__).parent / 'benchmark_results.json'

    if not json_path.exists():
        print(f"Error: {json_path} not found")
        print("Run the benchmark first: ./benchmark.exe")
        sys.exit(1)

    output_dir = json_path.parent
    data = load_results(json_path)

    print("Generating visualizations...")
    plot_throughput(data, output_dir)
    plot_rows_per_sec(data, output_dir)
    plot_latency(data, output_dir)
    plot_combined_dashboard(data, output_dir)

    print("\nVisualization complete!")
    print(f"Results saved in: {output_dir}")

if __name__ == '__main__':
    main()
