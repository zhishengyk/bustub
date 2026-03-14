#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <cpp_random_distributions/zipfian_int_distribution.h>

#include "argparse/argparse.hpp"
#include "buffer/buffer_pool_manager.h"
#include "fmt/core.h"
#include "storage/disk/disk_manager_memory.h"

namespace {

using Clock = std::chrono::steady_clock;

struct BenchPageHeader {
  uint64_t seed_;
  uint64_t page_id_;
  char data_[0];
};

auto ModifyPage(char *data, size_t page_idx, uint64_t seed) -> void {
  auto *pg = reinterpret_cast<BenchPageHeader *>(data);
  pg->seed_ = seed;
  pg->page_id_ = page_idx;
  pg->data_[pg->seed_ % 4000] = static_cast<char>(pg->seed_ % 256);
}

auto CheckPageConsistentNoSeed(const char *data, size_t page_idx) -> void {
  const auto *pg = reinterpret_cast<const BenchPageHeader *>(data);
  if (pg->page_id_ != page_idx) {
    fmt::println(stderr, "page header not consistent: page_id_={} page_idx={}", pg->page_id_, page_idx);
    std::terminate();
  }
  auto left = static_cast<unsigned int>(static_cast<unsigned char>(pg->data_[pg->seed_ % 4000]));
  auto right = static_cast<unsigned int>(pg->seed_ % 256);
  if (left != right) {
    fmt::println(stderr, "page content not consistent: data_[{}]={} seed_ % 256={}", pg->seed_ % 4000, left, right);
    std::terminate();
  }
}

auto CheckPageConsistent(const char *data, size_t page_idx, uint64_t seed) -> void {
  const auto *pg = reinterpret_cast<const BenchPageHeader *>(data);
  if (pg->seed_ != seed) {
    fmt::println(stderr, "page seed not consistent: seed_={} seed={}", pg->seed_, seed);
    std::terminate();
  }
  CheckPageConsistentNoSeed(data, page_idx);
}

struct Percentiles {
  double avg_us_{0};
  uint64_t p50_us_{0};
  uint64_t p95_us_{0};
  uint64_t p99_us_{0};
  size_t count_{0};
};

auto ComputePercentiles(std::vector<uint64_t> values) -> Percentiles {
  Percentiles stats;
  stats.count_ = values.size();
  if (values.empty()) {
    return stats;
  }

  std::sort(values.begin(), values.end());
  uint64_t total = 0;
  for (auto v : values) {
    total += v;
  }
  stats.avg_us_ = static_cast<double>(total) / static_cast<double>(values.size());

  auto pick = [&](double ratio) -> uint64_t {
    size_t idx = static_cast<size_t>(ratio * static_cast<double>(values.size()));
    if (idx >= values.size()) {
      idx = values.size() - 1;
    }
    return values[idx];
  };

  stats.p50_us_ = pick(0.50);
  stats.p95_us_ = pick(0.95);
  stats.p99_us_ = pick(0.99);
  return stats;
}

}  // namespace

auto main(int argc, char **argv) -> int {
  using bustub::AccessType;
  using bustub::BufferPoolManager;
  using bustub::DiskManagerUnlimitedMemory;
  using bustub::page_id_t;

  argparse::ArgumentParser program("bpm-p99");
  program.add_argument("--duration");
  program.add_argument("--latency");
  program.add_argument("--scan-thread-n");
  program.add_argument("--get-thread-n");
  program.add_argument("--bpm-size");
  program.add_argument("--db-size");

  try {
    program.parse_args(argc, argv);
  } catch (const std::runtime_error &err) {
    std::cerr << err.what() << std::endl;
    std::cerr << program;
    return 1;
  }

  uint64_t duration_ms = program.present("--duration") ? std::stoull(program.get("--duration")) : 3000;
  uint64_t enable_latency = program.present("--latency") ? std::stoull(program.get("--latency")) : 1;
  uint64_t scan_thread_n = program.present("--scan-thread-n") ? std::stoull(program.get("--scan-thread-n")) : 8;
  uint64_t get_thread_n = program.present("--get-thread-n") ? std::stoull(program.get("--get-thread-n")) : 8;
  uint64_t bpm_size = program.present("--bpm-size") ? std::stoull(program.get("--bpm-size")) : 64;
  uint64_t db_size = program.present("--db-size") ? std::stoull(program.get("--db-size")) : 6400;

  auto disk_manager = std::make_unique<DiskManagerUnlimitedMemory>();
  auto bpm = std::make_unique<BufferPoolManager>(bpm_size, disk_manager.get());
  std::vector<page_id_t> page_ids;
  page_ids.reserve(db_size);

  for (size_t i = 0; i < db_size; i++) {
    page_id_t page_id = bpm->NewPage();
    {
      auto guard = bpm->WritePage(page_id);
      ModifyPage(guard.GetDataMut(), i, 0);
    }
    page_ids.push_back(page_id);
  }

  disk_manager->EnableLatencySimulator(enable_latency != 0);

  auto deadline = Clock::now() + std::chrono::milliseconds(duration_ms);
  std::vector<std::vector<uint64_t>> scan_latencies(scan_thread_n);
  std::vector<std::vector<uint64_t>> get_latencies(get_thread_n);
  std::vector<std::thread> threads;

  for (size_t thread_id = 0; thread_id < scan_thread_n; thread_id++) {
    threads.emplace_back([&, thread_id] {
      size_t page_idx_start = db_size * thread_id / scan_thread_n;
      size_t page_idx_end = db_size * (thread_id + 1) / scan_thread_n;
      size_t page_idx = page_idx_start;
      auto &samples = scan_latencies[thread_id];

      while (Clock::now() < deadline) {
        auto start = Clock::now();
        {
          auto page = bpm->ReadPage(page_ids[page_idx], AccessType::Scan);
          CheckPageConsistentNoSeed(page.GetData(), page_idx);
        }
        auto end = Clock::now();
        samples.push_back(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()));

        page_idx += 1;
        if (page_idx >= page_idx_end) {
          page_idx = page_idx_start;
        }
      }
    });
  }

  for (size_t thread_id = 0; thread_id < get_thread_n; thread_id++) {
    threads.emplace_back([&, thread_id] {
      std::random_device r;
      std::default_random_engine gen(r());
      zipfian_int_distribution<size_t> dist(0, db_size - 1, 0.8);
      std::unordered_map<page_id_t, uint64_t> records;
      auto &samples = get_latencies[thread_id];

      while (Clock::now() < deadline) {
        auto rand = dist(gen);
        auto page_idx = std::min(rand / get_thread_n * get_thread_n + thread_id, db_size - 1);

        auto start = Clock::now();
        {
          auto page = bpm->WritePage(page_ids[page_idx], AccessType::Lookup);
          auto &seed = records[page_idx];
          CheckPageConsistent(page.GetData(), page_idx, seed);
          seed = seed + 1;
          ModifyPage(page.GetDataMut(), page_idx, seed);
        }
        auto end = Clock::now();
        samples.push_back(
            static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count()));
      }
    });
  }

  for (auto &thread : threads) {
    thread.join();
  }

  std::vector<uint64_t> all_scan;
  std::vector<uint64_t> all_get;
  for (auto &v : scan_latencies) {
    all_scan.insert(all_scan.end(), v.begin(), v.end());
  }
  for (auto &v : get_latencies) {
    all_get.insert(all_get.end(), v.begin(), v.end());
  }

  auto scan_stats = ComputePercentiles(std::move(all_scan));
  auto get_stats = ComputePercentiles(std::move(all_get));

  fmt::print("config duration_ms={} latency={} bpm_size={} db_size={} scan_threads={} get_threads={}\n", duration_ms,
             enable_latency, bpm_size, db_size, scan_thread_n, get_thread_n);
  fmt::print("scan count={} avg_us={:.1f} p50_us={} p95_us={} p99_us={}\n", scan_stats.count_, scan_stats.avg_us_,
             scan_stats.p50_us_, scan_stats.p95_us_, scan_stats.p99_us_);
  fmt::print("get  count={} avg_us={:.1f} p50_us={} p95_us={} p99_us={}\n", get_stats.count_, get_stats.avg_us_,
             get_stats.p50_us_, get_stats.p95_us_, get_stats.p99_us_);

  return 0;
}
