//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"

#include <atomic>
#include <chrono>  // NOLINT(build/c++11)
#include <cstdlib>
#include <vector>

#include "common/macros.h"
#include "fmt/format.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

namespace {

using ProfileClock = std::chrono::steady_clock;

auto ProfileEnabled() -> bool {
  static const bool enabled = std::getenv("BUSTUB_PROFILE") != nullptr;
  return enabled;
}

auto ProfileNowNs() -> uint64_t {
  return static_cast<uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(ProfileClock::now().time_since_epoch()).count());
}

void UpdateMax(std::atomic<uint64_t> &target, uint64_t value) {
  auto current = target.load(std::memory_order_relaxed);
  while (current < value &&
         !target.compare_exchange_weak(current, value, std::memory_order_relaxed, std::memory_order_relaxed)) {
  }
}

struct DiskSchedulerProfile {
  std::atomic<uint64_t> schedule_calls_{0};
  std::atomic<uint64_t> enqueued_requests_{0};
  std::atomic<uint64_t> read_requests_{0};
  std::atomic<uint64_t> write_requests_{0};
  std::atomic<uint64_t> queue_wait_ns_total_{0};
  std::atomic<uint64_t> queue_wait_ns_max_{0};
  std::atomic<uint64_t> service_ns_total_{0};
  std::atomic<uint64_t> service_ns_max_{0};

  void Report() const {
    if (!ProfileEnabled()) {
      return;
    }

    const auto requests = enqueued_requests_.load(std::memory_order_relaxed);
    const auto queue_total = queue_wait_ns_total_.load(std::memory_order_relaxed);
    const auto service_total = service_ns_total_.load(std::memory_order_relaxed);

    fmt::println(stderr, "[profile][disk] schedule_calls={}", schedule_calls_.load(std::memory_order_relaxed));
    fmt::println(stderr, "[profile][disk] requests={} reads={} writes={}", requests,
                 read_requests_.load(std::memory_order_relaxed), write_requests_.load(std::memory_order_relaxed));
    if (requests == 0) {
      return;
    }

    fmt::println(stderr, "[profile][disk] avg_queue_wait_us={:.2f} max_queue_wait_us={:.2f}",
                 static_cast<double>(queue_total) / static_cast<double>(requests) / 1000.0,
                 static_cast<double>(queue_wait_ns_max_.load(std::memory_order_relaxed)) / 1000.0);
    fmt::println(stderr, "[profile][disk] avg_service_us={:.2f} max_service_us={:.2f}",
                 static_cast<double>(service_total) / static_cast<double>(requests) / 1000.0,
                 static_cast<double>(service_ns_max_.load(std::memory_order_relaxed)) / 1000.0);
  }
};

auto GetDiskSchedulerProfile() -> DiskSchedulerProfile & {
  static auto *profile = [] {
    auto *prof = new DiskSchedulerProfile();
    if (ProfileEnabled()) {
      std::atexit([] { GetDiskSchedulerProfile().Report(); });
    }
    return prof;
  }();
  return *profile;
}

}  // namespace

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // 启动后台工作线程
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // 向队列中放入一个 `std::nullopt`，通知后台线程退出循环
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): 补充实现
 *
 * @brief 调度一组请求，由 DiskManager 执行。
 *
 * @param requests 待调度的请求集合。
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
  const auto profile_enabled = ProfileEnabled();
  auto &profile = GetDiskSchedulerProfile();
  profile.schedule_calls_.fetch_add(1, std::memory_order_relaxed);

  for (auto &request : requests) {
    if (profile_enabled) {
      request.profile_enqueued_ns_ = ProfileNowNs();
      profile.enqueued_requests_.fetch_add(1, std::memory_order_relaxed);
      if (request.is_write_) {
        profile.write_requests_.fetch_add(1, std::memory_order_relaxed);
      } else {
        profile.read_requests_.fetch_add(1, std::memory_order_relaxed);
      }
    }
    request_queue_.Put(std::move(request));
  }
}

/**
 * TODO(P1): 补充实现
 *
 * @brief 后台工作线程函数，负责处理已经调度的请求。
 *
 * 后台线程在 DiskScheduler 生命周期内需要持续处理请求，也就是说，在 `~DiskScheduler()`
 * 被调用之前，这个函数都不应该返回；而当析构发生时，你又需要确保该函数能够正常退出。
 */
void DiskScheduler::StartWorkerThread() {
  const auto profile_enabled = ProfileEnabled();
  auto &profile = GetDiskSchedulerProfile();

  while (true) {
    auto request_opt = request_queue_.Get();

    if (!request_opt.has_value()) {
      return;
    }

    auto request = std::move(request_opt.value());
    uint64_t service_start_ns = 0;

    if (profile_enabled) {
      const auto now_ns = ProfileNowNs();
      if (request.profile_enqueued_ns_ != 0) {
        const auto queue_wait_ns = now_ns - request.profile_enqueued_ns_;
        profile.queue_wait_ns_total_.fetch_add(queue_wait_ns, std::memory_order_relaxed);
        UpdateMax(profile.queue_wait_ns_max_, queue_wait_ns);
      }
      service_start_ns = now_ns;
    }

    if (request.is_write_) {
      disk_manager_->WritePage(request.page_id_, request.data_);
    } else {
      disk_manager_->ReadPage(request.page_id_, request.data_);
    }

    if (profile_enabled) {
      const auto service_ns = ProfileNowNs() - service_start_ns;
      profile.service_ns_total_.fetch_add(service_ns, std::memory_order_relaxed);
      UpdateMax(profile.service_ns_max_, service_ns);
    }

    request.callback_.set_value(true);
  }
}

}  // namespace bustub
