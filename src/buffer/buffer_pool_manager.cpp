//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：buffer_pool_manager.cpp
//
// 标识：src/buffer/buffer_pool_manager.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include <atomic>
#include <chrono>  // NOLINT(build/c++11)
#include <cstdlib>

#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

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

struct BpmProfile {
  std::atomic<uint64_t> read_hits_{0};
  std::atomic<uint64_t> read_misses_{0};
  std::atomic<uint64_t> write_hits_{0};
  std::atomic<uint64_t> write_misses_{0};
  std::atomic<uint64_t> no_frame_failures_{0};
  std::atomic<uint64_t> evictions_{0};
  std::atomic<uint64_t> dirty_writebacks_{0};
  std::atomic<uint64_t> read_ios_{0};

  std::atomic<uint64_t> read_hit_ns_total_{0};
  std::atomic<uint64_t> read_miss_ns_total_{0};
  std::atomic<uint64_t> write_hit_ns_total_{0};
  std::atomic<uint64_t> write_miss_ns_total_{0};
  std::atomic<uint64_t> bpm_latch_hold_ns_total_{0};
  std::atomic<uint64_t> bpm_latch_hold_ns_max_{0};
  std::atomic<uint64_t> evict_ns_total_{0};
  std::atomic<uint64_t> evict_ns_max_{0};
  std::atomic<uint64_t> writeback_wait_ns_total_{0};
  std::atomic<uint64_t> writeback_wait_ns_max_{0};
  std::atomic<uint64_t> readin_wait_ns_total_{0};
  std::atomic<uint64_t> readin_wait_ns_max_{0};
  std::atomic<uint64_t> read_miss_ns_max_{0};
  std::atomic<uint64_t> write_miss_ns_max_{0};

  static auto AvgUs(uint64_t total_ns, uint64_t count) -> double {
    if (count == 0) {
      return 0.0;
    }
    return static_cast<double>(total_ns) / static_cast<double>(count) / 1000.0;
  }

  void Report() const {
    if (!ProfileEnabled()) {
      return;
    }

    const auto read_hits = read_hits_.load(std::memory_order_relaxed);
    const auto read_misses = read_misses_.load(std::memory_order_relaxed);
    const auto write_hits = write_hits_.load(std::memory_order_relaxed);
    const auto write_misses = write_misses_.load(std::memory_order_relaxed);
    const auto total_ops = read_hits + read_misses + write_hits + write_misses;

    fmt::println(stderr, "[profile][bpm] read_hits={} read_misses={} write_hits={} write_misses={}", read_hits,
                 read_misses, write_hits, write_misses);
    fmt::println(stderr, "[profile][bpm] evictions={} dirty_writebacks={} read_ios={} no_frame_failures={}",
                 evictions_.load(std::memory_order_relaxed), dirty_writebacks_.load(std::memory_order_relaxed),
                 read_ios_.load(std::memory_order_relaxed), no_frame_failures_.load(std::memory_order_relaxed));
    fmt::println(stderr, "[profile][bpm] avg_read_hit_us={:.2f} avg_read_miss_us={:.2f}",  //
                 AvgUs(read_hit_ns_total_.load(std::memory_order_relaxed), read_hits),
                 AvgUs(read_miss_ns_total_.load(std::memory_order_relaxed), read_misses));
    fmt::println(stderr, "[profile][bpm] avg_write_hit_us={:.2f} avg_write_miss_us={:.2f}",  //
                 AvgUs(write_hit_ns_total_.load(std::memory_order_relaxed), write_hits),
                 AvgUs(write_miss_ns_total_.load(std::memory_order_relaxed), write_misses));
    fmt::println(stderr, "[profile][bpm] avg_bpm_latch_hold_us={:.2f} max_bpm_latch_hold_us={:.2f}",
                 AvgUs(bpm_latch_hold_ns_total_.load(std::memory_order_relaxed), total_ops),
                 static_cast<double>(bpm_latch_hold_ns_max_.load(std::memory_order_relaxed)) / 1000.0);

    const auto evictions = evictions_.load(std::memory_order_relaxed);
    const auto dirty_writebacks = dirty_writebacks_.load(std::memory_order_relaxed);
    const auto read_ios = read_ios_.load(std::memory_order_relaxed);
    fmt::println(stderr, "[profile][bpm] avg_evict_us={:.2f} max_evict_us={:.2f}",
                 AvgUs(evict_ns_total_.load(std::memory_order_relaxed), evictions),
                 static_cast<double>(evict_ns_max_.load(std::memory_order_relaxed)) / 1000.0);
    fmt::println(stderr, "[profile][bpm] avg_writeback_wait_us={:.2f} max_writeback_wait_us={:.2f}",
                 AvgUs(writeback_wait_ns_total_.load(std::memory_order_relaxed), dirty_writebacks),
                 static_cast<double>(writeback_wait_ns_max_.load(std::memory_order_relaxed)) / 1000.0);
    fmt::println(stderr, "[profile][bpm] avg_readin_wait_us={:.2f} max_readin_wait_us={:.2f}",
                 AvgUs(readin_wait_ns_total_.load(std::memory_order_relaxed), read_ios),
                 static_cast<double>(readin_wait_ns_max_.load(std::memory_order_relaxed)) / 1000.0);
    fmt::println(stderr, "[profile][bpm] max_read_miss_us={:.2f} max_write_miss_us={:.2f}",
                 static_cast<double>(read_miss_ns_max_.load(std::memory_order_relaxed)) / 1000.0,
                 static_cast<double>(write_miss_ns_max_.load(std::memory_order_relaxed)) / 1000.0);
  }
};

auto GetBpmProfile() -> BpmProfile & {
  static auto *profile = [] {
    auto *prof = new BpmProfile();
    if (ProfileEnabled()) {
      std::atexit([] { GetBpmProfile().Report(); });
    }
    return prof;
  }();
  return *profile;
}

}  // namespace

/**
 * @brief `FrameHeader` 的构造函数，会将所有字段初始化为默认值。
 *
 * 更多信息请参阅 "buffer/buffer_pool_manager.h" 中 `FrameHeader` 的文档。
 *
 * @param frame_id 我们要为其创建头部的帧 ID / 索引。
 */
FrameHeader::FrameHeader(frame_id_t frame_id) : frame_id_(frame_id), data_(BUSTUB_PAGE_SIZE, 0) { Reset(); }

/**
 * @brief 获取指向该帧数据的原始 const 指针。
 *
 * @return const char* 指向该帧存储的不可变数据的指针。
 */
auto FrameHeader::GetData() const -> const char * { return data_.data(); }

/**
 * @brief 获取指向该帧数据的原始可变指针。
 *
 * @return char* 指向该帧存储的可变数据的指针。
 */
auto FrameHeader::GetDataMut() -> char * { return data_.data(); }

/**
 * @brief 重置 `FrameHeader` 的成员字段。
 */
void FrameHeader::Reset() {
  std::fill(data_.begin(), data_.end(), 0);
  pin_count_.store(0);
  is_dirty_ = false;
  page_id_.reset();
}

/**
 * @brief 创建一个新的 `BufferPoolManager` 实例并初始化所有字段。
 *
 * 更多信息请参阅 "buffer/buffer_pool_manager.h" 中 `BufferPoolManager` 的文档。
 *
 * ### 实现说明
 *
 * 我们已经为你实现了一个与参考解兼容的构造函数版本。
 * 如果它与你的实现不匹配，你可以自由修改这里的任何内容。
 *
 * 但请注意：如果偏离指导过多，我们将更难帮助你。
 * 我们建议先使用提供的步骤实现一个可工作的 buffer pool manager。
 *
 * 当你得到完整可用的解法（通过所有 Gradescope 测试）后，再尝试更有趣的改进。
 *
 * @param num_frames buffer pool 的大小。
 * @param disk_manager 磁盘管理器。
 * @param log_manager 日志管理器。对于 P1 请忽略该参数。
 */
BufferPoolManager::BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager)
    : num_frames_(num_frames),
      next_page_id_(0),
      bpm_latch_(std::make_shared<std::mutex>()),
      replacer_(std::make_shared<ArcReplacer>(num_frames)),
      disk_scheduler_(std::make_shared<DiskScheduler>(disk_manager)),
      log_manager_(log_manager) {
  // 严格来说这不是必须的……
  std::scoped_lock latch(*bpm_latch_);

  // 将单调递增计数器初始化为 0。
  next_page_id_.store(0);

  // 预先分配所有内存帧。
  frames_.reserve(num_frames_);

  // 页表应恰好包含 `num_frames_` 个槽位，对应恰好 `num_frames_` 个帧。
  page_table_.reserve(num_frames_);

  // 初始化所有帧头，并将所有可能的帧 ID 填入空闲帧列表（因为初始时所有帧都空闲）。
  for (size_t i = 0; i < num_frames_; i++) {
    frames_.push_back(std::make_shared<FrameHeader>(i));
    free_frames_.push_back(static_cast<int>(i));
  }
}

/**
 * @brief 销毁 `BufferPoolManager`，释放缓冲池占用的所有内存。
 */
BufferPoolManager::~BufferPoolManager() = default;

/**
 * @brief 返回该缓冲池管理的帧数量。
 */
auto BufferPoolManager::Size() const -> size_t { return num_frames_; }

/**
 * @brief 在磁盘上分配一个新页面。
 *
 * ### 实现说明
 *
 * 你需要维护一个线程安全、单调递增的计数器，形式为 `std::atomic<page_id_t>`。
 * 更多信息可参阅 [atomics](https://en.cppreference.com/w/cpp/atomic/atomic) 文档。
 *
 * TODO(P1): 补充实现。
 *
 * @return 新分配页面的 page ID。
 */
auto BufferPoolManager::NewPage() -> page_id_t { return next_page_id_.fetch_add(1); }

/**
 * @brief 从数据库中删除一个页面（包括磁盘和内存中的副本）。
 *
 * 如果该页面在缓冲池中被 pin，本函数不执行任何操作并返回 `false`。
 * 否则，本函数会从磁盘和内存（若仍在缓冲池中）中删除该页面，并返回 `true`。
 *
 * ### 实现说明
 *
 * 请考虑页面或页面元数据可能存在的所有位置，并据此实现该函数。
 * 通常你会希望在实现 `CheckedReadPage` 和 `CheckedWritePage` 之后再实现该函数。
 *
 * 你应在 disk scheduler 中调用 `DeallocatePage` 以释放空间供新页面使用。
 *
 * TODO(P1): 补充实现。
 *
 * @param page_id 要删除页面的 page ID。
 * @return 若页面存在但无法删除则返回 `false`；若页面不存在或删除成功则返回 `true`。
 */
auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  bpm_cv_.wait(lock, [&] { return pending_pages_.count(page_id) == 0; });

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    disk_scheduler_->DeallocatePage(page_id);
    return true;
  }

  const auto frame_id = it->second;
  auto frame = frames_[frame_id];
  if (frame->pin_count_.load() > 0) {
    return false;
  }

  page_table_.erase(it);
  replacer_->Remove(frame_id);
  frame->Reset();
  free_frames_.push_back(frame_id);
  disk_scheduler_->DeallocatePage(page_id);
  return true;
}

/**
 * @brief 获取一个页面数据的可选写锁保护。用户可按需指定 `AccessType`。
 *
 * 如果无法将该页面加载到内存中，本函数将返回 `std::nullopt`。
 *
 * 页面数据只能通过 page guard 访问。`BufferPoolManager` 的使用者应根据访问模式获取
 * `ReadPageGuard` 或 `WritePageGuard`，以确保数据访问是线程安全的。
 *
 * 同一时刻一个页面最多只能有 1 个 `WritePageGuard` 进行读写。
 * 这使得访问既可只读也可可变，即拥有 `WritePageGuard` 的线程可以任意修改页面数据。
 * 如果用户希望多个线程同时读同一页面，应改用 `CheckedReadPage` 获取 `ReadPageGuard`。
 *
 * ### 实现说明
 *
 * 你需要处理三个主要场景。前两个相对简单：
 * 一个是可用内存充足，另一个是不需要额外 I/O。请思考这两种情况的具体含义。
 *
 * 第三个场景最棘手：当我们没有任何“容易获得”的可用内存时，
 * buffer pool 需要找到可用内存来装入页面，并使用你之前实现的替换算法
 * 选择可驱逐候选帧。
 *
 * 一旦 buffer pool 选定了待驱逐帧，为了把目标页面装入该帧，
 * 可能需要执行多次 I/O 操作。
 *
 * `CheckedReadPage` 和这里很可能有大量共享代码，你可以考虑抽取辅助函数。
 *
 * 这两个函数是本项目的核心，我们不会再提供更多提示。祝你好运！
 *
 * TODO(P1): 补充实现。
 *
 * @param page_id 要写入的页面 ID。
 * @param access_type 页面访问类型。
 * @return std::optional<WritePageGuard> 可选的闩锁保护：如果没有空闲帧（内存不足）
 * 则返回 `std::nullopt`；否则返回 `WritePageGuard`，保证对页面数据的独占可变访问。
 */
auto BufferPoolManager::CheckedWritePage(page_id_t page_id, AccessType access_type) -> std::optional<WritePageGuard> {
  const auto profile_enabled = ProfileEnabled();
  const auto op_start_ns = profile_enabled ? ProfileNowNs() : 0;
  std::shared_ptr<FrameHeader> frame;
  std::optional<page_id_t> dirty_writeback_page_id;
  frame_id_t frame_id = -1;
  uint64_t bpm_latch_hold_ns = 0;
  uint64_t evict_ns = 0;
  uint64_t writeback_wait_ns = 0;
  uint64_t readin_wait_ns = 0;
  bool cache_hit = false;
  bool failed_no_frame = false;
  bool did_evict = false;
  bool did_dirty_writeback = false;
  bool did_read_io = false;

  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);
    while (true) {
      const auto lock_acquired_ns = profile_enabled ? ProfileNowNs() : 0;
      auto it = page_table_.find(page_id);
      if (it != page_table_.end()) {
        cache_hit = true;
        frame_id = it->second;
        frame = frames_[frame_id];
        frame->pin_count_.fetch_add(1);
        replacer_->SetEvictable(frame_id, false);
        replacer_->RecordAccess(frame_id, page_id, access_type);
        if (profile_enabled) {
          bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
        }
        break;
      }

      if (pending_pages_.count(page_id) != 0) {
        if (profile_enabled) {
          bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
        }
        bpm_cv_.wait(lock, [&] { return pending_pages_.count(page_id) == 0; });
        continue;
      }

      if (!free_frames_.empty()) {
        frame_id = free_frames_.front();
        free_frames_.pop_front();
        frame = frames_[frame_id];
      } else {
        const auto evict_start_ns = profile_enabled ? ProfileNowNs() : 0;
        auto victim_opt = replacer_->Evict();
        if (profile_enabled) {
          evict_ns += ProfileNowNs() - evict_start_ns;
        }
        if (!victim_opt.has_value()) {
          failed_no_frame = true;
          if (profile_enabled) {
            bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
          }
          break;
        }

        did_evict = true;
        frame_id = victim_opt.value();
        frame = frames_[frame_id];

        if (frame->page_id_.has_value()) {
          const auto old_page_id = frame->page_id_.value();
          page_table_.erase(old_page_id);

          if (frame->is_dirty_) {
            did_dirty_writeback = true;
            dirty_writeback_page_id = old_page_id;
            pending_pages_.insert(old_page_id);
          }
        }
      }

      pending_pages_.insert(page_id);
      if (profile_enabled) {
        bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
      }
      break;
    }
  }

  if (!cache_hit && !failed_no_frame) {
    if (dirty_writeback_page_id.has_value()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();

      DiskRequest write_req{true, frame->GetDataMut(), dirty_writeback_page_id.value(), std::move(promise)};
      std::vector<DiskRequest> requests;
      requests.push_back(std::move(write_req));

      const auto io_wait_start_ns = profile_enabled ? ProfileNowNs() : 0;
      disk_scheduler_->Schedule(requests);
      future.get();
      if (profile_enabled) {
        writeback_wait_ns += ProfileNowNs() - io_wait_start_ns;
      }
    }

    frame->Reset();

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    DiskRequest read_req{false, frame->GetDataMut(), page_id, std::move(promise)};
    std::vector<DiskRequest> requests;
    requests.push_back(std::move(read_req));

    did_read_io = true;
    const auto io_wait_start_ns = profile_enabled ? ProfileNowNs() : 0;
    disk_scheduler_->Schedule(requests);
    future.get();
    if (profile_enabled) {
      readin_wait_ns += ProfileNowNs() - io_wait_start_ns;
    }

    {
      std::unique_lock<std::mutex> lock(*bpm_latch_);
      const auto lock_acquired_ns = profile_enabled ? ProfileNowNs() : 0;
      if (dirty_writeback_page_id.has_value()) {
        pending_pages_.erase(dirty_writeback_page_id.value());
      }
      frame->page_id_ = page_id;
      frame->pin_count_.store(1);
      frame->is_dirty_ = false;

      page_table_[page_id] = frame_id;
      replacer_->RecordAccess(frame_id, page_id, access_type);
      replacer_->SetEvictable(frame_id, false);
      pending_pages_.erase(page_id);
      if (profile_enabled) {
        bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
      }
    }
    bpm_cv_.notify_all();
  }

  if (profile_enabled) {
    auto &profile = GetBpmProfile();
    const auto total_ns = ProfileNowNs() - op_start_ns;
    if (cache_hit) {
      profile.write_hits_.fetch_add(1, std::memory_order_relaxed);
      profile.write_hit_ns_total_.fetch_add(total_ns, std::memory_order_relaxed);
    } else {
      profile.write_misses_.fetch_add(1, std::memory_order_relaxed);
      profile.write_miss_ns_total_.fetch_add(total_ns, std::memory_order_relaxed);
      UpdateMax(profile.write_miss_ns_max_, total_ns);
    }
    if (failed_no_frame) {
      profile.no_frame_failures_.fetch_add(1, std::memory_order_relaxed);
    }
    if (did_evict) {
      profile.evictions_.fetch_add(1, std::memory_order_relaxed);
      profile.evict_ns_total_.fetch_add(evict_ns, std::memory_order_relaxed);
      UpdateMax(profile.evict_ns_max_, evict_ns);
    }
    if (did_dirty_writeback) {
      profile.dirty_writebacks_.fetch_add(1, std::memory_order_relaxed);
      profile.writeback_wait_ns_total_.fetch_add(writeback_wait_ns, std::memory_order_relaxed);
      UpdateMax(profile.writeback_wait_ns_max_, writeback_wait_ns);
    }
    if (did_read_io) {
      profile.read_ios_.fetch_add(1, std::memory_order_relaxed);
      profile.readin_wait_ns_total_.fetch_add(readin_wait_ns, std::memory_order_relaxed);
      UpdateMax(profile.readin_wait_ns_max_, readin_wait_ns);
    }
    profile.bpm_latch_hold_ns_total_.fetch_add(bpm_latch_hold_ns, std::memory_order_relaxed);
    UpdateMax(profile.bpm_latch_hold_ns_max_, bpm_latch_hold_ns);
  }

  if (failed_no_frame) {
    return std::nullopt;
  }

  return WritePageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief 获取一个页面数据的可选读锁保护。用户可按需指定 `AccessType`。
 *
 * 如果无法将该页面加载到内存中，本函数将返回 `std::nullopt`。
 *
 * 页面数据只能通过 page guard 访问。`BufferPoolManager` 的使用者应根据访问模式获取
 * `ReadPageGuard` 或 `WritePageGuard`，以确保数据访问是线程安全的。
 *
 * 不同线程可同时持有任意数量的 `ReadPageGuard` 读取同一页面。
 * 但所有访问都必须是只读的。如果用户需要修改页面数据，
 * 应改用 `CheckedWritePage` 获取 `WritePageGuard`。
 *
 * ### 实现说明
 *
 * 参见 `CheckedWritePage` 的实现细节。
 *
 * TODO(P1): 补充实现。
 *
 * @param page_id 要读取的页面 ID。
 * @param access_type 页面访问类型。
 * @return std::optional<ReadPageGuard> 可选的闩锁保护：如果没有空闲帧（内存不足）
 * 则返回 `std::nullopt`；否则返回 `ReadPageGuard`，保证对页面数据的共享只读访问。
 */
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type) -> std::optional<ReadPageGuard> {
  const auto profile_enabled = ProfileEnabled();
  const auto op_start_ns = profile_enabled ? ProfileNowNs() : 0;
  std::shared_ptr<FrameHeader> frame;
  std::optional<page_id_t> dirty_writeback_page_id;
  frame_id_t frame_id = -1;
  uint64_t bpm_latch_hold_ns = 0;
  uint64_t evict_ns = 0;
  uint64_t writeback_wait_ns = 0;
  uint64_t readin_wait_ns = 0;
  bool cache_hit = false;
  bool failed_no_frame = false;
  bool did_evict = false;
  bool did_dirty_writeback = false;
  bool did_read_io = false;

  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);
    while (true) {
      const auto lock_acquired_ns = profile_enabled ? ProfileNowNs() : 0;
      auto it = page_table_.find(page_id);
      if (it != page_table_.end()) {
        cache_hit = true;
        frame_id = it->second;
        frame = frames_[frame_id];
        frame->pin_count_.fetch_add(1);
        replacer_->SetEvictable(frame_id, false);
        replacer_->RecordAccess(frame_id, page_id, access_type);
        if (profile_enabled) {
          bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
        }
        break;
      }

      if (pending_pages_.count(page_id) != 0) {
        if (profile_enabled) {
          bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
        }
        bpm_cv_.wait(lock, [&] { return pending_pages_.count(page_id) == 0; });
        continue;
      }

      if (!free_frames_.empty()) {
        frame_id = free_frames_.front();
        free_frames_.pop_front();
        frame = frames_[frame_id];
      } else {
        const auto evict_start_ns = profile_enabled ? ProfileNowNs() : 0;
        auto victim_opt = replacer_->Evict();
        if (profile_enabled) {
          evict_ns += ProfileNowNs() - evict_start_ns;
        }
        if (!victim_opt.has_value()) {
          failed_no_frame = true;
          if (profile_enabled) {
            bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
          }
          break;
        }

        did_evict = true;
        frame_id = victim_opt.value();
        frame = frames_[frame_id];

        if (frame->page_id_.has_value()) {
          const auto old_page_id = frame->page_id_.value();
          page_table_.erase(old_page_id);

          if (frame->is_dirty_) {
            did_dirty_writeback = true;
            dirty_writeback_page_id = old_page_id;
            pending_pages_.insert(old_page_id);
          }
        }
      }

      pending_pages_.insert(page_id);
      if (profile_enabled) {
        bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
      }
      break;
    }
  }

  if (!cache_hit && !failed_no_frame) {
    if (dirty_writeback_page_id.has_value()) {
      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();

      DiskRequest write_req{true, frame->GetDataMut(), dirty_writeback_page_id.value(), std::move(promise)};
      std::vector<DiskRequest> requests;
      requests.push_back(std::move(write_req));

      const auto io_wait_start_ns = profile_enabled ? ProfileNowNs() : 0;
      disk_scheduler_->Schedule(requests);
      future.get();
      if (profile_enabled) {
        writeback_wait_ns += ProfileNowNs() - io_wait_start_ns;
      }
    }

    frame->Reset();

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    DiskRequest read_req{false, frame->GetDataMut(), page_id, std::move(promise)};
    std::vector<DiskRequest> requests;
    requests.push_back(std::move(read_req));

    did_read_io = true;
    const auto io_wait_start_ns = profile_enabled ? ProfileNowNs() : 0;
    disk_scheduler_->Schedule(requests);
    future.get();
    if (profile_enabled) {
      readin_wait_ns += ProfileNowNs() - io_wait_start_ns;
    }

    {
      std::unique_lock<std::mutex> lock(*bpm_latch_);
      const auto lock_acquired_ns = profile_enabled ? ProfileNowNs() : 0;
      if (dirty_writeback_page_id.has_value()) {
        pending_pages_.erase(dirty_writeback_page_id.value());
      }
      frame->page_id_ = page_id;
      frame->pin_count_.store(1);
      frame->is_dirty_ = false;

      page_table_[page_id] = frame_id;
      replacer_->RecordAccess(frame_id, page_id, access_type);
      replacer_->SetEvictable(frame_id, false);
      pending_pages_.erase(page_id);
      if (profile_enabled) {
        bpm_latch_hold_ns += ProfileNowNs() - lock_acquired_ns;
      }
    }
    bpm_cv_.notify_all();
  }

  if (profile_enabled) {
    auto &profile = GetBpmProfile();
    const auto total_ns = ProfileNowNs() - op_start_ns;
    if (cache_hit) {
      profile.read_hits_.fetch_add(1, std::memory_order_relaxed);
      profile.read_hit_ns_total_.fetch_add(total_ns, std::memory_order_relaxed);
    } else {
      profile.read_misses_.fetch_add(1, std::memory_order_relaxed);
      profile.read_miss_ns_total_.fetch_add(total_ns, std::memory_order_relaxed);
      UpdateMax(profile.read_miss_ns_max_, total_ns);
    }
    if (failed_no_frame) {
      profile.no_frame_failures_.fetch_add(1, std::memory_order_relaxed);
    }
    if (did_evict) {
      profile.evictions_.fetch_add(1, std::memory_order_relaxed);
      profile.evict_ns_total_.fetch_add(evict_ns, std::memory_order_relaxed);
      UpdateMax(profile.evict_ns_max_, evict_ns);
    }
    if (did_dirty_writeback) {
      profile.dirty_writebacks_.fetch_add(1, std::memory_order_relaxed);
      profile.writeback_wait_ns_total_.fetch_add(writeback_wait_ns, std::memory_order_relaxed);
      UpdateMax(profile.writeback_wait_ns_max_, writeback_wait_ns);
    }
    if (did_read_io) {
      profile.read_ios_.fetch_add(1, std::memory_order_relaxed);
      profile.readin_wait_ns_total_.fetch_add(readin_wait_ns, std::memory_order_relaxed);
      UpdateMax(profile.readin_wait_ns_max_, readin_wait_ns);
    }
    profile.bpm_latch_hold_ns_total_.fetch_add(bpm_latch_hold_ns, std::memory_order_relaxed);
    UpdateMax(profile.bpm_latch_hold_ns_max_, bpm_latch_hold_ns);
  }

  if (failed_no_frame) {
    return std::nullopt;
  }

  return ReadPageGuard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
}

/**
 * @brief `CheckedWritePage` 的包装函数：若存在则解包内部值。
 *
 * 如果 `CheckedWritePage` 返回 `std::nullopt`，**该函数会直接终止整个进程。**
 *
 * 该函数**仅**用于测试和易用性场景。
 * 如果 buffer pool manager 存在任何内存耗尽的可能，请使用 `CheckedPageWrite` 以便自行处理该情况。
 *
 * 更多实现信息请参阅 `CheckedPageWrite` 的文档。
 *
 * @param page_id 要读取的页面 ID。
 * @param access_type 页面访问类型。
 * @return WritePageGuard 页面保护器，保证对页面数据的独占可变访问。
 */
auto BufferPoolManager::WritePage(page_id_t page_id, AccessType access_type) -> WritePageGuard {
  auto guard_opt = CheckedWritePage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedWritePage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief `CheckedReadPage` 的包装函数：若存在则解包内部值。
 *
 * 如果 `CheckedReadPage` 返回 `std::nullopt`，**该函数会直接终止整个进程。**
 *
 * 该函数**仅**用于测试和易用性场景。
 * 如果 buffer pool manager 存在任何内存耗尽的可能，请使用 `CheckedPageWrite` 以便自行处理该情况。
 *
 * 更多实现信息请参阅 `CheckedPageRead` 的文档。
 *
 * @param page_id 要读取的页面 ID。
 * @param access_type 页面访问类型。
 * @return ReadPageGuard 页面保护器，保证对页面数据的共享只读访问。
 */
auto BufferPoolManager::ReadPage(page_id_t page_id, AccessType access_type) -> ReadPageGuard {
  auto guard_opt = CheckedReadPage(page_id, access_type);

  if (!guard_opt.has_value()) {
    fmt::println(stderr, "\n`CheckedReadPage` failed to bring in page {}\n", page_id);
    std::abort();
  }

  return std::move(guard_opt).value();
}

/**
 * @brief 以不安全方式将某个页面的数据刷新到磁盘。
 *
 * 如果页面已被修改，本函数会把页面数据写回磁盘。
 * 若给定页面不在内存中，本函数返回 `false`。
 *
 * 本函数中不应对页面加锁。
 * 这意味着你需要仔细考虑何时切换 `is_dirty_` 标志位。
 *
 * ### 实现说明
 *
 * 建议在完成 `CheckedReadPage` 和 `CheckedWritePage` 后再实现该函数，
 * 这样通常更容易理解该怎么做。
 *
 * TODO(P1): 补充实现
 *
 * @param page_id 要刷新的页面 page ID。
 * @return 若页表中找不到该页面则返回 `false`；否则返回 `true`。
 */
auto BufferPoolManager::FlushPageUnsafe(page_id_t page_id) -> bool {
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  bpm_cv_.wait(lock, [&] { return pending_pages_.count(page_id) == 0; });

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return false;
  }

  auto frame = frames_[it->second];
  const bool was_unpinned = frame->pin_count_.load() == 0;

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest request{true, frame->GetDataMut(), page_id, std::move(promise)};
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(request));

  disk_scheduler_->Schedule(requests);
  future.get();

  if (was_unpinned) {
    frame->is_dirty_ = false;
  }
  return true;
}

/**
 * @brief 以安全方式将某个页面的数据刷新到磁盘。
 *
 * 如果页面已被修改，本函数会把页面数据写回磁盘。
 * 若给定页面不在内存中，本函数返回 `false`。
 *
 * 本函数中应对页面加锁，以确保写盘的是一致状态。
 *
 * ### 实现说明
 *
 * 建议在完成 `CheckedReadPage`、`CheckedWritePage` 以及 page guard 中的 `Flush`
 * 后再实现该函数，通常会更容易理解该怎么做。
 *
 * TODO(P1): 补充实现
 *
 * @param page_id 要刷新的页面 page ID。
 * @return 若页表中找不到该页面则返回 `false`；否则返回 `true`。
 */
auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::shared_ptr<FrameHeader> frame;
  frame_id_t frame_id;

  {
    std::unique_lock<std::mutex> lock(*bpm_latch_);
    bpm_cv_.wait(lock, [&] { return pending_pages_.count(page_id) == 0; });
    auto it = page_table_.find(page_id);
    if (it == page_table_.end()) {
      return false;
    }

    frame_id = it->second;
    frame = frames_[frame_id];
    frame->pin_count_.fetch_add(1);
    replacer_->SetEvictable(frame_id, false);
  }

  ReadPageGuard guard(page_id, frame, replacer_, bpm_latch_, disk_scheduler_);
  guard.Flush();
  return true;
}

/**
 * @brief 以不安全方式将内存中的所有页面数据刷新到磁盘。
 *
 * 本函数中不应对页面加锁。
 * 这意味着你需要仔细考虑何时切换 `is_dirty_` 标志位。
 *
 * ### 实现说明
 *
 * 建议在完成 `CheckedReadPage`、`CheckedWritePage` 和 `FlushPage` 后再实现该函数，
 * 这样通常更容易理解该怎么做。
 *
 * TODO(P1): 补充实现
 */
void BufferPoolManager::FlushAllPagesUnsafe() {
  std::scoped_lock lock(*bpm_latch_);

  for (const auto &[page_id, frame_id] : page_table_) {
    auto frame = frames_[frame_id];
    const bool was_unpinned = frame->pin_count_.load() == 0;

    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();

    DiskRequest request{true, frame->GetDataMut(), page_id, std::move(promise)};
    std::vector<DiskRequest> requests;
    requests.push_back(std::move(request));

    disk_scheduler_->Schedule(requests);
    future.get();

    if (was_unpinned) {
      frame->is_dirty_ = false;
    }
  }
}

/**
 * @brief 以安全方式将内存中的所有页面数据刷新到磁盘。
 *
 * 本函数中应对页面加锁，以确保写盘的是一致状态。
 *
 * ### 实现说明
 *
 * 建议在完成 `CheckedReadPage`、`CheckedWritePage` 和 `FlushPage` 后再实现该函数，
 * 这样通常更容易理解该怎么做。
 *
 * TODO(P1): 补充实现
 */
void BufferPoolManager::FlushAllPages() {
  std::vector<page_id_t> page_ids;

  {
    std::scoped_lock lock(*bpm_latch_);
    page_ids.reserve(page_table_.size());
    for (const auto &[page_id, frame_id] : page_table_) {
      static_cast<void>(frame_id);
      page_ids.push_back(page_id);
    }
  }

  for (const auto page_id : page_ids) {
    static_cast<void>(FlushPage(page_id));
  }
}

/**
 * @brief 获取某个页面的 pin count。如果页面不在内存中，返回 `std::nullopt`。
 *
 * 本函数是线程安全的。调用方可在多线程环境中调用该函数，多个线程可访问同一页面。
 *
 * 本函数用于测试。如果实现不正确，肯定会导致测试套件和自动评分器出现问题。
 *
 * # 实现说明
 *
 * 我们会使用该函数测试你的 buffer pool manager 是否正确维护 pin count。
 * 由于 `FrameHeader` 中的 `pin_count_` 字段是原子类型，
 * 你不需要获取承载该页面的帧锁；可以直接通过原子 `load` 安全读取该值。
 * 但你仍然需要获取 buffer pool 的 latch。
 *
 * 如果你不熟悉原子类型，请参阅官方 C++ 文档：
 * [here](https://en.cppreference.com/w/cpp/atomic/atomic).
 *
 * TODO(P1): 补充实现
 *
 * @param page_id 我们要获取 pin count 的页面 page ID。
 * @return std::optional<size_t> 若页面存在则返回 pin count；否则返回 `std::nullopt`。
 */
auto BufferPoolManager::GetPinCount(page_id_t page_id) -> std::optional<size_t> {
  std::unique_lock<std::mutex> lock(*bpm_latch_);
  bpm_cv_.wait(lock, [&] { return pending_pages_.count(page_id) == 0; });
  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    return std::nullopt;
  }
  auto frame_id = it->second;
  return frames_[frame_id]->pin_count_.load();
}

}  // namespace bustub
