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
#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

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
auto BufferPoolManager::NewPage() -> page_id_t { 
  return next_page_id_.fetch_add(1);
}

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
  std::scoped_lock lock(*bpm_latch_);

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
  std::shared_ptr<FrameHeader> frame;

  {
    std::scoped_lock lock(*bpm_latch_);
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
      const auto frame_id = it->second;
      frame = frames_[frame_id];
      frame->pin_count_.fetch_add(1);
      replacer_->SetEvictable(frame_id, false);
      replacer_->RecordAccess(frame_id, page_id, access_type);
    } else {
      frame_id_t frame_id;

      if (!free_frames_.empty()) {
        frame_id = free_frames_.front();
        free_frames_.pop_front();
        frame = frames_[frame_id];
      } else {
        auto victim_opt = replacer_->Evict();
        if (!victim_opt.has_value()) {
          return std::nullopt;
        }
        frame_id = victim_opt.value();
        frame = frames_[frame_id];

        if (frame->page_id_.has_value()) {
          const auto old_page_id = frame->page_id_.value();
          page_table_.erase(old_page_id);

          if (frame->is_dirty_) {
            auto promise = disk_scheduler_->CreatePromise();
            auto future = promise.get_future();

            DiskRequest write_req{true, frame->GetDataMut(), old_page_id, std::move(promise)};
            std::vector<DiskRequest> requests;
            requests.push_back(std::move(write_req));

            disk_scheduler_->Schedule(requests);
            future.get();
          }
        }
      }

      frame->Reset();

      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();

      DiskRequest read_req{false, frame->GetDataMut(), page_id, std::move(promise)};
      std::vector<DiskRequest> requests;
      requests.push_back(std::move(read_req));

      disk_scheduler_->Schedule(requests);
      future.get();

      frame->page_id_ = page_id;
      frame->pin_count_.store(1);
      frame->is_dirty_ = false;

      page_table_[page_id] = frame_id;
      replacer_->RecordAccess(frame_id, page_id, access_type);
      replacer_->SetEvictable(frame_id, false);
    }
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
auto BufferPoolManager::CheckedReadPage(page_id_t page_id, AccessType access_type)
-> std::optional<ReadPageGuard> {
  std::shared_ptr<FrameHeader> frame;

  {
    std::scoped_lock lock(*bpm_latch_);
    auto it = page_table_.find(page_id);
    if (it != page_table_.end()) {
      const auto frame_id = it->second;
      frame = frames_[frame_id];
      frame->pin_count_.fetch_add(1);
      replacer_->SetEvictable(frame_id, false);
      replacer_->RecordAccess(frame_id, page_id, access_type);
    } else {
      frame_id_t frame_id;

      if (!free_frames_.empty()) {
        frame_id = free_frames_.front();
        free_frames_.pop_front();
        frame = frames_[frame_id];
      } else {
        auto victim_opt = replacer_->Evict();
        if (!victim_opt.has_value()) {
          return std::nullopt;
        }
        frame_id = victim_opt.value();
        frame = frames_[frame_id];

        if (frame->page_id_.has_value()) {
          const auto old_page_id = frame->page_id_.value();
          page_table_.erase(old_page_id);

          if (frame->is_dirty_) {
            auto promise = disk_scheduler_->CreatePromise();
            auto future = promise.get_future();

            DiskRequest write_req{true, frame->GetDataMut(), old_page_id, std::move(promise)};
            std::vector<DiskRequest> requests;
            requests.push_back(std::move(write_req));

            disk_scheduler_->Schedule(requests);
            future.get();
          }
        }
      }

      frame->Reset();

      auto promise = disk_scheduler_->CreatePromise();
      auto future = promise.get_future();

      DiskRequest read_req{false, frame->GetDataMut(), page_id, std::move(promise)};
      std::vector<DiskRequest> requests;
      requests.push_back(std::move(read_req));

      disk_scheduler_->Schedule(requests);
      future.get();

      frame->page_id_ = page_id;
      frame->pin_count_.store(1);
      frame->is_dirty_ = false;

      page_table_[page_id] = frame_id;
      replacer_->RecordAccess(frame_id, page_id, access_type);
      replacer_->SetEvictable(frame_id, false);
    }
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
  std::scoped_lock lock(*bpm_latch_);

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
    std::scoped_lock lock(*bpm_latch_);
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
  std::scoped_lock lock(*bpm_latch_);
  auto it = page_table_.find(page_id);
  if(it == page_table_.end()){
    return std::nullopt;
  }
  auto frame_id = it->second;
  return frames_[frame_id]->pin_count_.load();
}

}  // 命名空间 bustub
