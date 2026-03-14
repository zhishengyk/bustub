//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// 文件标识: src/include/storage/page/page_guard.h
//
// 版权所有 (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "buffer/arc_replacer.h"
#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;
class FrameHeader;

/**
 * @brief 一个提供页面数据线程安全读访问的 RAII 对象。
 *
 * 在 BusTub 系统中，与 buffer pool 中页面数据交互的_唯一_方式应该是通过 page guard。
 * 由于 `ReadPageGuard` 是一个 RAII 对象，系统不需要手动加锁和解锁页面闩锁。
 *
 * 对于 `ReadPageGuard`，可以有多个线程共享读取同一个页面的数据。
 * 但是，只要某个页面上存在任意一个 `ReadPageGuard`，就不应该有线程修改该页面数据。
 */
class ReadPageGuard {
  /** @brief 只有 buffer pool manager 可以构造一个有效的 `ReadPageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief `ReadPageGuard` 的默认构造函数。
   *
   * 注意，我们绝对不希望使用一个仅通过默认构造创建出来的 guard。
   * 我们之所以定义这个默认构造函数，只是为了允许先在栈上放一个“未初始化”的 guard，
   * 之后再通过 `=` 对它进行移动赋值。
   *
   * **使用未初始化的 page guard 属于未定义行为。**
   *
   * 换句话说，获得一个有效 `ReadPageGuard` 的唯一方式是通过 buffer pool manager。
   */
  ReadPageGuard() = default;

  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;
  ReadPageGuard(ReadPageGuard &&that) noexcept;
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto IsDirty() const -> bool;
  void Flush();
  void Drop();
  ~ReadPageGuard();

 private:
  /** @brief 只有 buffer pool manager 可以构造一个有效的 `ReadPageGuard`。 */
  explicit ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<ArcReplacer> replacer,
                         std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

  /** @brief 当前 guard 正在保护的页面 ID。 */
  page_id_t page_id_{INVALID_PAGE_ID};

  /**
   * @brief 保存当前 guard 所保护页面的 frame。
   *
   * 这个 page guard 的几乎所有操作都应该通过这个指向 `FrameHeader` 的共享指针完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief 指向 buffer pool 的 replacer 的共享指针。
   *
   * 由于 buffer pool 无法知道这个 `ReadPageGuard` 何时析构，我们需要保存一个指向
   * buffer pool replacer 的指针，以便在析构时把对应 frame 重新标记为可驱逐。
   */
  std::shared_ptr<ArcReplacer> replacer_;

  /**
   * @brief 指向 buffer pool 闩锁的共享指针。
   *
   * 由于 buffer pool 无法知道这个 `ReadPageGuard` 何时析构，我们需要保存一个指向
   * buffer pool 闩锁的指针，以便在需要更新 replacer 中 frame 的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief 指向 buffer pool 磁盘调度器的共享指针。
   *
   * 在将页面刷回磁盘时使用。
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief 这个 `ReadPageGuard` 的有效性标记。
   *
   * 由于我们必须允许构造无效的 page guard（见上面的说明），因此需要维护某种状态来表明
   * 当前 page guard 是否有效。注意，默认构造函数会自动把这个字段设为 `false`。
   *
   * 如果没有这个标记，那么移动构造函数 / 移动赋值运算符在析构或调用 `Drop()` 时
   * 可能会错误地处理无效成员，从而导致段错误。
   */
  bool is_valid_{false};

  /**
   * TODO(P1): 你可以在这里添加你认为必要的字段。
   *
   * 如果你想写得更“花”一点，可以研究一下 `std::shared_lock`，并用它来实现加锁逻辑，
   * 而不是手动调用 `lock` 和 `unlock`。
   */
};

/**
 * @brief 一个提供页面数据线程安全写访问的 RAII 对象。
 *
 * 在 BusTub 系统中，与 buffer pool 中页面数据交互的_唯一_方式应该是通过 page guard。
 * 由于 `WritePageGuard` 是一个 RAII 对象，系统不需要手动加锁和解锁页面闩锁。
 *
 * 对于 `WritePageGuard`，同一时间只能有一个线程独占拥有页面数据的访问权。
 * 这意味着持有 `WritePageGuard` 的线程可以任意修改页面数据。
 * 但是，只要某个页面上存在 `WritePageGuard`，同一时间就不应该再存在其他
 * `WritePageGuard` 或该页面上的 `ReadPageGuard`。
 */
class WritePageGuard {
  /** @brief 只有 buffer pool manager 可以构造一个有效的 `WritePageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief `WritePageGuard` 的默认构造函数。
   *
   * 注意，我们绝对不希望使用一个仅通过默认构造创建出来的 guard。
   * 我们之所以定义这个默认构造函数，只是为了允许先在栈上放一个“未初始化”的 guard，
   * 之后再通过 `=` 对它进行移动赋值。
   *
   * **使用未初始化的 page guard 属于未定义行为。**
   *
   * 换句话说，获得一个有效 `WritePageGuard` 的唯一方式是通过 buffer pool manager。
   */
  WritePageGuard() = default;

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;
  WritePageGuard(WritePageGuard &&that) noexcept;
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto GetDataMut() -> char *;
  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }
  auto IsDirty() const -> bool;
  void Flush();
  void Drop();
  ~WritePageGuard();

 private:
  /** @brief 只有 buffer pool manager 可以构造一个有效的 `WritePageGuard`。 */
  explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<ArcReplacer> replacer,
                          std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

  /** @brief 当前 guard 正在保护的页面 ID。 */
  page_id_t page_id_{INVALID_PAGE_ID};

  /**
   * @brief 保存当前 guard 所保护页面的 frame。
   *
   * 这个 page guard 的几乎所有操作都应该通过这个指向 `FrameHeader` 的共享指针完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief 指向 buffer pool 的 replacer 的共享指针。
   *
   * 由于 buffer pool 无法知道这个 `WritePageGuard` 何时析构，我们需要保存一个指向
   * buffer pool replacer 的指针，以便在析构时把对应 frame 重新标记为可驱逐。
   */
  std::shared_ptr<ArcReplacer> replacer_;

  /**
   * @brief 指向 buffer pool 闩锁的共享指针。
   *
   * 由于 buffer pool 无法知道这个 `WritePageGuard` 何时析构，我们需要保存一个指向
   * buffer pool 闩锁的指针，以便在需要更新 replacer 中 frame 的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief 指向 buffer pool 磁盘调度器的共享指针。
   *
   * 在将页面刷回磁盘时使用。
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief 这个 `WritePageGuard` 的有效性标记。
   *
   * 由于我们必须允许构造无效的 page guard（见上面的说明），因此需要维护某种状态来表明
   * 当前 page guard 是否有效。注意，默认构造函数会自动把这个字段设为 `false`。
   *
   * 如果没有这个标记，那么移动构造函数 / 移动赋值运算符在析构或调用 `Drop()` 时
   * 可能会错误地处理无效成员，从而导致段错误。
   */
  bool is_valid_{false};

  /**
   * TODO(P1): 你可以在这里添加你认为必要的字段。
   *
   * 如果你想写得更“花”一点，可以研究一下 `std::unique_lock`，并用它来实现加锁逻辑，
   * 而不是手动调用 `lock` 和 `unlock`。
   */
};

}  // namespace bustub
