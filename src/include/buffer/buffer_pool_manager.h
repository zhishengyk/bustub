//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：buffer_pool_manager.h
//
// 标识：src/include/buffer/buffer_pool_manager.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <shared_mutex>
#include <unordered_map>
#include <vector>

#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "recovery/log_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

class BufferPoolManager;
class ReadPageGuard;
class WritePageGuard;

/**
 * @brief `BufferPoolManager` 的辅助类，用于管理一个内存帧及其相关元数据。
 *
 * 该类表示 `BufferPoolManager` 用来存放页面数据的内存帧头。注意，实际的帧内存并不直接存储在
 * `FrameHeader` 内部，而是由 `FrameHeader` 持有指向帧数据的指针，并与 `FrameHeader` 分开存放。
 *
 * ---
 *
 * 你可能会关心（也可能不关心）为什么字段 `data_` 被设计成按需分配的 vector，而不是指向某块预分配
 * 内存的直接指针。
 *
 * 在传统的生产级缓冲池管理器中，缓冲池要管理的内存通常会一次性分配为一个连续的大数组（可以理解为一次
 * 很大的 `malloc`，预先分配数 GB 内存）。随后再把这块连续内存切分为连续的帧。换句话说，帧是以数组基址
 * 为起点、按页面大小（4 KB）步长的偏移来定义的。
 *
 * 在 BusTub 中，我们改为单独分配每个帧（通过 `std::vector<char>`），以便更容易借助 address sanitizer
 * 检测缓冲区溢出。由于 C++ 本身没有内存安全保障，如果所有页面都连续存放，就很容易把页面数据指针强转为
 * 大类型后越界写入并覆盖其他页面数据。
 *
 * 如果你想尝试为自己的缓冲池管理器使用更高效的数据结构，可以自由尝试。不过，在后续项目（尤其是 project 2）
 * 中，能够检测 buffer overflow 往往会带来明显收益。
 */
class FrameHeader {
  friend class BufferPoolManager;
  friend class ReadPageGuard;
  friend class WritePageGuard;

 public:
  explicit FrameHeader(frame_id_t frame_id);

 private:
  auto GetData() const -> const char *;
  auto GetDataMut() -> char *;
  void Reset();

  /** @brief 该头部所表示帧的帧 ID / 下标。 */
  const frame_id_t frame_id_;

  /** @brief 该帧的读写闩锁。 */
  std::shared_mutex rwlatch_;

  /** @brief 该帧上的 pin 数量，用于确保页面留在内存中。 */
  std::atomic<size_t> pin_count_;

  /** @brief 脏页标记。 */
  bool is_dirty_;

  /**
   * @brief 指向该帧所持有页面数据的指针。
   *
   * 若该帧当前不持有任何页面数据，则帧内容为全 0 字节。
   */
  std::vector<char> data_;

  /**
   * TODO(P1): 你可以在此处添加你认为必要的字段或辅助函数。
   *
   * 一个可选优化是：存储当前 `FrameHeader` 所承载页面的可选 page ID。
   * 这样可能让你在缓冲池管理器的其他位置跳过对对应 (page ID, frame ID) 对的查找。
   */
};

/**
 * @brief `BufferPoolManager` 类声明。
 *
 * 正如说明文档所述，buffer pool 负责在主存缓冲区与持久化存储之间来回移动物理数据页。
 * 它同时也充当缓存：将常用页面保留在内存中以加快访问，并将不常用或冷页面驱逐回存储。
 *
 * 在实现 buffer pool manager 前，请确保完整阅读说明文档。
 * 你还需要先完成 `ArcReplacer` 和 `DiskManager` 类的实现。
 */
class BufferPoolManager {
 public:
  BufferPoolManager(size_t num_frames, DiskManager *disk_manager, LogManager *log_manager = nullptr);
  ~BufferPoolManager();

  auto Size() const -> size_t;
  auto NewPage() -> page_id_t;
  auto DeletePage(page_id_t page_id) -> bool;
  auto CheckedWritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown)
      -> std::optional<WritePageGuard>;
  auto CheckedReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> std::optional<ReadPageGuard>;
  auto WritePage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> WritePageGuard;
  auto ReadPage(page_id_t page_id, AccessType access_type = AccessType::Unknown) -> ReadPageGuard;
  auto FlushPageUnsafe(page_id_t page_id) -> bool;
  auto FlushPage(page_id_t page_id) -> bool;
  void FlushAllPagesUnsafe();
  void FlushAllPages();
  auto GetPinCount(page_id_t page_id) -> std::optional<size_t>;

 private:
  /** @brief 缓冲池中的帧数量。 */
  const size_t num_frames_;

  /** @brief 下一个待分配的页面 ID。 */
  std::atomic<page_id_t> next_page_id_;

  /**
   * @brief 用于保护缓冲池内部数据结构的闩锁。
   *
   * TODO(P1): 建议将此注释替换为“该闩锁实际保护哪些内容”的具体说明。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /** @brief 该缓冲池所管理帧对应的帧头集合。 */
  std::vector<std::shared_ptr<FrameHeader>> frames_;

  /** @brief 用于维护页面与缓冲池帧映射关系的页表。 */
  std::unordered_map<page_id_t, frame_id_t> page_table_;

  /** @brief 不持有任何页面数据的空闲帧列表。 */
  std::list<frame_id_t> free_frames_;

  /** @brief 用于查找未 pin / 可驱逐候选页面的 replacer。 */
  std::shared_ptr<ArcReplacer> replacer_;

  /** @brief 指向磁盘调度器的指针。与 page guard 共享，用于刷盘。 */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief 指向日志管理器的指针。
   *
   * 注意：P1 中请忽略该字段。
   */
  LogManager *log_manager_ __attribute__((__unused__));

  /**
   * TODO(P1): 如果你认为有必要，可以添加额外的私有成员和辅助函数。
   *
   * 不同页面访问模式之间很可能会出现较多重复代码。
   *
   * 建议实现一个辅助函数，用于返回“空闲且内部无数据”的帧 ID。
   * 另外，你也可以实现一个辅助函数，用于返回“已经存有某页面数据的 `FrameHeader` 的共享指针”
   * 或该 `FrameHeader` 的下标。
   */
};
}  // 命名空间 bustub
