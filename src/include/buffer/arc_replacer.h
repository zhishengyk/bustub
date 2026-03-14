
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：arc_replacer.h
//
// 标识：src/include/buffer/arc_replacer.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <memory>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

enum class ArcStatus { MRU, MFU, MRU_GHOST, MFU_GHOST };

// TODO(student): 你可以按需修改或移除这个结构体。
struct FrameStatus {
  page_id_t page_id_;
  frame_id_t frame_id_;
  bool evictable_;
  ArcStatus arc_status_;
  // 当条目在 alive 列表（mru_/mfu_）中时，记录其 O(1) 定位迭代器。
  std::list<frame_id_t>::iterator alive_pos_;
  // 当条目在 ghost 列表（mru_ghost_/mfu_ghost_）中时，记录其 O(1) 定位迭代器。
  std::list<page_id_t>::iterator ghost_pos_;
  FrameStatus(page_id_t pid, frame_id_t fid, bool ev, ArcStatus st)
      : page_id_(pid), frame_id_(fid), evictable_(ev), arc_status_(st) {}
};

/**
 * ArcReplacer 实现 ARC 替换策略。
 */
class ArcReplacer {
 public:
  explicit ArcReplacer(size_t num_frames);

  DISALLOW_COPY_AND_MOVE(ArcReplacer);

  /**
   * TODO(P1): 补充实现
   *
   * @brief 销毁 ArcReplacer。
   */
  ~ArcReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;
  void RecordAccess(frame_id_t frame_id, page_id_t page_id, AccessType access_type = AccessType::Unknown);
  void SetEvictable(frame_id_t frame_id, bool set_evictable);
  void Remove(frame_id_t frame_id);
  auto Size() -> size_t;

 private:
  // TODO(student): 请实现！你可以按需替换或移除这些成员变量。
  std::list<frame_id_t> mru_;
  std::list<frame_id_t> mfu_;
  std::list<page_id_t> mru_ghost_;
  std::list<page_id_t> mfu_ghost_;

  /* 记录 mru_ 和 mfu_ 中的条目
   * 这里使用 frame_id_t 来保证同一存活帧不会出现重复记录 */
  std::unordered_map<frame_id_t, std::shared_ptr<FrameStatus>> alive_map_;
  /* 记录 mru_ghost_ 和 mfu_ghost_ 中的条目
   * 这里使用 page_id_t 而不是 frame_id_t，因为在 ghost 列表中
   * page_id 是唯一标识 */
  std::unordered_map<page_id_t, std::shared_ptr<FrameStatus>> ghost_map_;

  /* 存活且可驱逐条目的数量 */
  [[maybe_unused]] size_t curr_size_{0};
  /* 原始论文中的 p */
  [[maybe_unused]] size_t mru_target_size_{0};
  /* 原始论文中的 c */
  [[maybe_unused]] size_t replacer_size_;
  std::mutex latch_;

  // TODO(student): 你可以按需添加成员变量 / 函数。
};

}  // namespace bustub
