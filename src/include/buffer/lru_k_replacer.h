//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：lru_k_replacer.h
//
// 标识：src/include/buffer/lru_k_replacer.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "buffer/arc_replacer.h"
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

class LRUKNode {
 private:
  /** 该页面最近 K 次访问时间戳的历史记录。最久远的时间戳存放在前面。 */
  // 如果开始使用这些字段，请移除 maybe_unused。你可以按需调整成员变量。

  [[maybe_unused]] std::list<size_t> history_;
  [[maybe_unused]] size_t k_;
  [[maybe_unused]] frame_id_t fid_;
  [[maybe_unused]] bool is_evictable_{false};
};

/**
 * LRUKReplacer 实现 LRU-k 替换策略。
 *
 * LRU-k 算法会驱逐反向 k 距离在所有帧中最大的帧。
 * 反向 k 距离定义为：当前时间戳与该帧第 k 次最近访问时间戳的时间差。
 *
 * 若某个帧的历史访问次数少于 k，则其反向 k 距离视为 +inf。
 * 当多个帧的反向 k 距离都为 +inf 时，使用经典 LRU 算法选择受害帧。
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): 补充实现
   *
   * @brief 销毁 LRUKReplacer。
   */
  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(student): 请实现！你可以按需替换这些成员变量。
  // 如果开始使用这些字段，请移除 maybe_unused。
  [[maybe_unused]] std::unordered_map<frame_id_t, LRUKNode> node_store_;
  [[maybe_unused]] size_t current_timestamp_{0};
  [[maybe_unused]] size_t curr_size_{0};
  [[maybe_unused]] size_t replacer_size_;
  [[maybe_unused]] size_t k_;
  [[maybe_unused]] std::mutex latch_;
};

}  // namespace bustub
