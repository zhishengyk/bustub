//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：replacer.h
//
// 标识：src/include/buffer/replacer.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/config.h"

namespace bustub {

/**
 * Replacer 是用于跟踪页面使用情况的抽象类。
 */
class Replacer {
 public:
  Replacer() = default;
  virtual ~Replacer() = default;

  /**
   * 根据替换策略移除受害帧。
   * @param[out] frame_id 被移除的帧 ID；若未找到受害帧则为 nullptr
   * @return 若找到受害帧返回 true，否则返回 false
   */
  virtual auto Victim(frame_id_t *frame_id) -> bool = 0;

  /**
   * 将一个帧置为 pin，表示在 unpin 前不应被作为受害帧。
   * @param frame_id 要 pin 的帧 ID
   */
  virtual void Pin(frame_id_t frame_id) = 0;

  /**
   * 将一个帧解除 pin，表示现在可以被作为受害帧。
   * @param frame_id 要 unpin 的帧 ID
   */
  virtual void Unpin(frame_id_t frame_id) = 0;

  /** @return replacer 中可作为受害帧的元素数量 */
  virtual auto Size() -> size_t = 0;
};

}  // namespace bustub
