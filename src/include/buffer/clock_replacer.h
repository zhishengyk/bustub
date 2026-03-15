//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：clock_replacer.h
//
// 标识：src/include/buffer/clock_replacer.h
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * ClockReplacer 实现时钟替换策略，它是最近最少使用（LRU）策略的近似。
 */
class ClockReplacer : public Replacer {
 public:
  explicit ClockReplacer(size_t num_pages);

  ~ClockReplacer() override;

  auto Victim(frame_id_t *frame_id) -> bool override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  auto Size() -> size_t override;

 private:
  // TODO(student): 请实现！
};

}  // namespace bustub
