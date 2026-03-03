//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：clock_replacer.cpp
//
// 标识：src/buffer/clock_replacer.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"

namespace bustub {

/**
 * 创建一个新的 ClockReplacer。
 * @param num_pages ClockReplacer 需要存储的最大页数
 */
ClockReplacer::ClockReplacer(size_t num_pages) {}

/**
 * 销毁 ClockReplacer。
 */
ClockReplacer::~ClockReplacer() = default;

auto ClockReplacer::Victim(frame_id_t *frame_id) -> bool { return false; }

void ClockReplacer::Pin(frame_id_t frame_id) {}

void ClockReplacer::Unpin(frame_id_t frame_id) {}

auto ClockReplacer::Size() -> size_t { return 0; }

}  // 命名空间 bustub
