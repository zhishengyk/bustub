//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：lru_replacer.cpp
//
// 标识：src/buffer/lru_replacer.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

/**
 * 创建一个新的 LRUReplacer。
 * @param num_pages LRUReplacer 需要存储的最大页数
 */
LRUReplacer::LRUReplacer(size_t num_pages) {}

/**
 * 销毁 LRUReplacer。
 */
LRUReplacer::~LRUReplacer() = default;

auto LRUReplacer::Victim(frame_id_t *frame_id) -> bool { return false; }

void LRUReplacer::Pin(frame_id_t frame_id) {}

void LRUReplacer::Unpin(frame_id_t frame_id) {}

auto LRUReplacer::Size() -> size_t { return 0; }

}  // 命名空间 bustub
