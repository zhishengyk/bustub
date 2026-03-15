//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// 文件：lru_k_replacer.cpp
//
// 标识：src/buffer/lru_k_replacer.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): 补充实现
 *
 * @brief 创建一个新的 LRUKReplacer。
 * @param num_frames LRUReplacer 需要存储的最大帧数
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): 补充实现
 *
 * @brief 找到反向 k 距离最大的帧并将其驱逐。只有被标记为“可驱逐”的帧
 * 才是候选驱逐对象。
 *
 * 历史访问次数少于 k 次的帧，其反向 k 距离记为 +inf。
 * 如果多个帧的反向 k 距离都为 inf，则驱逐最早时间戳最久远的帧。
 *
 * 成功驱逐一个帧后，应减少 replacer 的大小，并移除该帧的访问历史。
 *
 * @return 若成功驱逐则返回帧 ID；若没有可驱逐帧则返回 `std::nullopt`。
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { return std::nullopt; }

/**
 * TODO(P1): 补充实现
 *
 * @brief 记录给定 frame id 在当前时间戳的一次访问事件。
 * 如果此前未见过该 frame id，则为其访问历史创建新条目。
 *
 * 如果 frame id 非法（例如大于 replacer_size_），应抛出异常。你也可以在
 * frame id 非法时使用 BUSTUB_ASSERT 终止进程。
 *
 * @param frame_id 收到新访问的帧 ID。
 * @param access_type 本次访问的类型。该参数仅用于排行榜测试。
 */
void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {}

/**
 * TODO(P1): 补充实现
 *
 * @brief 切换帧是否可驱逐。该函数也会控制 replacer 的大小。
 * 注意 size 等于可驱逐条目的数量。
 *
 * 如果某帧之前可驱逐，现在被设为不可驱逐，则 size 应减少。
 * 如果某帧之前不可驱逐，现在被设为可驱逐，则 size 应增加。
 *
 * 如果 frame id 非法，应抛出异常或终止进程。
 *
 * 其他场景下，该函数应直接结束且不修改任何内容。
 *
 * @param frame_id 要修改“可驱逐”状态的帧 ID
 * @param set_evictable 给定帧是否设为可驱逐
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {}

/**
 * TODO(P1): 补充实现
 *
 * @brief 从 replacer 中移除一个可驱逐帧及其访问历史。
 * 如果移除成功，该函数也应减少 replacer 的大小。
 *
 * 注意这与驱逐帧不同。驱逐总是移除反向 k 距离最大的帧；
 * 本函数移除指定 frame id，不考虑其反向 k 距离。
 *
 * 如果在不可驱逐帧上调用 Remove，应抛出异常或终止进程。
 *
 * 如果找不到指定帧，应直接返回。
 *
 * @param frame_id 要移除的帧 ID
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {}

/**
 * TODO(P1): 补充实现
 *
 * @brief 返回 replacer 的大小，即可驱逐帧的数量。
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t { return 0; }

}  // namespace bustub
