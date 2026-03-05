// :bustub-keep-private:
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// arc_replacer.cpp
//
// 标识：src/buffer/arc_replacer.cpp
//
// 版权所有 (c) 2015-2025，卡内基梅隆大学数据库小组
//
//===----------------------------------------------------------------------===//

#include "buffer/arc_replacer.h"

#include <optional>
#include <stdexcept>

#include "common/config.h"

namespace bustub {

/**
 *
 * TODO(P1): 补充实现
 *
 * @brief 创建一个新的 ArcReplacer，列表初始为空，目标大小为 0
 * @param num_frames ArcReplacer 需要缓存的最大帧数
 */
ArcReplacer::ArcReplacer(size_t num_frames) : replacer_size_(num_frames) {}

/**
 * TODO(P1): 补充实现
 *
 * @brief 按照说明文档执行 Replace 操作，
 * 根据平衡策略从 mfu_ 或 mru_ 中驱逐页面到对应的 ghost 列表。
 *
 * 如果你想参考原始 ARC 论文，请注意我们的实现有两个变化：
 * 1. 当 mru_ 的大小等于目标大小时，在决定从哪个列表驱逐时，
 * 我们不会像论文那样检查最后一次访问。
 * 这没有问题，因为原论文中该决策本身就是任意的。
 * 2. 不可驱逐条目会被跳过。如果目标侧（mru_ / mfu_）的条目都被 pin 住，
 * 我们会尝试驱逐另一侧（mfu_ / mru_），并将其移动到对应 ghost 列表
 * （mfu_ghost_ / mru_ghost_）。
 *
 * @return 被驱逐帧的 frame id；若无法驱逐则返回 std::nullopt
 */
auto ArcReplacer::Evict() -> std::optional<frame_id_t> {
  std::scoped_lock lock(latch_);
  if (curr_size_ == 0) {
    return std::nullopt;
  }

  auto remove_existing_ghost = [&](page_id_t pid) {
    auto ghost_it = ghost_map_.find(pid);
    if (ghost_it == ghost_map_.end()) {
      return;
    }
    auto st = ghost_it->second;
    if (st->arc_status_ == ArcStatus::MRU_GHOST) {
      mru_ghost_.erase(st->ghost_pos_);
    } else if (st->arc_status_ == ArcStatus::MFU_GHOST) {
      mfu_ghost_.erase(st->ghost_pos_);
    }
    ghost_map_.erase(ghost_it);
  };

  auto evict_from_mru = [&]() -> std::optional<frame_id_t> {
    for (auto it = mru_.begin(); it != mru_.end(); ++it) {
      auto alive_it = alive_map_.find(*it);
      if (alive_it == alive_map_.end()) {
        continue;
      }
      auto st = alive_it->second;
      if (!st->evictable_) {
        continue;
      }

      const frame_id_t victim_fid = st->frame_id_;
      const page_id_t victim_pid = st->page_id_;

      mru_.erase(st->alive_pos_);
      alive_map_.erase(alive_it);

      remove_existing_ghost(victim_pid);
      mru_ghost_.push_back(victim_pid);
      st->arc_status_ = ArcStatus::MRU_GHOST;
      st->evictable_ = false;
      st->ghost_pos_ = std::prev(mru_ghost_.end());
      ghost_map_[victim_pid] = st;

      curr_size_--;
      return victim_fid;
    }
    return std::nullopt;
  };

  auto evict_from_mfu = [&]() -> std::optional<frame_id_t> {
    for (auto it = mfu_.rbegin(); it != mfu_.rend(); ++it) {
      auto alive_it = alive_map_.find(*it);
      if (alive_it == alive_map_.end()) {
        continue;
      }
      auto st = alive_it->second;
      if (!st->evictable_) {
        continue;
      }

      const frame_id_t victim_fid = st->frame_id_;
      const page_id_t victim_pid = st->page_id_;

      mfu_.erase(st->alive_pos_);
      alive_map_.erase(alive_it);

      remove_existing_ghost(victim_pid);
      mfu_ghost_.push_front(victim_pid);
      st->arc_status_ = ArcStatus::MFU_GHOST;
      st->evictable_ = false;
      st->ghost_pos_ = mfu_ghost_.begin();
      ghost_map_[victim_pid] = st;

      curr_size_--;
      return victim_fid;
    }
    return std::nullopt;
  };

  const bool prefer_mfu = (mru_.size() < mru_target_size_);
  if (prefer_mfu) {
    if (auto victim = evict_from_mfu(); victim.has_value()) {
      return victim;
    }
    return evict_from_mru();
  }

  if (auto victim = evict_from_mru(); victim.has_value()) {
    return victim;
  }
  return evict_from_mfu();
}


/**
 * TODO(P1): 补充实现
 *
 * @brief 记录一次帧访问，并相应调整 ARC 的元数据：
 * 若访问页已存在于任一列表中，则将其移动到 mfu_ 头部；
 * 否则将其放到 mru_ 头部。
 *
 * 执行原论文中除 REPLACE 之外的操作，REPLACE 由 `Evict()` 处理。
 *
 * 请按以下四种情况处理：
 * 1. 访问命中 mru_ 或 mfu_
 * 2/3. 访问命中 mru_ghost_ / mfu_ghost_
 * 4. 访问未命中所有列表
 *
 * 该过程会完成对四个列表的所有修改，为 `Evict()` 简化为
 * “查找并驱逐受害者到 ghost 列表”做准备。
 *
 * 注意：frame_id 用作存活页面的标识，
 * page_id 用作 ghost 页面标识，因为页面“死亡”后 page_id 才是唯一标识。
 * 对存活页面使用 page_id 理论上等价（一一映射），
 * 但使用 frame_id 更直观。
 *
 * @param frame_id 收到新访问的帧 ID。
 * @param page_id 映射到该帧的页面 ID。
 * @param access_type 本次访问类型。该参数仅用于排行榜测试。
 */
void ArcReplacer::RecordAccess(frame_id_t frame_id, page_id_t page_id, [[maybe_unused]] AccessType access_type) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::runtime_error("invalid frame id");
  }

  auto alive_it = alive_map_.find(frame_id);
  if (alive_it != alive_map_.end()) {
    auto st = alive_it->second;
    st->page_id_ = page_id;
    if (st->arc_status_ == ArcStatus::MRU) {
      mru_.erase(st->alive_pos_);
    } else if (st->arc_status_ == ArcStatus::MFU) {
      mfu_.erase(st->alive_pos_);
    } else {
      throw std::runtime_error("alive frame has invalid arc status");
    }
    mfu_.push_front(frame_id);
    st->arc_status_ = ArcStatus::MFU;
    st->alive_pos_ = mfu_.begin();
    return;
  }

  auto ghost_it = ghost_map_.find(page_id);
  if (ghost_it != ghost_map_.end()) {
    const size_t mru_ghost_sz = mru_ghost_.size();
    const size_t mfu_ghost_sz = mfu_ghost_.size();
    auto st = ghost_it->second;

    if (st->arc_status_ == ArcStatus::MRU_GHOST) {
      const size_t delta = (mru_ghost_sz >= mfu_ghost_sz) ? 1 : (mfu_ghost_sz / mru_ghost_sz);
      mru_target_size_ = std::min(replacer_size_, mru_target_size_ + delta);
      mru_ghost_.erase(st->ghost_pos_);
    } else if (st->arc_status_ == ArcStatus::MFU_GHOST) {
      const size_t delta = (mfu_ghost_sz >= mru_ghost_sz) ? 1 : (mru_ghost_sz / mfu_ghost_sz);
      mru_target_size_ = (delta >= mru_target_size_) ? 0 : (mru_target_size_ - delta);
      mfu_ghost_.erase(st->ghost_pos_);
    } else {
      throw std::runtime_error("ghost page has invalid arc status");
    }

    ghost_map_.erase(ghost_it);

    st->frame_id_ = frame_id;
    st->page_id_ = page_id;
    st->evictable_ = false;
    st->arc_status_ = ArcStatus::MFU;
    mfu_.push_front(frame_id);
    st->alive_pos_ = mfu_.begin();
    alive_map_[frame_id] = st;
    return;
  }

  auto drop_oldest_mru_ghost = [&]() {
    if (mru_ghost_.empty()) {
      return;
    }
    const page_id_t old_pid = mru_ghost_.front();
    auto old_it = ghost_map_.find(old_pid);
    if (old_it != ghost_map_.end()) {
      mru_ghost_.erase(old_it->second->ghost_pos_);
      ghost_map_.erase(old_it);
    } else {
      mru_ghost_.pop_front();
    }
  };

  auto drop_oldest_mfu_ghost = [&]() {
    if (mfu_ghost_.empty()) {
      return;
    }
    const page_id_t old_pid = mfu_ghost_.back();
    auto old_it = ghost_map_.find(old_pid);
    if (old_it != ghost_map_.end()) {
      mfu_ghost_.erase(old_it->second->ghost_pos_);
      ghost_map_.erase(old_it);
    } else {
      mfu_ghost_.pop_back();
    }
  };

  if (mru_.size() + mru_ghost_.size() == replacer_size_) {
    if (!mru_ghost_.empty()) {
      drop_oldest_mru_ghost();
    }
  } else if (mru_.size() + mru_ghost_.size() < replacer_size_) {
    const size_t total = mru_.size() + mfu_.size() + mru_ghost_.size() + mfu_ghost_.size();
    if (total == (2 * replacer_size_)) {
      drop_oldest_mfu_ghost();
    }
  }

  auto st = std::make_shared<FrameStatus>(page_id, frame_id, false, ArcStatus::MRU);
  mru_.push_back(frame_id);
  st->alive_pos_ = std::prev(mru_.end());
  alive_map_[frame_id] = st;
}

/**
 * TODO(P1): 补充实现
 *
 * @brief 切换帧是否可驱逐。该函数也会控制 replacer 的大小。
 * 注意 size 等于可驱逐条目的数量。
 *
 * 如果某帧之前可驱逐，现在设为不可驱逐，则 size 应减少。
 * 如果某帧之前不可驱逐，现在设为可驱逐，则 size 应增加。
 *
 * 如果 frame id 非法，应抛出异常或终止进程。
 *
 * 其他场景下，该函数应直接结束且不修改任何内容。
 *
 * @param frame_id 要修改“可驱逐”状态的帧 ID
 * @param set_evictable 给定帧是否设为可驱逐
 */
void ArcReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);

  if (frame_id < 0 || static_cast<size_t>(frame_id) > replacer_size_) {
    throw std::runtime_error("invalid frame id");
  }

  auto now_frame = alive_map_.find(frame_id);
  if (now_frame == alive_map_.end()) {
    return;
  }

  if (now_frame->second->evictable_ == set_evictable) {
    return;
  }

  if (set_evictable) {
    curr_size_++;
  } else {
    curr_size_--;
  }
  now_frame->second->evictable_ = set_evictable;
}

/**
 * TODO(P1): 补充实现
 *
 * @brief 从 replacer 中移除一个可驱逐帧。
 * 如果移除成功，该函数也应减少 replacer 的大小。
 *
 * 注意这与驱逐帧不同。驱逐总是移除 ARC 算法决定的帧。
 *
 * 如果在不可驱逐帧上调用 Remove，应抛出异常或终止进程。
 *
 * 如果找不到指定帧，应直接返回。
 *
 * @param frame_id 要移除的帧 ID
 */
void ArcReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);
  auto now_frame = alive_map_.find(frame_id);
  if (now_frame == alive_map_.end()) {
    return;
  }

  auto st = now_frame->second;
  if (!st->evictable_) {
    throw std::runtime_error("frame is not evictable");
  }

  if (st->arc_status_ == ArcStatus::MRU) {
    mru_.erase(st->alive_pos_);
  } else if (st->arc_status_ == ArcStatus::MFU) {
    mfu_.erase(st->alive_pos_);
  } else {
    throw std::runtime_error("frame is not in mru or mfu");
  }
  alive_map_.erase(now_frame);
  curr_size_--;
}

/**
 * TODO(P1): 补充实现
 *
 * @brief 返回 replacer 的大小，即可驱逐帧的数量。
 *
 * @return size_t
 */

auto ArcReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return curr_size_;
}

}  // 命名空间 bustub
