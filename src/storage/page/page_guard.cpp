//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// 文件标识: src/storage/page/page_guard.cpp
//
// 版权所有 (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>
#include "buffer/arc_replacer.h"
#include "common/macros.h"

namespace bustub {

/**
 * @brief 唯一一个能创建有效 `ReadPageGuard` 的 RAII 构造函数。
 *
 * 注意，只有 buffer pool manager 可以调用这个构造函数。
 *
 * TODO(P1): 补充实现。
 *
 * @param page_id 我们要读取的页面 ID。
 * @param frame 指向保存该页面的 frame 的共享指针。
 * @param replacer 指向 buffer pool manager 的 replacer 的共享指针。
 * @param bpm_latch 指向 buffer pool manager 的全局闩锁的共享指针。
 * @param disk_scheduler 指向 buffer pool manager 的磁盘调度器的共享指针。
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock_shared();
  is_valid_ = true;
}

/**
 * @brief `ReadPageGuard` 的移动构造函数。
 *
 * ### 实现说明
 *
 * 如果你不熟悉 move semantics，请先补一下相关知识。网上有很多不错的资料
 * （例如文章、微软教程、YouTube 视频）对这个主题有比较深入的解释。
 *
 * 一定要让另一个 guard 失效；否则你很可能会遇到 double free 问题！对两个对象来说，
 * 你至少都需要更新 5 个字段。
 *
 * TODO(P1): 补充实现。
 *
 * @param that 另一个页面 guard。
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief `ReadPageGuard` 的移动赋值运算符。
 *
 * ### 实现说明
 *
 * 如果你不熟悉 move semantics，请先补一下相关知识。网上有很多不错的资料
 * （例如文章、微软教程、YouTube 视频）对这个主题有比较深入的解释。
 *
 * 一定要让另一个 guard 失效；否则你很可能会遇到 double free 问题！对两个对象来说，
 * 你至少都需要更新 5 个字段。并且对于当前对象，还要先确保它已经正确释放了自己
 * 可能持有的资源。
 *
 * TODO(P1): 补充实现。
 *
 * @param that 另一个页面 guard。
 * @return ReadPageGuard& 新的有效 `ReadPageGuard`。
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;
  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief 获取当前 guard 正在保护的页面 ID。
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief 获取当前 guard 正在保护的页面数据的 `const` 指针。
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief 返回该页面是否为脏页（已修改但尚未刷回磁盘）。
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief 将当前页面的数据安全地刷到磁盘。
 *
 * TODO(P1): 补充实现。
 */
void ReadPageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest req{true, frame_->GetDataMut(), page_id_, std::move(promise)};
  std::vector<DiskRequest> reqs;
  reqs.push_back(std::move(req));

  disk_scheduler_->Schedule(reqs);
  future.get();

  frame_->is_dirty_ = false;
}

/**
 * @brief 手动释放一个有效 `ReadPageGuard` 持有的资源。如果该 guard 无效，则什么都不做。
 *
 * ### 实现说明
 *
 * 一定要避免 double free！另外，请**非常非常**仔细地想清楚你到底拥有了哪些资源，
 * 以及你应该按什么顺序释放这些资源。如果释放顺序错了，后面的 Gradescope 测试
 * 大概率会挂掉。在某些特定场景下，你可能还需要获取 buffer pool manager 的 latch。
 *
 * TODO(P1): 补充实现。
 */
void ReadPageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  frame_->rwlatch_.unlock_shared();
  {
    std::scoped_lock lock(*bpm_latch_);
    auto new_pin_count = frame_->pin_count_.fetch_sub(1) - 1;
    if (new_pin_count == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  is_valid_ = false;
  page_id_ = INVALID_PAGE_ID;
  frame_.reset();
  replacer_.reset();
  bpm_latch_.reset();
  disk_scheduler_.reset();
}

/** @brief `ReadPageGuard` 的析构函数。它只是简单调用 `Drop()`。 */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief 唯一一个能创建有效 `WritePageGuard` 的 RAII 构造函数。
 *
 * 注意，只有 buffer pool manager 可以调用这个构造函数。
 *
 * TODO(P1): 补充实现。
 *
 * @param page_id 我们要写入的页面 ID。
 * @param frame 指向保存该页面的 frame 的共享指针。
 * @param replacer 指向 buffer pool manager 的 replacer 的共享指针。
 * @param bpm_latch 指向 buffer pool manager 的全局闩锁的共享指针。
 * @param disk_scheduler 指向 buffer pool manager 的磁盘调度器的共享指针。
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<ArcReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  frame_->rwlatch_.lock();
  frame_->is_dirty_ = true;
  is_valid_ = true;
}

/**
 * @brief `WritePageGuard` 的移动构造函数。
 *
 * ### 实现说明
 *
 * 如果你不熟悉 move semantics，请先补一下相关知识。网上有很多不错的资料
 * （例如文章、微软教程、YouTube 视频）对这个主题有比较深入的解释。
 *
 * 一定要让另一个 guard 失效；否则你很可能会遇到 double free 问题！对两个对象来说，
 * 你至少都需要更新 5 个字段。
 *
 * TODO(P1): 补充实现。
 *
 * @param that 另一个页面 guard。
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
}

/**
 * @brief `WritePageGuard` 的移动赋值运算符。
 *
 * ### 实现说明
 *
 * 如果你不熟悉 move semantics，请先补一下相关知识。网上有很多不错的资料
 * （例如文章、微软教程、YouTube 视频）对这个主题有比较深入的解释。
 *
 * 一定要让另一个 guard 失效；否则你很可能会遇到 double free 问题！对两个对象来说，
 * 你至少都需要更新 5 个字段。并且对于当前对象，还要先确保它已经正确释放了自己
 * 可能持有的资源。
 *
 * TODO(P1): 补充实现。
 *
 * @param that 另一个页面 guard。
 * @return WritePageGuard& 新的有效 `WritePageGuard`。
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this == &that) {
    return *this;
  }
  Drop();
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;

  that.page_id_ = INVALID_PAGE_ID;
  that.is_valid_ = false;
  return *this;
}

/**
 * @brief 获取当前 guard 正在保护的页面 ID。
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief 获取当前 guard 正在保护的页面数据的 `const` 指针。
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief 获取当前 guard 正在保护的页面数据的可变指针。
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief 返回该页面是否为脏页（已修改但尚未刷回磁盘）。
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief 将当前页面的数据安全地刷到磁盘。
 *
 * TODO(P1): 补充实现。
 */
void WritePageGuard::Flush() {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");

  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();

  DiskRequest req{true, frame_->GetDataMut(), page_id_, std::move(promise)};
  std::vector<DiskRequest> requests;
  requests.push_back(std::move(req));

  disk_scheduler_->Schedule(requests);
  future.get();

  frame_->is_dirty_ = false;
}

/**
 * @brief 手动释放一个有效 `WritePageGuard` 持有的资源。如果该 guard 无效，则什么都不做。
 *
 * ### 实现说明
 *
 * 一定要避免 double free！另外，请**非常非常**仔细地想清楚你到底拥有了哪些资源，
 * 以及你应该按什么顺序释放这些资源。如果释放顺序错了，后面的 Gradescope 测试
 * 大概率会挂掉。在某些特定场景下，你可能还需要获取 buffer pool manager 的 latch。
 *
 * TODO(P1): 补充实现。
 */
void WritePageGuard::Drop() {
  if (!is_valid_) {
    return;
  }

  frame_->rwlatch_.unlock();
  {
    std::scoped_lock lock(*bpm_latch_);
    auto new_pin_count = frame_->pin_count_.fetch_sub(1) - 1;
    if (new_pin_count == 0) {
      replacer_->SetEvictable(frame_->frame_id_, true);
    }
  }
  is_valid_ = false;
  page_id_ = INVALID_PAGE_ID;
  frame_.reset();
  replacer_.reset();
  bpm_latch_.reset();
  disk_scheduler_.reset();
}

/** @brief `WritePageGuard` 的析构函数。它只是简单调用 `Drop()`。 */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
