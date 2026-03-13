//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include <vector>
#include "common/macros.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  
  // 启动后台工作线程
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // 向队列中放入一个 `std::nullopt`，通知后台线程退出循环
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): 补充实现
 *
 * @brief 调度一组请求，由 DiskManager 执行。
 *
 * @param requests 待调度的请求集合。
 */
void DiskScheduler::Schedule(std::vector<DiskRequest> &requests) {
    for(auto &request : requests){
      request_queue_.Put(std::move(request));
    }
}


/**
 * TODO(P1): 补充实现
 *
 * @brief 后台工作线程函数，负责处理已经调度的请求。
 *
 * 后台线程在 DiskScheduler 生命周期内需要持续处理请求，也就是说，在 `~DiskScheduler()`
 * 被调用之前，这个函数都不应该返回；而当析构发生时，你又需要确保该函数能够正常退出。
 */
void DiskScheduler::StartWorkerThread() {
   while(true){
    auto request_opt = request_queue_.Get();

    if(!request_opt.has_value()){
      return ;
    }

    auto request = std::move(request_opt.value());

    if (request.is_write_) {
      disk_manager_->WritePage(request.page_id_, request.data_);
    } else {
      disk_manager_->ReadPage(request.page_id_, request.data_);
    }

    request.callback_.set_value(true);
  }
}
 
}  // namespace bustub
