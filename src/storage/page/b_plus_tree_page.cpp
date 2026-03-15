//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_page.cpp
//
// Identification: src/storage/page/b_plus_tree_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

/*
 * 用于读取和修改页面类型的辅助函数。
 * 页面类型枚举定义在 b_plus_tree_page.h 中。
 */
auto BPlusTreePage::IsLeafPage() const -> bool { return page_type_ == IndexPageType::LEAF_PAGE; }
void BPlusTreePage::SetPageType(IndexPageType page_type) { page_type_ = page_type; }

/*
 * 用于读取和修改当前大小的辅助函数。
 * 当前大小表示页面中存储的键值对数量。
 */
auto BPlusTreePage::GetSize() const -> int { return size_; }
void BPlusTreePage::SetSize(int size) { size_ = size; }
void BPlusTreePage::ChangeSizeBy(int amount) { size_ += amount; }

/*
 * 用于读取和修改页面最大大小的辅助函数。
 * 最大大小即页面容量。
 */
auto BPlusTreePage::GetMaxSize() const -> int { return max_size_; }
void BPlusTreePage::SetMaxSize(int size) { max_size_ = size; }

/*
 * 用于计算页面最小大小的辅助函数。
 * 一般来说，最小大小为 max_size / 2，
 * 但具体是向上取整还是向下取整取决于你的实现。
 */
auto BPlusTreePage::GetMinSize() const -> int { return max_size_ / 2; }

}  // namespace bustub
