//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_leaf_page.cpp
//
// Identification: src/storage/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * 辅助函数与工具函数
 *****************************************************************************/

/**
 * @brief 新建 Leaf Page 后的初始化函数
 *
 * 从 Buffer Pool 创建出一个新的 Leaf Page 后，
 * 必须调用该初始化函数来设置默认值，
 * 包括设置页面类型、将当前大小设为 0、设置 next page id，以及设置最大大小。
 *
 * @param max_size Leaf Node 的最大大小
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
  SetMaxSize(max_size);
  num_tombstones_ = 0;
}

/**
 * @brief 获取当前页面中的 tombstone 对应的键。
 * @return 当前页面中最近 `NumTombs` 个待删除键，按时间顺序返回（最旧的在前）。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstones() const -> std::vector<KeyType> {
  std::vector<KeyType> tombs;
  for (size_t i = 0; i < num_tombstones_; i++) {
    size_t tomb_index = tombstones_[i];
    tombs.push_back(key_array_[tomb_index]);
  }
  return tombs;
}

/**
 * 用于读取和修改 next page id 的辅助函数。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * 根据给定下标（即数组偏移）返回对应的键。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index < 0 || index >= GetSize()) {
    throw Exception("Invalid index for KeyAt in Leaf Page");
  }
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    throw Exception("Invalid index for ValueAt in Leaf Page");
  }
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAtRef(int index) const -> const KeyType & {
  if (index < 0 || index >= GetSize()) {
    throw Exception("Invalid index for KeyAtRef in Leaf Page");
  }
  return key_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ValueAtRef(int index) const -> const ValueType & {
  if (index < 0 || index >= GetSize()) {
    throw Exception("Invalid index for ValueAtRef in Leaf Page");
  }
  return rid_array_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetTombstoneCount() const -> size_t { return num_tombstones_; }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::TombstoneAt(size_t index) const -> size_t {
  if (index >= num_tombstones_) {
    throw Exception("Invalid index for TombstoneAt in Leaf Page");
  }
  return tombstones_[index];
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::IsTombstoned(int index) const -> bool {
  if (index < 0 || index >= GetSize()) {
    throw Exception("Invalid index for IsTombstoned in Leaf Page");
  }
  for (size_t i = 0; i < num_tombstones_; i++) {
    if (static_cast<int>(tombstones_[i]) == index) {
      return true;
    }
  }
  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetTombstoneCount(size_t count) {
  if (count > LEAF_PAGE_TOMB_CNT) {
    throw Exception("Invalid tombstone count for Leaf Page");
  }
  num_tombstones_ = count;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetTombstoneAt(size_t index, size_t value) {
  if (index >= LEAF_PAGE_TOMB_CNT) {
    throw Exception("Invalid index for SetTombstoneAt in Leaf Page");
  }
  tombstones_[index] = value;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetEntryAt(int index, const KeyType &key, const ValueType &value) {
  if (index < 0 || index >= GetSize()) {
    throw Exception("Invalid index for SetEntryAt in Leaf Page");
  }
  key_array_[index] = key;
  rid_array_[index] = value;
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
