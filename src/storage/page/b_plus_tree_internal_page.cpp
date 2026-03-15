//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree_internal_page.cpp
//
// Identification: src/storage/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * 辅助函数与工具函数
 *****************************************************************************/

/**
 * @brief 新建 Internal Page 后的初始化函数。
 *
 * 该函数会为新创建的页面写入必要的页头信息，
 * 包括设置页面类型、当前大小以及最大页面大小。
 *
 * @param max_size 页面的最大大小
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetMaxSize(max_size);
}

/**
 * @brief 根据给定下标获取对应的键。
 *
 * @param index 要获取的键的下标。该下标必须非零。
 * @return 对应下标处的键
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  if (index <= 0 || index >= GetSize()) {
    throw Exception("Invalid index for KeyAt in Internal Page");
  }
  return key_array_[index];
}

/**
 * @brief 设置指定下标处的键。
 *
 * @param index 要设置的键的下标。该下标必须非零。
 * @param key 新的键值
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  if (index <= 0 || index >= GetSize()) {
    throw Exception("Invalid index for SetKeyAt in Internal Page");
  }
  key_array_[index] = key;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const -> int {
  for (int i = 0; i < GetSize(); i++) {
    if (page_id_array_[i] == value) {
      return i;
    }
  }
  throw Exception("Value not found in Internal Page");
}

/**
 * @brief 根据给定下标获取对应的子页面指针。
 *
 * @param index 要获取的值的下标
 * @return 对应下标处的值
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  if (index < 0 || index >= GetSize()) {
    throw Exception("Invalid index for ValueAt in Internal Page");
  }
  return page_id_array_[index];
}

// Internal Node 的 ValueType 应为 page_id_t。
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
