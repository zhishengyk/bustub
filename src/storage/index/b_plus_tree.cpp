//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.cpp
//
// Identification: src/storage/index/b_plus_tree.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/index/b_plus_tree.h"
#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

FULL_INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : bpm_(std::make_shared<TracedBufferPoolManager>(buffer_pool_manager)),
      index_name_(std::move(name)),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

/**
 * @brief 用于判断当前 B+ 树是否为空的辅助函数。
 * @return 如果这棵 B+ 树中没有任何键值对，则返回 true。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * 查找
 *****************************************************************************/
/**
 * @brief 返回与输入 key 对应的唯一 value。
 *
 * 这个方法用于点查询。
 *
 * @param key 输入键
 * @param[out] result 如果 key 存在，则把对应的唯一 value 放进这个结果向量
 * @return 返回 true 表示 key 存在
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  if (IsEmpty()) {
    return false;
  }

  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t current_page_id = header_page->root_page_id_;

  ReadPageGuard page_guard = bpm_->ReadPage(current_page_id);
  auto page = page_guard.As<BPlusTreePage>();

  while (!page->IsLeafPage()) {
    auto internal_page = page_guard.As<InternalPage>();

    int child_idx = 0;
    while (child_idx + 1 < internal_page->GetSize() && comparator_(key, internal_page->KeyAt(child_idx + 1)) >= 0) {
      child_idx++;
    }

    current_page_id = internal_page->ValueAt(child_idx);
    page_guard = bpm_->ReadPage(current_page_id);
    page = page_guard.As<BPlusTreePage>();
  }

  auto leaf_page = page_guard.As<LeafPage>();
  for (int i = 0; i < leaf_page->GetSize(); i++) {
    if (comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      return true;
    }
  }

  return false;
}

/*****************************************************************************
 * 插入
 *****************************************************************************/
/**
 * @brief 向 B+ 树中插入一个固定的键值对。
 *
 * 如果当前树为空，则创建新树、更新 root page id 并插入条目；
 * 否则，把条目插入到对应的 leaf page 中。
 *
 * @param key 要插入的键
 * @param value 与该键关联的值
 * @return 由于这里只支持唯一键，如果插入重复键则返回 false；否则返回 true。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  UNIMPLEMENTED("TODO(P2): Add implementation.");
  // Context 不是必须使用，但通常会有帮助。
  Context ctx;
}

/*****************************************************************************
 * 删除
 *****************************************************************************/
/**
 * @brief 删除与输入 key 对应的键值对。
 * 如果当前树为空，直接返回。
 * 否则，需要先找到正确的 leaf page 作为删除目标，然后从 leaf page 中删除条目。
 * 别忘了在有需要时处理重分配或合并。
 *
 * @param key 输入键
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  // Context 实例
  Context ctx;
  UNIMPLEMENTED("TODO(P2): Add implementation.");
}

/*****************************************************************************
 * 索引迭代器
 *****************************************************************************/
/**
 * @brief 无输入参数时，先找到最左侧的 leaf page，再构造索引迭代器。
 *
 * 你可以在实现 Task #3 时完成它。
 *
 * @return 索引迭代器
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief 输入参数为下界 key 时，先找到包含该 key 的 leaf page，再构造索引迭代器。
 * @return 索引迭代器
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @brief 无输入参数时，构造一个表示 leaf node 末尾位置的索引迭代器。
 * @return 索引迭代器
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { UNIMPLEMENTED("TODO(P2): Add implementation."); }

/**
 * @return 这棵树的根页面 page id
 *
 * 你可以在实现 Task #3 时顺手完成它。
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  ReadPageGuard guard = bpm_->ReadPage(header_page_id_);
  auto header_page = guard.As<BPlusTreeHeaderPage>();
  return header_page->root_page_id_;
}
template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
