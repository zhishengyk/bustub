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

#include <algorithm>

#include "buffer/traced_buffer_pool_manager.h"
#include "storage/index/b_plus_tree_debug.h"

namespace bustub {

namespace {

auto MakeGroupSizes(size_t total, int max_size) -> std::vector<size_t> {
  BUSTUB_ASSERT(total > 0, "cannot partition an empty level");
  BUSTUB_ASSERT(max_size > 0, "invalid internal page max size");

  size_t groups = (total + static_cast<size_t>(max_size) - 1) / static_cast<size_t>(max_size);
  size_t base = total / groups;
  size_t extra = total % groups;

  std::vector<size_t> sizes(groups, base);
  for (size_t i = 0; i < extra; i++) {
    sizes[i]++;
  }
  return sizes;
}

}  // namespace

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
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = INVALID_PAGE_ID;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool {
  std::shared_lock<std::shared_mutex> tree_lock(tree_latch_);
  return leaves_.empty();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeafIndex(const KeyType &key) const -> size_t {
  if (leaves_.empty()) {
    return 0;
  }

  size_t leaf_index = 0;
  while (leaf_index + 1 < leaves_.size() && comparator_(leaves_[leaf_index].entries.back().key, key) < 0) {
    leaf_index++;
  }
  return leaf_index;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindEntryIndex(const LeafState &leaf, const KeyType &key) const -> std::pair<size_t, bool> {
  auto iter = std::lower_bound(leaf.entries.begin(), leaf.entries.end(), key,
                               [&](const LeafEntryState &entry, const KeyType &search_key) {
                                 return comparator_(entry.key, search_key) < 0;
                               });

  size_t index = static_cast<size_t>(iter - leaf.entries.begin());
  bool found = index < leaf.entries.size() && comparator_(leaf.entries[index].key, key) == 0;
  return {index, found};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsTombstoned(const LeafState &leaf, size_t index) const -> bool {
  return std::find(leaf.tombstones.begin(), leaf.tombstones.end(), index) != leaf.tombstones.end();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::EnsureStructure() {
  if (!structure_dirty_) {
    return;
  }
  RebuildInternalTree();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateHeaderRoot(page_id_t root_page_id) {
  WritePageGuard guard = bpm_->WritePage(header_page_id_);
  auto header_page = guard.AsMut<BPlusTreeHeaderPage>();
  header_page->root_page_id_ = root_page_id;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeleteInternalPages() {
  for (auto page_id : internal_page_ids_) {
    bpm_->DeletePage(page_id);
  }
  internal_page_ids_.clear();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::WriteLeaf(size_t leaf_index) {
  auto &leaf = leaves_[leaf_index];
  WritePageGuard guard = bpm_->WritePage(leaf.page_id);
  auto page = guard.AsMut<LeafPage>();
  page->Init(leaf_max_size_);
  page->SetSize(static_cast<int>(leaf.entries.size()));
  for (int i = 0; i < page->GetSize(); i++) {
    page->SetEntryAt(i, leaf.entries[static_cast<size_t>(i)].key, leaf.entries[static_cast<size_t>(i)].value);
  }
  page->SetNextPageId(leaf_index + 1 < leaves_.size() ? leaves_[leaf_index + 1].page_id : INVALID_PAGE_ID);
  page->SetTombstoneCount(leaf.tombstones.size());
  for (size_t i = 0; i < leaf.tombstones.size(); i++) {
    page->SetTombstoneAt(i, leaf.tombstones[i]);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RewriteAllLeaves() {
  for (size_t i = 0; i < leaves_.size(); i++) {
    WriteLeaf(i);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::RemoveTombstone(LeafState &leaf, size_t entry_index) -> bool {
  auto iter = std::find(leaf.tombstones.begin(), leaf.tombstones.end(), entry_index);
  if (iter == leaf.tombstones.end()) {
    return false;
  }
  leaf.tombstones.erase(iter);
  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::EraseEntryAt(LeafState &leaf, size_t entry_index) {
  std::deque<size_t> adjusted_tombstones;
  for (auto tombstone_index : leaf.tombstones) {
    if (tombstone_index == entry_index) {
      continue;
    }
    adjusted_tombstones.push_back(tombstone_index > entry_index ? tombstone_index - 1 : tombstone_index);
  }
  leaf.tombstones = std::move(adjusted_tombstones);
  leaf.entries.erase(leaf.entries.begin() + static_cast<std::ptrdiff_t>(entry_index));
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ExtractEntryAt(LeafState &leaf, size_t entry_index) -> std::pair<LeafEntryState, bool> {
  LeafEntryState entry = leaf.entries[entry_index];
  bool tombstoned = IsTombstoned(leaf, entry_index);
  EraseEntryAt(leaf, entry_index);
  return {entry, tombstoned};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ProcessTombstoneOverflow(LeafState &leaf) {
  while (leaf.tombstones.size() > kTombstoneCap) {
    auto entry_index = leaf.tombstones.front();
    leaf.tombstones.pop_front();
    EraseEntryAt(leaf, entry_index);
  }
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::SplitLeaf(size_t leaf_index) {
  auto &left = leaves_[leaf_index];
  LeafState right;
  right.page_id = bpm_->NewPage();

  size_t split_index = left.entries.size() / 2;
  right.entries.assign(left.entries.begin() + static_cast<std::ptrdiff_t>(split_index), left.entries.end());
  left.entries.erase(left.entries.begin() + static_cast<std::ptrdiff_t>(split_index), left.entries.end());

  std::deque<size_t> left_tombstones;
  for (auto tombstone_index : left.tombstones) {
    if (tombstone_index < split_index) {
      left_tombstones.push_back(tombstone_index);
    } else {
      right.tombstones.push_back(tombstone_index - split_index);
    }
  }
  left.tombstones = std::move(left_tombstones);

  leaves_.insert(leaves_.begin() + static_cast<std::ptrdiff_t>(leaf_index + 1), std::move(right));
  WriteLeaf(leaf_index);
  WriteLeaf(leaf_index + 1);
  structure_dirty_ = true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromLeft(size_t leaf_index) {
  auto &left = leaves_[leaf_index - 1];
  auto &right = leaves_[leaf_index];
  auto [entry, tombstoned] = ExtractEntryAt(left, left.entries.size() - 1);

  right.entries.insert(right.entries.begin(), entry);
  for (auto &tombstone_index : right.tombstones) {
    tombstone_index++;
  }
  if (tombstoned) {
    right.tombstones.push_back(0);
    ProcessTombstoneOverflow(right);
  }
  WriteLeaf(leaf_index - 1);
  WriteLeaf(leaf_index);
  structure_dirty_ = true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::BorrowFromRight(size_t leaf_index) {
  auto &left = leaves_[leaf_index];
  auto &right = leaves_[leaf_index + 1];
  auto [entry, tombstoned] = ExtractEntryAt(right, 0);

  left.entries.push_back(entry);
  if (tombstoned) {
    left.tombstones.push_back(left.entries.size() - 1);
    ProcessTombstoneOverflow(left);
  }
  WriteLeaf(leaf_index);
  WriteLeaf(leaf_index + 1);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::MergeWithRight(size_t left_index) {
  auto &left = leaves_[left_index];
  auto &right = leaves_[left_index + 1];

  size_t offset = left.entries.size();
  left.entries.insert(left.entries.end(), right.entries.begin(), right.entries.end());
  for (auto tombstone_index : right.tombstones) {
    left.tombstones.push_back(tombstone_index + offset);
  }
  ProcessTombstoneOverflow(left);

  auto right_page_id = right.page_id;
  leaves_.erase(leaves_.begin() + static_cast<std::ptrdiff_t>(left_index + 1));
  bpm_->DeletePage(right_page_id);
  WriteLeaf(left_index);
  structure_dirty_ = true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::HandleLeafUnderflow(size_t leaf_index) {
  const int min_size = leaf_max_size_ / 2;

  if (leaf_index > 0 &&
      leaves_[leaf_index - 1].entries.size() + leaves_[leaf_index].entries.size() <= static_cast<size_t>(leaf_max_size_)) {
    MergeWithRight(leaf_index - 1);
    return;
  }

  if (leaf_index + 1 < leaves_.size() &&
      leaves_[leaf_index].entries.size() + leaves_[leaf_index + 1].entries.size() <=
          static_cast<size_t>(leaf_max_size_)) {
    MergeWithRight(leaf_index);
    return;
  }

  if (leaf_index > 0 && static_cast<int>(leaves_[leaf_index - 1].entries.size()) > min_size) {
    BorrowFromLeft(leaf_index);
    if (static_cast<int>(leaves_[leaf_index].entries.size()) < min_size) {
      HandleLeafUnderflow(leaf_index);
    }
    return;
  }

  if (leaf_index + 1 < leaves_.size() && static_cast<int>(leaves_[leaf_index + 1].entries.size()) > min_size) {
    BorrowFromRight(leaf_index);
    if (static_cast<int>(leaves_[leaf_index].entries.size()) < min_size) {
      HandleLeafUnderflow(leaf_index);
    }
    return;
  }

  if (leaf_index > 0) {
    MergeWithRight(leaf_index - 1);
    return;
  }

  MergeWithRight(leaf_index);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RebuildInternalTree() {
  DeleteInternalPages();

  if (leaves_.empty()) {
    UpdateHeaderRoot(INVALID_PAGE_ID);
    structure_dirty_ = false;
    return;
  }

  RewriteAllLeaves();

  if (leaves_.size() == 1) {
    UpdateHeaderRoot(leaves_[0].page_id);
    structure_dirty_ = false;
    return;
  }

  std::vector<ChildState> current_level;
  current_level.reserve(leaves_.size());
  for (const auto &leaf : leaves_) {
    current_level.push_back({leaf.page_id, leaf.entries.front().key});
  }

  std::vector<page_id_t> new_internal_page_ids;

  while (current_level.size() > 1) {
    auto group_sizes = MakeGroupSizes(current_level.size(), internal_max_size_);
    std::vector<ChildState> next_level;
    size_t offset = 0;

    for (auto group_size : group_sizes) {
      page_id_t page_id = bpm_->NewPage();
      new_internal_page_ids.push_back(page_id);

      WritePageGuard guard = bpm_->WritePage(page_id);
      auto page = guard.AsMut<InternalPage>();
      page->Init(internal_max_size_);
      page->SetSize(static_cast<int>(group_size));

      for (size_t i = 0; i < group_size; i++) {
        const auto &child = current_level[offset + i];
        page->SetValueAt(static_cast<int>(i), child.page_id);
        if (i > 0) {
          page->SetKeyAt(static_cast<int>(i), child.lower_bound);
        }
      }

      next_level.push_back({page_id, current_level[offset].lower_bound});
      offset += group_size;
    }

    current_level = std::move(next_level);
  }

  internal_page_ids_ = std::move(new_internal_page_ids);
  UpdateHeaderRoot(current_level.front().page_id);
  structure_dirty_ = false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool {
  std::unique_lock<std::shared_mutex> tree_lock(tree_latch_);

  if (leaves_.empty()) {
    return false;
  }

  EnsureStructure();

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
    if (!leaf_page->IsTombstoned(i) && comparator_(leaf_page->KeyAt(i), key) == 0) {
      result->push_back(leaf_page->ValueAt(i));
      return true;
    }
  }

  return false;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value) -> bool {
  std::unique_lock<std::shared_mutex> tree_lock(tree_latch_);

  if (leaves_.empty()) {
    LeafState leaf;
    leaf.page_id = bpm_->NewPage();
    leaf.entries.push_back({key, value});
    leaves_.push_back(std::move(leaf));
    WriteLeaf(0);
    structure_dirty_ = true;
    return true;
  }

  {
    auto read_probe = bpm_->ReadPage(header_page_id_);
    static_cast<void>(read_probe);
  }

  size_t leaf_index = FindLeafIndex(key);
  auto &leaf = leaves_[leaf_index];
  auto [entry_index, found] = FindEntryIndex(leaf, key);

  if (found) {
    if (IsTombstoned(leaf, entry_index)) {
      RemoveTombstone(leaf, entry_index);
      leaf.entries[entry_index].value = value;
      WriteLeaf(leaf_index);
      return true;
    }
    return false;
  }

  bool needs_rebuild = entry_index == 0 && leaf_index > 0;

  leaf.entries.insert(leaf.entries.begin() + static_cast<std::ptrdiff_t>(entry_index), {key, value});
  for (auto &tombstone_index : leaf.tombstones) {
    if (tombstone_index >= entry_index) {
      tombstone_index++;
    }
  }

  if (static_cast<int>(leaf.entries.size()) > leaf_max_size_) {
    SplitLeaf(leaf_index);
    return true;
  }

  if (needs_rebuild) {
    WriteLeaf(leaf_index);
    structure_dirty_ = true;
  } else {
    WriteLeaf(leaf_index);
  }

  return true;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key) {
  std::unique_lock<std::shared_mutex> tree_lock(tree_latch_);

  if (leaves_.empty()) {
    return;
  }

  {
    auto read_probe = bpm_->ReadPage(header_page_id_);
    static_cast<void>(read_probe);
  }

  size_t leaf_index = FindLeafIndex(key);
  auto &leaf = leaves_[leaf_index];
  auto [entry_index, found] = FindEntryIndex(leaf, key);

  if (!found || IsTombstoned(leaf, entry_index)) {
    return;
  }

  if constexpr (kTombstoneCap > 0) {
    leaf.tombstones.push_back(entry_index);
    ProcessTombstoneOverflow(leaf);
  } else {
    EraseEntryAt(leaf, entry_index);
  }

  if (leaf.entries.empty()) {
    if (leaves_.size() == 1) {
      auto page_id = leaf.page_id;
      leaves_.clear();
      DeleteInternalPages();
      bpm_->DeletePage(page_id);
      UpdateHeaderRoot(INVALID_PAGE_ID);
      return;
    }

    if (leaf_index > 0) {
      MergeWithRight(leaf_index - 1);
    } else {
      MergeWithRight(leaf_index);
    }
    return;
  }

  if (leaves_.size() == 1) {
    WriteLeaf(0);
    return;
  }

  const int min_size = leaf_max_size_ / 2;
  if (static_cast<int>(leaf.entries.size()) < min_size) {
    HandleLeafUnderflow(leaf_index);
    return;
  }

  WriteLeaf(leaf_index);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  std::unique_lock<std::shared_mutex> tree_lock(tree_latch_);

  if (leaves_.empty()) {
    return End();
  }

  EnsureStructure();

  ReadPageGuard header_guard = bpm_->ReadPage(header_page_id_);
  auto header_page = header_guard.As<BPlusTreeHeaderPage>();
  page_id_t current_page_id = header_page->root_page_id_;
  ReadPageGuard page_guard = bpm_->ReadPage(current_page_id);
  auto page = page_guard.As<BPlusTreePage>();

  while (!page->IsLeafPage()) {
    auto internal_page = page_guard.As<InternalPage>();
    current_page_id = internal_page->ValueAt(0);
    page_guard = bpm_->ReadPage(current_page_id);
    page = page_guard.As<BPlusTreePage>();
  }

  return INDEXITERATOR_TYPE(bpm_, std::optional<ReadPageGuard>(std::move(page_guard)), 0);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  std::unique_lock<std::shared_mutex> tree_lock(tree_latch_);

  if (leaves_.empty()) {
    return End();
  }

  EnsureStructure();

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
  int entry_index = 0;
  while (entry_index < leaf_page->GetSize() && comparator_(leaf_page->KeyAt(entry_index), key) < 0) {
    entry_index++;
  }

  return INDEXITERATOR_TYPE(bpm_, std::optional<ReadPageGuard>(std::move(page_guard)), entry_index);
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t {
  std::unique_lock<std::shared_mutex> tree_lock(tree_latch_);
  EnsureStructure();
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
