//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_iterator.cpp
//
// Identification: src/storage/index/index_iterator.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/**
 * @note you can change the destructor/constructor method here
 * set your own input parameters
 */
FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(std::shared_ptr<TracedBufferPoolManager> bpm, std::optional<ReadPageGuard> guard,
                                  int index)
    : bpm_(std::move(bpm)), guard_(std::move(guard)), index_(index) {
  AdvanceToValid();
}

FULL_INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return !guard_.has_value(); }

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> std::pair<const KeyType &, const ValueType &> {
  BUSTUB_ASSERT(guard_.has_value(), "cannot dereference end iterator");
  auto leaf = guard_->template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
  BUSTUB_ASSERT(index_ >= 0 && index_ < leaf->GetSize(), "iterator index out of range");
  return {leaf->KeyAtRef(index_), leaf->ValueAtRef(index_)};
}

FULL_INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(guard_.has_value(), "cannot increment end iterator");
  index_++;
  AdvanceToValid();
  return *this;
}

FULL_INDEX_TEMPLATE_ARGUMENTS
void INDEXITERATOR_TYPE::AdvanceToValid() {
  while (guard_.has_value()) {
    auto leaf = guard_->template As<B_PLUS_TREE_LEAF_PAGE_TYPE>();
    while (index_ < leaf->GetSize() && leaf->IsTombstoned(index_)) {
      index_++;
    }
    if (index_ < leaf->GetSize()) {
      return;
    }

    auto next_page_id = leaf->GetNextPageId();
    if (next_page_id == INVALID_PAGE_ID) {
      guard_ = std::nullopt;
      index_ = 0;
      return;
    }

    guard_ = bpm_->ReadPage(next_page_id);
    index_ = 0;
  }
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 3>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 2>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, 1>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>, -1>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
