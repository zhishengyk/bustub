//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// b_plus_tree.h
//
// Identification: src/include/storage/index/b_plus_tree.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * b_plus_tree.h
 *
 * 这里定义了一个简单的 B+ 树数据结构：
 * internal page 负责导航查找路径，leaf
 * page
 * 负责存放实际数据。
 * (1) 仅支持唯一键
 * (2) 支持插入与删除
 * (3) 树结构可以动态扩张和收缩
 *
 * (4) 支持用于范围扫描的索引迭代器
 */
#pragma once

#include <algorithm>
#include <deque>
#include <filesystem>
#include <iostream>
#include <memory>
#include <optional>
#include <queue>
#include <shared_mutex>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/macros.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_header_page.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/page_guard.h"

namespace bustub {

struct PrintableBPlusTree;

/**
 * @brief Context 类的定义。
 *
 *
 * 提示：这个类用于帮助你跟踪当前正在访问或修改的页面。

 */
class Context {
 public:
  // 在插入或删除时，可以把 header page 的写锁保存在这里。
  // 当你想释放全部锁时，记得 drop 对应 guard，并把它设为 nullopt。
  std::optional<WritePageGuard> header_page_{std::nullopt};

  // 在这里缓存 root page id，方便判断当前页面是否为根页。
  page_id_t root_page_id_{INVALID_PAGE_ID};

  // 在这里保存正在修改的页面对应的写锁。
  std::deque<WritePageGuard> write_set_;

  // 查询时你可能会用到读锁集合，但不是必须的。
  std::deque<ReadPageGuard> read_set_;

  auto IsRootPage(page_id_t page_id) -> bool { return page_id == root_page_id_; }
};

#define BPLUSTREE_TYPE BPlusTree<KeyType, ValueType, KeyComparator, NumTombs>

// 对外提供交互式 B+ 树接口的主类。
FULL_INDEX_TEMPLATE_ARGUMENTS_DEFN
class BPlusTree {
  using InternalPage = BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>;
  using LeafPage = BPlusTreeLeafPage<KeyType, ValueType, KeyComparator, NumTombs>;

 public:
  explicit BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                     const KeyComparator &comparator, int leaf_max_size = LEAF_PAGE_SLOT_CNT,
                     int internal_max_size = INTERNAL_PAGE_SLOT_CNT);

  // 如果这棵 B+ 树中没有任何键值对，则返回 true。
  auto IsEmpty() const -> bool;

  // 向这棵 B+ 树中插入一个键值对。
  auto Insert(const KeyType &key, const ValueType &value) -> bool;

  // 从这棵 B+ 树中删除指定键及其对应的值。
  void Remove(const KeyType &key);

  // 返回给定 key 对应的 value。
  auto GetValue(const KeyType &key, std::vector<ValueType> *result) -> bool;

  // 返回根节点所在页面的 page id。
  auto GetRootPageId() -> page_id_t;

  // 索引迭代器相关接口。
  auto Begin() -> INDEXITERATOR_TYPE;

  auto End() -> INDEXITERATOR_TYPE;

  auto Begin(const KeyType &key) -> INDEXITERATOR_TYPE;

  void Print(BufferPoolManager *bpm);

  void Draw(BufferPoolManager *bpm, const std::filesystem::path &outf);

  auto DrawBPlusTree() -> std::string;

  // 从文件中读取数据，并逐条插入。
  void InsertFromFile(const std::filesystem::path &file_name);

  // 从文件中读取数据，并逐条删除。
  void RemoveFromFile(const std::filesystem::path &file_name);

  void BatchOpsFromFile(const std::filesystem::path &file_name);

  // 不要把这个类型改成 BufferPoolManager！
  std::shared_ptr<TracedBufferPoolManager> bpm_;

 private:
  void ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out);

  void PrintTree(page_id_t page_id, const BPlusTreePage *page);

  auto ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree;

  // 成员变量
  std::string index_name_;
  KeyComparator comparator_;
  std::vector<std::string> log;  // NOLINT
  int leaf_max_size_;
  int internal_max_size_;
  page_id_t header_page_id_;
};

/**
 * @brief 仅用于测试。PrintableBPlusTree 是一个可打印的 B+ 树结构。
 * 我们会先把 B+
 * 树转换成 PrintableBPlusTree，再将其输出。
 */
struct PrintableBPlusTree {
  int size_;
  std::string keys_;
  std::vector<PrintableBPlusTree> children_;

  /**
   * @brief 对可打印 B+ 树进行 BFS 遍历，并将结果输出到 out_buf。
   *
   * @param out_buf
   * 输出流

   */
  void Print(std::ostream &out_buf) {
    std::vector<PrintableBPlusTree *> que = {this};
    while (!que.empty()) {
      std::vector<PrintableBPlusTree *> new_que;

      for (auto &t : que) {
        int padding = (t->size_ - t->keys_.size()) / 2;
        out_buf << std::string(padding, ' ');
        out_buf << t->keys_;
        out_buf << std::string(padding, ' ');

        for (auto &c : t->children_) {
          new_que.push_back(&c);
        }
      }
      out_buf << "\n";
      que = new_que;
    }
  }
};

}  // namespace bustub
