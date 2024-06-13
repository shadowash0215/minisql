#include "index/b_plus_tree.h"

#include <string>

#include "glog/logging.h"
#include "index/basic_comparator.h"
#include "index/generic_key.h"
#include "page/index_roots_page.h"

#define DEFAULT_LEAF_MAX_SIZE ((PAGE_SIZE - LEAF_PAGE_HEADER_SIZE) / (KM.GetKeySize()+ sizeof(RowId)))
#define DEFAULT_INTERNAL_MAX_SIZE ((PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (KM.GetKeySize() + sizeof(page_id_t)))

/**
 * TODO: Student Implement
 */
BPlusTree::BPlusTree(index_id_t index_id, BufferPoolManager *buffer_pool_manager, const KeyManager &KM,
                     int leaf_max_size, int internal_max_size)
    : index_id_(index_id),
      buffer_pool_manager_(buffer_pool_manager),
      processor_(KM),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  // Find root_page_id from header page
  auto header_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (header_page == nullptr) {
    throw "out of memory";
  }
  if (!header_page->GetRootId(index_id_, &root_page_id_)) {
    // LOG(INFO) << "Cannot find root page id for index " << index_id_;
    root_page_id_ = INVALID_PAGE_ID;
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, false);

  if (leaf_max_size_ == UNDEFINED_SIZE) {
    leaf_max_size_ = DEFAULT_LEAF_MAX_SIZE;
  }
  if (internal_max_size_ == UNDEFINED_SIZE) {
    internal_max_size_ = DEFAULT_INTERNAL_MAX_SIZE;
  }
}

void BPlusTree::Destroy(page_id_t current_page_id) {
  if (current_page_id == INVALID_PAGE_ID) {
    current_page_id = root_page_id_;
  }
  if (current_page_id == INVALID_PAGE_ID) {
    return;
  }
  if (current_page_id = root_page_id_) {
    auto header_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
    header_page->Delete(index_id_);
  }
  auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
  if (!page->IsLeafPage()) {
    auto *internal_page = reinterpret_cast<InternalPage *>(page);
    for (int i = 0; i < internal_page->GetSize(); i++) {
      Destroy(internal_page->ValueAt(i));
    }
  }
  buffer_pool_manager_->UnpinPage(current_page_id, false);
  buffer_pool_manager_->DeletePage(current_page_id);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
bool BPlusTree::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
bool BPlusTree::GetValue(const GenericKey *key, std::vector<RowId> &result, Txn *transaction) {
  // LOG(INFO) << "GetValue";
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_));
  if (leaf_page == nullptr) {
    return false;
  }
  RowId value;
  bool is_exist = leaf_page->Lookup(key, value, processor_);
  if (is_exist) result.push_back(value);
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  return is_exist;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::Insert(GenericKey *key, const RowId &value, Txn *transaction) {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
void BPlusTree::StartNewTree(GenericKey *key, const RowId &value) {
  auto page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
  if (page == nullptr) {
    throw "out of memory";
  }
  page->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), leaf_max_size_);
  page->Insert(key, value, processor_);
  UpdateRootPageId(true);
  buffer_pool_manager_->UnpinPage(root_page_id_, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immediately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
bool BPlusTree::InsertIntoLeaf(GenericKey *key, const RowId &value, Txn *transaction) {
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_, false));
  RowId tmp;
  if (leaf_page->Lookup(key, tmp, processor_)) {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    leaf_page->Insert(key, value, processor_);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  } else {
    auto new_page = Split(leaf_page, transaction);
    GenericKey *middle_key = new_page->KeyAt(0);
    if (processor_.CompareKeys(key, middle_key) < 0) {
      leaf_page->Insert(key, value, processor_);
    } else {
      new_page->Insert(key, value, processor_);
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
    InsertIntoParent(leaf_page, middle_key, new_page, transaction);
    return true;
  }
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
BPlusTreeInternalPage *BPlusTree::Split(InternalPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto *new_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(new_page_id)->GetData());
  if (new_page == nullptr) {
    throw "out of memory";
  }
  new_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), internal_max_size_);
  node->MoveHalfTo(new_page, buffer_pool_manager_);
  return new_page;
}

BPlusTreeLeafPage *BPlusTree::Split(LeafPage *node, Txn *transaction) {
  page_id_t new_page_id;
  auto *new_page = reinterpret_cast<LeafPage *>(buffer_pool_manager_->NewPage(new_page_id)->GetData());
  if (new_page == nullptr) {
    throw "out of memory";
  }
  new_page->Init(new_page_id, node->GetParentPageId(), processor_.GetKeySize(), leaf_max_size_);
  // LOG(INFO) << "Split: node_page_id = " << node->GetPageId() << ", new_page_id = " << new_page_id;
  // auto page2 = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(2)->GetData());
  // auto page3 = reinterpret_cast<LeafPage *>(buffer_pool_manager_->FetchPage(3)->GetData());
  // LOG(INFO) << "Split: page2_address = " << page2 << ", page3_address = " << page3;
  node->MoveHalfTo(new_page);
  new_page->SetNextPageId(node->GetNextPageId());
  node->SetNextPageId(new_page_id);
  return new_page;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
void BPlusTree::InsertIntoParent(BPlusTreePage *old_node, GenericKey *key, BPlusTreePage *new_node, Txn *transaction) {
  if (old_node->IsRootPage()) {
    auto *new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->NewPage(root_page_id_)->GetData());
    if (new_root == nullptr) {
      throw "out of memory";
    }
    new_root->Init(root_page_id_, INVALID_PAGE_ID, processor_.GetKeySize(), internal_max_size_);
    new_root->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    old_node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    UpdateRootPageId();
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
  } else {
    page_id_t parent_page_id = old_node->GetParentPageId();
    auto *parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(parent_page_id)->GetData());
    ASSERT(parent_page != nullptr, "parent page is nullptr");
    if (parent_page->GetSize() < internal_max_size_) {
      parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
      new_node->SetParentPageId(parent_page_id);
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
    } else {
      auto *new_page = Split(parent_page, transaction);
      GenericKey *middle_key = new_page->KeyAt(0);
      if (processor_.CompareKeys(key, middle_key) < 0) {
        parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(parent_page_id);
      } else {
        new_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
        new_node->SetParentPageId(new_page->GetPageId());
      }
      buffer_pool_manager_->UnpinPage(parent_page_id, true);
      buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
      InsertIntoParent(parent_page, middle_key, new_page, transaction);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
void BPlusTree::Remove(const GenericKey *key, Txn *transaction) {
  if (IsEmpty()) {
    return;
  }
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_));
  ASSERT(leaf_page != nullptr, "leaf page is nullptr");
  leaf_page->RemoveAndDeleteRecord(key, processor_);
  if (leaf_page->GetSize() < leaf_page->GetMinSize()) {
    // Coalesce or redistribute
    bool need_delete = CoalesceOrRedistribute(leaf_page, transaction);
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    if (need_delete) {
      buffer_pool_manager_->DeletePage(leaf_page->GetPageId());
    }
  } else {
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  }
}

/* todo
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
template <typename N>
bool BPlusTree::CoalesceOrRedistribute(N *&node, Txn *transaction) {
  if (node->IsRootPage()) {
    return AdjustRoot(node);
  }
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  ASSERT(parent_page != nullptr, "parent page is nullptr");
  int index = parent_page->ValueIndex(node->GetPageId());
  N *neighbor_node = nullptr;
  if (index == 0) {
    neighbor_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(1))->GetData());
  } else {
    neighbor_node = reinterpret_cast<N *>(buffer_pool_manager_->FetchPage(parent_page->ValueAt(index - 1))->GetData());
  }
  ASSERT(neighbor_node != nullptr, "neighbor page is nullptr");
  if (neighbor_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
    Redistribute(neighbor_node, node, index);
    return false;
  } else {
    bool need_delete_parent = Coalesce(neighbor_node, node, parent_page, index);
    if (need_delete_parent) {
      buffer_pool_manager_->DeletePage(parent_page->GetPageId());
    }
    if (index == 0) {
      node = neighbor_node;
    }
    return true;
  }
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion happened
 */
bool BPlusTree::Coalesce(LeafPage *&neighbor_node, LeafPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  if (index == 0) {
    neighbor_node->MoveAllTo(node);
    node->SetNextPageId(neighbor_node->GetNextPageId());
  } else {
    node->MoveAllTo(neighbor_node);
    neighbor_node->SetNextPageId(node->GetNextPageId());
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  parent->Remove(index == 0 ? 1 : index);
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

bool BPlusTree::Coalesce(InternalPage *&neighbor_node, InternalPage *&node, InternalPage *&parent, int index,
                         Txn *transaction) {
  GenericKey *middle_key = parent->KeyAt(index == 0 ? 1 : index);
  if (index == 0) {
    neighbor_node->MoveAllTo(node, middle_key, buffer_pool_manager_);
  } else {
    node->MoveAllTo(neighbor_node, middle_key, buffer_pool_manager_);
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), false);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  parent->Remove(index == 0 ? 1 : index);
  if (parent->GetSize() < parent->GetMinSize()) {
    return CoalesceOrRedistribute(parent, transaction);
  }
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
void BPlusTree::Redistribute(LeafPage *neighbor_node, LeafPage *node, int index) {
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  ASSERT(parent_page != nullptr, "parent page is nullptr");
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node);
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}

void BPlusTree::Redistribute(InternalPage *neighbor_node, InternalPage *node, int index) {
  auto parent_page = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(node->GetParentPageId())->GetData());
  ASSERT(parent_page != nullptr, "parent page is nullptr");
  GenericKey *middle_key = parent_page->KeyAt(index == 0 ? 1 : index);
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, middle_key, buffer_pool_manager_);
    parent_page->SetKeyAt(1, neighbor_node->KeyAt(0));
  } else {
    neighbor_node->MoveLastToFrontOf(node, middle_key, buffer_pool_manager_);
    parent_page->SetKeyAt(index, node->KeyAt(0));
  }
  buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happened
 */
bool BPlusTree::AdjustRoot(BPlusTreePage *old_root_node) {
  int size = old_root_node->GetSize();
  if (old_root_node->IsLeafPage() && size == 0) {
    // case 2
    buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId();
    return true;
  } else if (!old_root_node->IsLeafPage() && size == 1) {
    // case 1
    auto old_root_page = reinterpret_cast<InternalPage *>(old_root_node);
    page_id_t only_child_page_id = old_root_page->RemoveAndReturnOnlyChild();
    auto new_root = reinterpret_cast<InternalPage *>(buffer_pool_manager_->FetchPage(only_child_page_id)->GetData());
    new_root->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root->GetPageId();
    buffer_pool_manager_->UnpinPage(new_root->GetPageId(), true);
    buffer_pool_manager_->UnpinPage(old_root_page->GetPageId(), false);
    buffer_pool_manager_->DeletePage(old_root_page->GetPageId());
    UpdateRootPageId();
    return true;
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the left most leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin() {
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(nullptr, root_page_id_, true));
  int page_id = leaf_page->GetPageId();
  buffer_pool_manager_->UnpinPage(page_id, false);
  return IndexIterator(page_id, buffer_pool_manager_, 0);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
IndexIterator BPlusTree::Begin(const GenericKey *key) {
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPage(key, root_page_id_));
  int page_id = leaf_page->GetPageId();
  RowId value;
  if (leaf_page->Lookup(key, value, processor_)) {
    int index = leaf_page->KeyIndex(key, processor_);
    buffer_pool_manager_->UnpinPage(page_id, false);
    return IndexIterator(page_id, buffer_pool_manager_, index);
  } else {
    buffer_pool_manager_->UnpinPage(page_id, false);
    return End();
  }
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
IndexIterator BPlusTree::End() {
  return IndexIterator(INVALID_PAGE_ID, nullptr, 0);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 * Note: the leaf page is pinned, you need to unpin it after use.
 */
Page *BPlusTree::FindLeafPage(const GenericKey *key, page_id_t page_id, bool leftMost) {
  page_id_t current_page_id = page_id;
  while (current_page_id != INVALID_PAGE_ID) {
    auto *page = reinterpret_cast<BPlusTreePage *>(buffer_pool_manager_->FetchPage(current_page_id)->GetData());
    ASSERT(page != nullptr, "page is nullptr");
    if (page->IsLeafPage()) {
      return reinterpret_cast<Page *>(page);
    } else {
      auto *internal_page = reinterpret_cast<InternalPage *>(page);
      current_page_id = leftMost ? internal_page->ValueAt(0) : internal_page->Lookup(key, processor_);
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      default value is false. When set to true,
 * insert a record <index_name, current_page_id> into header page instead of
 * updating it.
 */
void BPlusTree::UpdateRootPageId(int insert_record) {
  auto header_page = reinterpret_cast<IndexRootsPage *>(buffer_pool_manager_->FetchPage(INDEX_ROOTS_PAGE_ID)->GetData());
  if (header_page == nullptr) {
    throw "out of memory";
  }
  if (insert_record) {
    header_page->Insert(index_id_, root_page_id_);
  } else {
    header_page->Update(index_id_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(INDEX_ROOTS_PAGE_ID, true);
}

/**
 * This method is used for debug only, You don't need to modify
 */
void BPlusTree::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out, Schema *schema) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId()
        << ",Parent=" << leaf->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      Row ans;
      processor_.DeserializeToKey(leaf->KeyAt(i), ans, schema);
      out << "<TD>" << ans.GetField(0)->toString() << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId()
        << ",Parent=" << inner->GetParentPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        Row ans;
        processor_.DeserializeToKey(inner->KeyAt(i), ans, schema);
        out << ans.GetField(0)->toString();
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out, schema);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 */
void BPlusTree::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
      bpm->UnpinPage(internal->ValueAt(i), false);
    }
  }
}

bool BPlusTree::Check() {
  bool all_unpinned = buffer_pool_manager_->CheckAllUnpinned();
  if (!all_unpinned) {
    LOG(ERROR) << "problem in page unpin" << endl;
  }
  return all_unpinned;
}