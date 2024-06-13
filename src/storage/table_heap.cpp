#include "storage/table_heap.h"

/**
 * @brief Insert a tuple into the table heap with given row.
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) {
  page_id_t cur_page_id = first_page_id_;
  // find the suitable page to insert the tuple
  while (cur_page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
    // cannot find page
    if (page == nullptr) {
      LOG(ERROR) << "The buffer pool is full and no space to replace" << std::endl;
      return false;
    }
    page->WLatch();
    // insert tuple successfully
    if (page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page_id, true);
      return true;
    }
    // find next page
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    page_id_t nexr_page_id = page->GetNextPageId();
    if (nexr_page_id == INVALID_PAGE_ID) {
      break;
    }
    cur_page_id = nexr_page_id;
  }
  // Create a new page and insert the tuple
  page_id_t new_page_id = INVALID_PAGE_ID;
  auto new_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->NewPage(new_page_id));
  // Cannot create a new page
  if (new_page == nullptr) {
    return false;
  }
  // if the heap is not empty, then the new page turns to be the last page
  if (first_page_id_ == INVALID_PAGE_ID) {
    first_page_id_ = new_page_id;
  } else {
    auto last_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
    last_page->WLatch();
    last_page->SetNextPageId(new_page_id);
    last_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_id, true);
  }
  new_page->WLatch();
  new_page->Init(new_page_id, cur_page_id, log_manager_, txn);
  if (new_page->InsertTuple(row, schema_, txn, lock_manager_, log_manager_)) {
    new_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    return true;
  }
  new_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(new_page_id, true);
  return false;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * @brief Update a tuple in the table heap with given row and rid.
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  page_id_t page_id = rid.GetPageId();
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  // cannot find page
  if (page == nullptr) {
    LOG(ERROR) << "The buffer pool is full and no space to replace" << std::endl;
    return false;
  }
  page->WLatch();
  Row *old_row = new Row(rid);
  // if update successfully
  if (page->UpdateTuple(row, old_row, schema_, txn, lock_manager_, log_manager_)) {
    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page_id, true);
    delete old_row;
    return true;
  }
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page_id, true);
  delete old_row;
  return false;
}

/**
 * @brief Apply delete operation to the table heap with given rid.
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  // Step1: Find the page which contains the tuple.
  // Step2: Delete the tuple from the page.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  page->WLatch();
  page->ApplyDelete(rid, txn, nullptr);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * @brief Get a tuple from the table heap with given row.
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  page_id_t page_id = row->GetRowId().GetPageId();
  TablePage *page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));
  // cannot find page
  if (page == nullptr) {
    LOG(ERROR) << "The buffer pool is full and no space to replace" << std::endl;
    return false;
  }
  page->RLatch();
  if (page->GetTuple(row, schema_, txn, lock_manager_)) {
    page->RUnlatch();
    if (buffer_pool_manager_->UnpinPage(page_id, false)) {
      return true;
    }
    LOG(WARNING) << "Unknown mistake" << std::endl;
    return false;
  }
  page->RUnlatch();
  LOG(WARNING) << "Get tuple failed" << std::endl;
  return false;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  
    // Delete table_heap recursively.
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID) {
      DeleteTable(temp_table_page->GetNextPageId());
    }
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * @brief Return an iterator to the beginning of the table heap.
 */
TableIterator TableHeap::Begin(Txn *txn) {
  page_id_t cur_page_id = first_page_id_;
  RowId first_rid;
  while (cur_page_id != INVALID_PAGE_ID) {
    auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(cur_page_id));
    ASSERT(page != nullptr, "Fetch page failed.");
    page->RLatch();
    if (page->GetFirstTupleRid(&first_rid)) {
      // if the first tuple is found, then return the iterator
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(cur_page_id, false);
      Row *row = new Row(first_rid);
      GetTuple(row, txn);
      return TableIterator(this, row->GetRowId(), txn);
    }
    // move to the next page
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(cur_page_id, false);
    cur_page_id = page->GetNextPageId();
  }
  // if the table heap is empty, then return the end iterator
  return End();
}

/**
 * @brief Return an iterator to the end of the table heap.
 */
TableIterator TableHeap::End() {
  return TableIterator(nullptr, RowId(INVALID_PAGE_ID, 0), nullptr);
}
