#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * @brief Construct a new Table Iterator object.
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) : ite_tableheap(table_heap), ite_txn(txn) {
  if (rid.GetPageId() == INVALID_PAGE_ID) {
    ite_row = nullptr;
  } else {
    ite_row = new Row(rid);
    ite_tableheap->GetTuple(ite_row, ite_txn);
  }
}

TableIterator::TableIterator(const TableIterator &other) : ite_tableheap(other.ite_tableheap), ite_txn(other.ite_txn) {
  if (other.ite_row != nullptr) {
    ite_row = new Row(*other.ite_row);
  } else {
    ite_row = nullptr;
  }
}

TableIterator::~TableIterator() {
  ite_tableheap = nullptr;
  ite_txn = nullptr;
  if (ite_row != nullptr) {
    delete ite_row;
    ite_row = nullptr;
  }
}

bool TableIterator::operator==(const TableIterator &itr) const {
  if (ite_tableheap != itr.ite_tableheap) {
    return false;
  }
  if (ite_txn != itr.ite_txn) {
    return false;
  }
  if (ite_row == nullptr && itr.ite_row == nullptr) {
    return true;
  }
  if (ite_row != nullptr && itr.ite_row != nullptr) {
    return ite_row->GetRowId() == itr.ite_row->GetRowId();
  }
  return false;
}

bool TableIterator::operator!=(const TableIterator &itr) const { return (!(*this == itr)); }

const Row &TableIterator::operator*() { return *ite_row; }

Row *TableIterator::operator->() { return ite_row; }

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
  if (this == &itr) {
    return *this;
  }
  ite_tableheap = itr.ite_tableheap;
  ite_txn = itr.ite_txn;
  if (ite_row != nullptr) {
    delete ite_row;
    ite_row = nullptr;
  }
  if (itr.ite_row != nullptr) {
    ite_row = new Row(*itr.ite_row);
  }
  return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  if (*this == ite_tableheap->End()) {
    ASSERT(false, "Cannot increment end iterator.");
  }
  // Find the next tuple.
  RowId next_rid;
  auto page = reinterpret_cast<TablePage *>(ite_tableheap->buffer_pool_manager_->FetchPage(ite_row->GetRowId().GetPageId()));
  ASSERT(page != nullptr, "Fetch page failed.");
  page->RLatch();
  // Update the current row.
  if (page->GetNextTupleRid(ite_row->GetRowId(), &next_rid)) {
    ite_row->destroy();
    ite_row->SetRowId(next_rid);
    ASSERT(ite_tableheap->GetTuple(ite_row, ite_txn), "Get tuple failed.");
    page->RUnlatch();
    ite_tableheap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
    return *this;
  }
  // Find the next page with a valid tuple.
  page_id_t next_page_id = page->GetNextPageId();
  page->RUnlatch();
  ite_tableheap->buffer_pool_manager_->UnpinPage(page->GetTablePageId(), false);
  while (next_page_id != INVALID_PAGE_ID) {
    // Fetch the next page.
    auto next_page = reinterpret_cast<TablePage *>(ite_tableheap->buffer_pool_manager_->FetchPage(next_page_id));
    ASSERT(next_page != nullptr, "Fetch page failed.");
    next_page->RLatch();
    // Get the first tuple in the next page.
    if (next_page->GetFirstTupleRid(&next_rid)) {
      // Update the current row.
      ite_row->destroy();
      ite_row->SetRowId(next_rid);
      ASSERT(ite_tableheap->GetTuple(ite_row, ite_txn), "Get tuple failed.");
      next_page->RUnlatch();
      ite_tableheap->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
      return *this;
    }
    // Move to the next page.
    next_page_id = next_page->GetNextPageId();
    next_page->RUnlatch();
    ite_tableheap->buffer_pool_manager_->UnpinPage(next_page->GetTablePageId(), false);
  }
  // Set the iterator to end.
  *this = ite_tableheap->End();
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  TableIterator temp = TableIterator(*this);
  ++(*this);
  return temp;
}
