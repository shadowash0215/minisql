#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return 4 + 4 + 4 + table_meta_pages_.size() * (4 + 4) + index_meta_pages_.size() * (4 + 4);
}

CatalogMeta::CatalogMeta() {}

/**
 * @return a new instance of CatalogMeta
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
      if (init) {
        catalog_meta_ = CatalogMeta::NewInstance();
        auto catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_->SerializeTo(catalog_page->GetData());
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
      } else {
        auto catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
        catalog_meta_ = CatalogMeta::DeserializeFrom(reinterpret_cast<char *>(catalog_page->GetData()));
        next_index_id_ = catalog_meta_->GetNextIndexId();
        next_table_id_ = catalog_meta_->GetNextTableId();
        for (auto it: catalog_meta_->table_meta_pages_) {
          LoadTable(it.first, it.second);
        }
        for (auto it: catalog_meta_->index_meta_pages_) {
          LoadIndex(it.first, it.second);
        }
        buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
      }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param schema the schema of the table
 * @param txn the transaction that is creating the table
 * @param table_info the table info that is created
 * @return DB_SUCCESS if the table is created successfully, DB_ALREADY_EXIST if the table already exists
 * @brief Create a table with the given name and schema
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  // Check if table already exists
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  } else {
    // Allocate data page for table
    page_id_t page_id;
    auto table_meta_page = buffer_pool_manager_->NewPage(page_id);
    // Create table
    table_id_t table_id = next_table_id_;
    table_names_.emplace(table_name, table_id);
    catalog_meta_->table_meta_pages_.emplace(table_id, page_id);
    next_table_id_ = catalog_meta_->GetNextTableId();
    // Create table heap
    auto table_schema = TableSchema::DeepCopySchema(schema);
    auto table_heap = TableHeap::Create(buffer_pool_manager_, table_schema, txn, log_manager_, lock_manager_);
    // Create table metadata
    auto table_meta_data = TableMetadata::Create(table_id, table_name, table_heap->GetFirstPageId(), table_schema);
    // Serialize table metadata
    table_meta_data->SerializeTo(table_meta_page->GetData());
    buffer_pool_manager_->UnpinPage(page_id, true);
    // Create table info
    table_info = TableInfo::Create();
    table_info->Init(table_meta_data, table_heap);
    tables_.emplace(table_id, table_info);
    return DB_SUCCESS;
  }

}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param table_info the table info that is returned
 * @return DB_SUCCESS if the table is found, DB_TABLE_NOT_EXIST if the table does not exist
 * @brief Get a table
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  auto table = table_names_.find(table_name);
  if (table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  } else {
    table_info = tables_.find(table->second)->second;
    return DB_SUCCESS;
  }
}

/**
 * @param tables the vector of table info that is returned
 * @return DB_SUCCESS if the tables are found
 * @brief Get all tables
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  tables.clear();
  for (auto iter : tables_) {
    tables.push_back(iter.second);
  }
  return DB_SUCCESS;
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param index_name the name of the index stored in the index_names_ map
 * @param index_keys the keys of the index that is created
 * @param txn the transaction that is creating the index
 * @param index_info the index info that is created
 * @param index_type the type of the index that is created
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_INDEX_ALREADY_EXIST if the index already exists, DB_COLUMN_NAME_NOT_EXIST if the column name does not exist in the schema, DB_SUCCESS if the index is created successfully
 * @brief Create an index on a table
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  auto table = table_names_.find(table_name);
  // Check if table exists
  if (table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  } else {
    // Check if index already exists
    auto index = index_names_.find(table_name);
    if (index != index_names_.end() && index->second.find(index_name) != index->second.end()) {
      return DB_INDEX_ALREADY_EXIST;
    } else {
      // Allocate data page for index
      page_id_t page_id;
      auto index_meta_page = buffer_pool_manager_->NewPage(page_id);
      // Create index
      next_index_id_ = catalog_meta_->GetNextIndexId();
      // Create index metadata
      auto table_info = tables_.find(table->second)->second;
      auto schema = table_info->GetSchema();
      // Get key mapping according to index keys
      std::vector<uint32_t> key_map;
      for (auto key : index_keys) {
        uint32_t index;
        if (schema->GetColumnIndex(key, index) == DB_COLUMN_NAME_NOT_EXIST) {
          return DB_COLUMN_NAME_NOT_EXIST;
        }
        key_map.push_back(index);
      }
      index_id_t index_id = next_index_id_;
      index_names_[table_name].emplace(index_name, index_id);
      catalog_meta_->index_meta_pages_.emplace(index_id, page_id);
      auto index_meta_data = IndexMetadata::Create(index_id, index_name, table->second, key_map);
      // Serialize index metadata
      index_meta_data->SerializeTo(index_meta_page->GetData());
      buffer_pool_manager_->UnpinPage(page_id, true);
      // Create index info
      index_info = IndexInfo::Create();
      index_info->Init(index_meta_data, table_info, buffer_pool_manager_);
      indexes_.emplace(index_id, index_info);
      return DB_SUCCESS;
    }
  }
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param index_name the name of the index stored in the index_names_ map
 * @param index_info the index info that is returned
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_INDEX_NOT_FOUND if the index does not exist, DB_SUCCESS if the index is found
 * @brief Get an index of a table
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  auto index_tuple = index_names_.find(table_name);
  if (index_tuple == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  } else {
    auto index = index_tuple->second.find(index_name);
    if (index == index_tuple->second.end()) {
      return DB_INDEX_NOT_FOUND;
    } else {
      index_info = indexes_.find(index->second)->second;
      return DB_SUCCESS;
    }
  }
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param indexes the vector of index info that is returned
 * @return DB_INDEX_NOT_FOUND if the table does not exist, DB_SUCCESS if the indexes are found
 * @brief Get all indexes of a table
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  auto index_tuple = index_names_.find(table_name);
  if (index_tuple == index_names_.end()) {
    return DB_INDEX_NOT_FOUND;
  } else {
    indexes.clear();
    for (auto index : index_tuple->second) {
      indexes.push_back(indexes_.find(index.second)->second);
    }
    return DB_SUCCESS;
  }
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_SUCCESS if the table is dropped
 * @brief Drop a table
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  auto table = table_names_.find(table_name);
  if (table == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  } else {
    // drop all the indexes of the table if exist
    std::vector<IndexInfo *> indexes;
    if (GetTableIndexes(table_name, indexes) == DB_SUCCESS) {
      for (auto index : indexes) {
        DropIndex(table_name, index->GetIndexName());
      }
    }
    // erase table from table_names_
    auto table_id = table->second;
    table_names_.erase(table);
    // erase table from tables_
    auto table_info = tables_.find(table_id)->second;
    tables_.erase(table_id);
    // erase table from tables_meta_pages_
    page_id_t page_id = catalog_meta_->table_meta_pages_[table_id];
    catalog_meta_->table_meta_pages_.erase(table_id);
    // delete table from buffer_pool_manager_
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
    // delete table
    table_info->GetTableHeap()->FreeTableHeap();
    return DB_SUCCESS; 
  }
}

/**
 * @param table_name the name of the table stored in the table_names_ map
 * @param index_name the name of the index stored in the index_names_ map
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_INDEX_NOT_FOUND if the index does not exist, DB_SUCCESS if the index is dropped
 * @brief Drop an index of a table
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  auto index_tuple = index_names_.find(table_name);
  if (index_tuple == index_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  } else {
    auto index = index_tuple->second.find(index_name);
    if (index == index_tuple->second.end()) {
      return DB_INDEX_NOT_FOUND;
    } else {
      // erase index from index_names_
      auto index_id = index->second;
      index_names_.erase(table_name);
      // erase index from indexes_
      auto index_info = indexes_.find(index_id)->second;
      indexes_.erase(index_id);
      // erase index from index_meta_pages_
      page_id_t page_id = catalog_meta_->index_meta_pages_[index_id];
      catalog_meta_->index_meta_pages_.erase(index_id);
      // delete index from buffer_pool_manager_
      buffer_pool_manager_->UnpinPage(page_id, false);
      buffer_pool_manager_->DeletePage(page_id);
      // delete index
      index_info->GetIndex()->Destroy();
      return DB_SUCCESS;
    }
  }
}

/**
 * @return DB_SUCCESS if the catalog metadata page is flushed
 * @brief Flush the catalog metadata page
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto catalog_page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  this->catalog_meta_->SerializeTo(catalog_page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  buffer_pool_manager_->FlushPage(CATALOG_META_PAGE_ID);
  return DB_SUCCESS;
}

/**
 * @param table_id the id of the table
 * @param page_id the id of the page
 * @return DB_TABLE_ALREADY_EXIST if the table already exists, DB_SUCCESS if the table is loaded
 * @brief Load a table
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  if (tables_.find(table_id) != tables_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  } else {
    catalog_meta_->table_meta_pages_.emplace(table_id, page_id);
    auto table_meta_page = buffer_pool_manager_->FetchPage(page_id);
    // Deserialize table metadata
    TableMetadata *table_meta_data;
    TableMetadata::DeserializeFrom(table_meta_page->GetData(), table_meta_data);
    // restore table heap
    auto table_schema = TableSchema::DeepCopySchema(table_meta_data->GetSchema());
    auto table_heap = TableHeap::Create(buffer_pool_manager_, page_id, table_schema, log_manager_, lock_manager_);
    // restore table info
    auto table_info = TableInfo::Create();
    table_info->Init(table_meta_data, table_heap);
    // Update table_names_ and tables_
    table_names_.emplace(table_meta_data->GetTableName(), table_id);
    tables_.emplace(table_id, table_info);
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_SUCCESS;
  }
}

/**
 * @param index_id the id of the index
 * @param page_id the id of the page
 * @return DB_INDEX_ALREADY_EXIST if the index already exists, DB_SUCCESS if the index is loaded
 * @brief Load an index
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  if (indexes_.find(index_id) != indexes_.end()) {
    return DB_INDEX_ALREADY_EXIST;
  } else {
    catalog_meta_->index_meta_pages_.emplace(index_id, page_id);
    auto index_meta_page = buffer_pool_manager_->FetchPage(page_id);
    // Deserialize index metadata
    IndexMetadata *index_meta_data;
    IndexMetadata::DeserializeFrom(index_meta_page->GetData(), index_meta_data);
    // restore index info
    auto table_info = tables_.find(index_meta_data->GetTableId())->second;
    auto schema = table_info->GetSchema();
    auto index_info = IndexInfo::Create();
    index_info->Init(index_meta_data, table_info, buffer_pool_manager_);
    // Update index_names_ and indexes_
    index_names_[table_info->GetTableName()].emplace(index_meta_data->GetIndexName(), index_id);
    indexes_.emplace(index_id, index_info);
    buffer_pool_manager_->UnpinPage(page_id, false);
    return DB_SUCCESS;    
  }
}

/**
 * @param table_id the id of the table
 * @return DB_TABLE_NOT_EXIST if the table does not exist, DB_SUCCESS if the table is dropped
 * @brief Get a table
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  auto table = tables_.find(table_id);
  if (table == tables_.end()) {
    return DB_NOT_EXIST;
  } else {
    table_info = table->second;
    return DB_SUCCESS;
  }
}