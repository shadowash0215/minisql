#include "storage/table_heap.h"

#include <unordered_map>
#include <vector>

#include "common/instance.h"
#include "gtest/gtest.h"
#include "record/field.h"
#include "record/schema.h"
#include "utils/utils.h"

static string db_file_name = "table_heap_test.db";
using Fields = std::vector<Field>;

TEST(TableHeapTest, Reset){
  std::string db_name = "table_heap_test.db";
  std::ofstream f(db_name, std::ios::trunc);
}

TEST(TableHeapTest, TableHeapSampleTest) {
  // init testing instance
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  const int row_nums = 10000;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;
  uint32_t size = 0;
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
        new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                   Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    ASSERT_TRUE(table_heap->InsertTuple(row, nullptr));
    if (row_values.find(row.GetRowId().Get()) != row_values.end()) {
      std::cout << row.GetRowId().Get() << std::endl;
      ASSERT_TRUE(false);
    } else {
      row_values.emplace(row.GetRowId().Get(), fields);
      size++;
    }
    delete[] characters;
  }

  ASSERT_EQ(row_nums, row_values.size());
  ASSERT_EQ(row_nums, size);
  for (auto row_kv : row_values) {
    size--;
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  ASSERT_EQ(size, 0);
}

TEST(TableHeapTest, TableHeapSelfTest) {
  setbuf(stdout, NULL);
  // init testing instance
  // DBStorageEngine engine(db_file_name);
  remove(db_file_name.c_str());
  auto disk_mgr_ = new DiskManager(db_file_name);
  auto bpm_ = new BufferPoolManager(DEFAULT_BUFFER_POOL_SIZE, disk_mgr_);
  
  const int row_nums = 100;
  // create schema
  std::vector<Column *> columns = {new Column("id", TypeId::kTypeInt, 0, false, false),
                                   new Column("name", TypeId::kTypeChar, 64, 1, true, false),
                                   new Column("account", TypeId::kTypeFloat, 2, true, false)};
  auto schema = std::make_shared<Schema>(columns);
  // create rows
  std::unordered_map<int64_t, Fields *> row_values;

  // TableHeap *table_heap = TableHeap::Create(engine.bpm_, schema.get(), nullptr, nullptr, nullptr);
  TableHeap *table_heap = TableHeap::Create(bpm_, schema.get(), nullptr, nullptr, nullptr);
  for (int i = 0; i < row_nums; i++) {
    int32_t len = RandomUtils::RandomInt(0, 64);
    char *characters = new char[len];
    RandomUtils::RandomString(characters, len);
    Fields *fields =
      new Fields{Field(TypeId::kTypeInt, i), Field(TypeId::kTypeChar, const_cast<char *>(characters), len, true),
                 Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
    Row row(*fields);
    table_heap->InsertTuple(row, nullptr);
    row_values[row.GetRowId().Get()] = fields;
    delete[] characters;
  }
  LOG(INFO)<<"INsert Done!";
  // iterator test
  auto it = table_heap->Begin(nullptr);
  set<int64_t> Set;
  for (int i = 0; i < row_nums; i++) {
    ASSERT((Set.count(it->GetRowId().Get()) == 0), "[error] - iterator error!");
    Set.insert(it->GetRowId().Get());
    // make sure the iterator is valid
    it++;
  }
  LOG(INFO)<<"Iteration done 1!";
  ASSERT_EQ(row_nums, row_values.size());
  for (auto row_kv : row_values) {
    Row row(RowId(row_kv.first));
    table_heap->GetTuple(&row, nullptr);
    ASSERT_EQ(schema.get()->GetColumnCount(), row.GetFields().size());
    for (size_t j = 0; j < schema.get()->GetColumnCount(); j++) {
      ASSERT_EQ(CmpBool::kTrue, row.GetField(j)->CompareEquals(row_kv.second->at(j)));
    }
    // free spaces
    delete row_kv.second;
  }
  LOG(INFO)<<"Iteration done 2!";
  // update test with new fields
  string characters = "123";
  Fields *fields = new Fields{Field(TypeId::kTypeInt, 2000),
                              Field(TypeId::kTypeChar, const_cast<char *>(characters.c_str()), 3, true),
                              Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-999.f, 999.f))};
  Row row(*fields);
  table_heap->InsertTuple(row, nullptr);
  LOG(INFO)<<"update with new fields!";
  // testget
  Row testGet(row.GetRowId());
  table_heap->GetTuple(&testGet, nullptr);
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(CmpBool::kTrue, testGet.GetField(i)->CompareEquals(fields->at(i)));
  }
  LOG(INFO)<<"Get Done!";
  characters = "123";
  Fields *updated_fields = new Fields{Field(TypeId::kTypeInt, 1000),
                                      Field(TypeId::kTypeChar, const_cast<char *>(characters.c_str()), 3, true),
                                      Field(TypeId::kTypeFloat, RandomUtils::RandomFloat(-1.f, 1.f))};
  Row updated_row(*updated_fields);
  table_heap->UpdateTuple(updated_row, row.GetRowId(), nullptr);
  Row testUpdated(row.GetRowId());
  table_heap->GetTuple(&testUpdated, nullptr);
  LOG(INFO)<<"last!";
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(CmpBool::kTrue, testUpdated.GetField(i)->CompareEquals(updated_fields->at(i)));
  }
  LOG(INFO)<<"Done!";
}