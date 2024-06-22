#include "executor/execute_engine.h"

#include <dirent.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <chrono>

#include "common/result_writer.h"
#include "executor/executors/delete_executor.h"
#include "executor/executors/index_scan_executor.h"
#include "executor/executors/insert_executor.h"
#include "executor/executors/seq_scan_executor.h"
#include "executor/executors/update_executor.h"
#include "executor/executors/values_executor.h"
#include "glog/logging.h"
#include "planner/planner.h"
#include "utils/utils.h"
#include "parser/syntax_tree_printer.h"
#include "utils/tree_file_mgr.h"

extern "C" {
extern int yyparse(void);
static FILE *yyin;
#include "parser/minisql_lex.h"
#include "parser/parser.h"
}

ExecuteEngine::ExecuteEngine() {
  char path[] = "./databases";
  DIR *dir;
  if ((dir = opendir(path)) == nullptr) {
    mkdir("./databases", 0777);
    dir = opendir(path);
  }
  /** When you have completed all the code for
   *  the test, run it using main.cpp and uncomment
   *  this part of the code.
   **/
  struct dirent *stdir;
  while((stdir = readdir(dir)) != nullptr) {
    if( strcmp( stdir->d_name , "." ) == 0 ||
        strcmp( stdir->d_name , "..") == 0 ||
        stdir->d_name[0] == '.')
      continue;
    dbs_[stdir->d_name] = new DBStorageEngine(stdir->d_name, false);
  }
  closedir(dir);
}

std::unique_ptr<AbstractExecutor> ExecuteEngine::CreateExecutor(ExecuteContext *exec_ctx,
                                                                const AbstractPlanNodeRef &plan) {
  switch (plan->GetType()) {
    // Create a new sequential scan executor
    case PlanType::SeqScan: {
      return std::make_unique<SeqScanExecutor>(exec_ctx, dynamic_cast<const SeqScanPlanNode *>(plan.get()));
    }
    // Create a new index scan executor
    case PlanType::IndexScan: {
      return std::make_unique<IndexScanExecutor>(exec_ctx, dynamic_cast<const IndexScanPlanNode *>(plan.get()));
    }
    // Create a new update executor
    case PlanType::Update: {
      auto update_plan = dynamic_cast<const UpdatePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, update_plan->GetChildPlan());
      return std::make_unique<UpdateExecutor>(exec_ctx, update_plan, std::move(child_executor));
    }
      // Create a new delete executor
    case PlanType::Delete: {
      auto delete_plan = dynamic_cast<const DeletePlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, delete_plan->GetChildPlan());
      return std::make_unique<DeleteExecutor>(exec_ctx, delete_plan, std::move(child_executor));
    }
    case PlanType::Insert: {
      auto insert_plan = dynamic_cast<const InsertPlanNode *>(plan.get());
      auto child_executor = CreateExecutor(exec_ctx, insert_plan->GetChildPlan());
      return std::make_unique<InsertExecutor>(exec_ctx, insert_plan, std::move(child_executor));
    }
    case PlanType::Values: {
      return std::make_unique<ValuesExecutor>(exec_ctx, dynamic_cast<const ValuesPlanNode *>(plan.get()));
    }
    default:
      throw std::logic_error("Unsupported plan type.");
  }
}

dberr_t ExecuteEngine::ExecutePlan(const AbstractPlanNodeRef &plan, std::vector<Row> *result_set, Txn *txn,
                                   ExecuteContext *exec_ctx) {
  // Construct the executor for the abstract plan node
  auto executor = CreateExecutor(exec_ctx, plan);

  try {
    executor->Init();
    RowId rid{};
    Row row{};
    while (executor->Next(&row, &rid)) {
      if (result_set != nullptr) {
        result_set->push_back(row);
      }
    }
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Executor Execution: " << ex.what() << std::endl;
    if (result_set != nullptr) {
      result_set->clear();
    }
    return DB_FAILED;
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::Execute(pSyntaxNode ast) {
  if (ast == nullptr) {
    return DB_FAILED;
  }
  auto start_time = std::chrono::system_clock::now();
  unique_ptr<ExecuteContext> context(nullptr);
  if (!current_db_.empty()) context = dbs_[current_db_]->MakeExecuteContext(nullptr);
  switch (ast->type_) {
    case kNodeCreateDB:
      return ExecuteCreateDatabase(ast, context.get());
    case kNodeDropDB:
      return ExecuteDropDatabase(ast, context.get());
    case kNodeShowDB:
      return ExecuteShowDatabases(ast, context.get());
    case kNodeUseDB:
      return ExecuteUseDatabase(ast, context.get());
    case kNodeShowTables:
      return ExecuteShowTables(ast, context.get());
    case kNodeCreateTable:
      return ExecuteCreateTable(ast, context.get());
    case kNodeDropTable:
      return ExecuteDropTable(ast, context.get());
    case kNodeShowIndexes:
      return ExecuteShowIndexes(ast, context.get());
    case kNodeCreateIndex:
      return ExecuteCreateIndex(ast, context.get());
    case kNodeDropIndex:
      return ExecuteDropIndex(ast, context.get());
    case kNodeTrxBegin:
      return ExecuteTrxBegin(ast, context.get());
    case kNodeTrxCommit:
      return ExecuteTrxCommit(ast, context.get());
    case kNodeTrxRollback:
      return ExecuteTrxRollback(ast, context.get());
    case kNodeExecFile:
      return ExecuteExecfile(ast, context.get());
    case kNodeQuit:
      return ExecuteQuit(ast, context.get());
    default:
      break;
  }
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  // Plan the query.
  Planner planner(context.get());
  std::vector<Row> result_set{};
  try {
    planner.PlanQuery(ast);
    // Execute the query.
    ExecutePlan(planner.plan_, &result_set, nullptr, context.get());
  } catch (const exception &ex) {
    std::cout << "Error Encountered in Planner: " << ex.what() << std::endl;
    return DB_FAILED;
  }
  auto stop_time = std::chrono::system_clock::now();
  double duration_time =
      double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
  // Return the result set as string.
  std::stringstream ss;
  ResultWriter writer(ss);

  if (planner.plan_->GetType() == PlanType::SeqScan || planner.plan_->GetType() == PlanType::IndexScan) {
    auto schema = planner.plan_->OutputSchema();
    auto num_of_columns = schema->GetColumnCount();
    if (!result_set.empty()) {
      // find the max width for each column
      vector<int> data_width(num_of_columns, 0);
      for (const auto &row : result_set) {
        for (uint32_t i = 0; i < num_of_columns; i++) {
          data_width[i] = max(data_width[i], int(row.GetField(i)->toString().size()));
        }
      }
      int k = 0;
      for (const auto &column : schema->GetColumns()) {
        data_width[k] = max(data_width[k], int(column->GetName().length()));
        k++;
      }
      // Generate header for the result set.
      writer.Divider(data_width);
      k = 0;
      writer.BeginRow();
      for (const auto &column : schema->GetColumns()) {
        writer.WriteHeaderCell(column->GetName(), data_width[k++]);
      }
      writer.EndRow();
      writer.Divider(data_width);

      // Transforming result set into strings.
      for (const auto &row : result_set) {
        writer.BeginRow();
        for (uint32_t i = 0; i < schema->GetColumnCount(); i++) {
          writer.WriteCell(row.GetField(i)->toString(), data_width[i]);
        }
        writer.EndRow();
      }
      writer.Divider(data_width);
    }
    writer.EndInformation(result_set.size(), duration_time, true);
  } else {
    writer.EndInformation(result_set.size(), duration_time, false);
  }
  std::cout << writer.stream_.rdbuf();
  // todo:: use shared_ptr for schema
  if (ast->type_ == kNodeSelect)
      delete planner.plan_->OutputSchema();
  return DB_SUCCESS;
}

void ExecuteEngine::ExecuteInformation(dberr_t result) {
  switch (result) {
    case DB_ALREADY_EXIST:
      cout << "Database already exists." << endl;
      break;
    case DB_NOT_EXIST:
      cout << "Database not exists." << endl;
      break;
    case DB_TABLE_ALREADY_EXIST:
      cout << "Table already exists." << endl;
      break;
    case DB_TABLE_NOT_EXIST:
      cout << "Table not exists." << endl;
      break;
    case DB_INDEX_ALREADY_EXIST:
      cout << "Index already exists." << endl;
      break;
    case DB_INDEX_NOT_FOUND:
      cout << "Index not exists." << endl;
      break;
    case DB_COLUMN_NAME_NOT_EXIST:
      cout << "Column not exists." << endl;
      break;
    case DB_KEY_NOT_FOUND:
      cout << "Key not exists." << endl;
      break;
    case DB_QUIT:
      cout << "Bye." << endl;
      break;
    default:
      break;
  }
}

dberr_t ExecuteEngine::ExecuteCreateDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    return DB_ALREADY_EXIST;
  }
  dbs_.insert(make_pair(db_name, new DBStorageEngine(db_name, true)));
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteDropDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) == dbs_.end()) {
    return DB_NOT_EXIST;
  }
  remove(("./databases/" + db_name).c_str());
  delete dbs_[db_name];
  dbs_.erase(db_name);
  if (db_name == current_db_)
    current_db_ = "";
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteShowDatabases(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowDatabases" << std::endl;
#endif
  if (dbs_.empty()) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_SUCCESS;
  }
  int max_width = 8;
  for (const auto &itr : dbs_) {
    if (itr.first.length() > max_width) max_width = itr.first.length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << "Database"
       << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : dbs_) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr.first << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteUseDatabase(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteUseDatabase" << std::endl;
#endif
  string db_name = ast->child_->val_;
  if (dbs_.find(db_name) != dbs_.end()) {
    current_db_ = db_name;
    cout << "Database changed" << endl;
    return DB_SUCCESS;
  }
  return DB_NOT_EXIST;
}

dberr_t ExecuteEngine::ExecuteShowTables(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowTables" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<TableInfo *> tables;
  if (dbs_[current_db_]->catalog_mgr_->GetTables(tables) == DB_FAILED) {
    cout << "Empty set (0.00 sec)" << endl;
    return DB_FAILED;
  }
  string table_in_db("Tables_in_" + current_db_);
  uint max_width = table_in_db.length();
  for (const auto &itr : tables) {
    if (itr->GetTableName().length() > max_width) max_width = itr->GetTableName().length();
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  cout << "| " << std::left << setfill(' ') << setw(max_width) << table_in_db << " |" << endl;
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  for (const auto &itr : tables) {
    cout << "| " << std::left << setfill(' ') << setw(max_width) << itr->GetTableName() << " |" << endl;
  }
  cout << "+" << setfill('-') << setw(max_width + 2) << ""
       << "+" << endl;
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateTable" << std::endl;
#endif
  DBStorageEngine *current_db_engine = dbs_[current_db_];
  if(current_db_engine == nullptr)
    return DB_FAILED;
  string table_name = ast->child_->val_;
  TableInfo *table_info = nullptr;
  if(current_db_engine->catalog_mgr_->GetTable(table_name, table_info) == DB_SUCCESS){
    cout << "table exits" << endl;
    return DB_FAILED;
  }
  table_info = nullptr;
  auto klist = ast->child_->next_;
  auto knode = klist->child_;
  vector<Column*> columns = {};
  vector<string> column_names = {};
  unordered_map<string, string> column_type = {};
  unordered_map<string, int> char_size = {};
  unordered_map<string, bool> is_unique = {};
  unordered_map<string, bool> is_primary = {};
  vector<string> unique_key = {};
  vector<string> primary_key = {};
  // get column names and types
  while(knode != nullptr && knode->type_ == kNodeColumnDefinition){
    string unique = "";
    if(knode->val_ == nullptr)
      unique = "";
    else
      unique = knode->val_;
    string column_name = knode->child_->val_;
    string type = knode->child_->next_->val_;
    column_names.push_back(column_name);
    column_type[column_name] = type;
    is_primary[column_name] = false;
    if(type == "char"){
      char_size[column_name] = stoi(knode->child_->next_->child_->val_);
      if(char_size[column_name] <= 0){
        cout << "char size <= 0" << endl;
        return DB_FAILED;
      }
    }
    if(unique == "unique"){
      is_unique[column_name] = true;
      unique_key.push_back(column_name);
    }
    else
      is_unique[column_name] = false;
    knode = knode->next_;
  }
  // get primary key
  if(knode != nullptr){
    auto pnode = knode->child_;
    while(pnode != nullptr){
      string column_name = pnode->val_;
      is_unique[column_name] = true;
      is_primary[column_name] = true;
      unique_key.push_back(column_name);  
      primary_key.push_back(column_name);
      pnode = pnode->next_;
    }
  }
  // get columns
  int column_index = 0;
  for(auto column_name : column_names){
    Column *column = nullptr;
    if(column_type[column_name] == "int")
        column = new Column(column_name, TypeId::kTypeInt, column_index, false, 
                            (is_unique[column_name] || is_primary[column_name]));
    else if(column_type[column_name] == "char")
      column = new Column(column_name, TypeId::kTypeChar, char_size[column_name], column_index, false, 
                            (is_unique[column_name]||is_primary[column_name]));
    else if(column_type[column_name] == "float")
      column = new Column(column_name, TypeId::kTypeFloat, column_index, false, 
                            (is_unique[column_name]||is_primary[column_name]));
    else{
      cout << "unknown typename" << column_type[column_name] << endl;
      return DB_FAILED;
    }
    columns.push_back(column);
    column_index++;
  }
  // create table
  Schema *schema = new Schema(columns);
  Txn *txn;
  if(current_db_engine->catalog_mgr_->CreateTable(table_name, schema, txn, table_info) != DB_SUCCESS){
    cout << "create table failed" << endl;
    return current_db_engine->catalog_mgr_->CreateTable(table_name, schema, txn, table_info);
  }
  // create index
  IndexInfo *index_info = nullptr;
  for(auto column_name : unique_key){
    index_info = nullptr;
    vector<string> index_keys {column_name};
    auto res = dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, column_name, index_keys, txn, index_info, "bptree");
    if (res != DB_SUCCESS) {
      cout << "create index failed" << endl;
      return res;
    }
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropTable(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropTable" << std::endl;
#endif
  if(ast == nullptr || ast->child_ == nullptr)
    return DB_FAILED;
  string table_name = ast->child_->val_;
  return dbs_[current_db_]->catalog_mgr_->DropTable(table_name);
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteShowIndexes(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteShowIndexes" << std::endl;
#endif
  if (current_db_.empty()) {
    cout << "No database selected" << endl;
    return DB_FAILED;
  }
  vector<IndexInfo *> indexes;
  vector<TableInfo *> tables;
  if(dbs_[current_db_]->catalog_mgr_->GetTables(tables) != DB_SUCCESS)
    return dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  for(auto table : tables){
    if(dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes) != DB_SUCCESS)
      return dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
    IndexInfo *index_info = nullptr;
    for(auto index : indexes)
      cout << index->GetIndexName() << endl;
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteCreateIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteCreateIndex" << std::endl;
#endif
  if(ast == nullptr || current_db_ == "")
    return DB_FAILED;
  string table_name = ast->child_->next_->val_;
  string index_name = ast->child_->val_;
  TableInfo *table_info = nullptr;
  if(dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info) != DB_SUCCESS)
    return dbs_[current_db_]->catalog_mgr_->GetTable(table_name, table_info);
  vector<string> column_names = {};
  auto pnode = ast->child_->next_->next_->child_;
  while(pnode != nullptr){
    column_names.push_back(pnode->val_);
    pnode = pnode->next_;
  }
  Schema *schema = table_info->GetSchema();
  IndexInfo *index_info = nullptr;
  Txn *txn;
  if(dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, column_names, txn, index_info, "bptree") != DB_SUCCESS)
    return dbs_[current_db_]->catalog_mgr_->CreateIndex(table_name, index_name, column_names, txn, index_info, "bptree");
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteDropIndex(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteDropIndex" << std::endl;
#endif
  if(ast == nullptr || current_db_ == "")
    return DB_FAILED;
  string index_name = ast->child_->val_;
  vector<TableInfo *> tables;
  if(dbs_[current_db_]->catalog_mgr_->GetTables(tables) != DB_SUCCESS)
    return dbs_[current_db_]->catalog_mgr_->GetTables(tables);
  string table_name = "";
  for(auto table : tables){
    vector<IndexInfo *> indexes;
    if(dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes) != DB_SUCCESS)
      return dbs_[current_db_]->catalog_mgr_->GetTableIndexes(table->GetTableName(), indexes);
    for(auto index : indexes){
      if(index->GetIndexName() == index_name){
        table_name = table->GetTableName();
        break;
      }
    }
  }
  IndexInfo *index_info = nullptr;
  if(dbs_[current_db_]->catalog_mgr_->GetIndex(table_name, index_name, index_info) != DB_SUCCESS){
    cout << "no index: " << index_name << endl;
    return dbs_[current_db_]->catalog_mgr_->GetIndex(table_name, index_name, index_info);
  }
  if(dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name) != DB_SUCCESS){
    cout << "fail to drop index: " << index_name << endl;
    return dbs_[current_db_]->catalog_mgr_->DropIndex(table_name, index_name);
  }
  return DB_SUCCESS;
}

dberr_t ExecuteEngine::ExecuteTrxBegin(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxBegin" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxCommit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxCommit" << std::endl;
#endif
  return DB_FAILED;
}

dberr_t ExecuteEngine::ExecuteTrxRollback(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteTrxRollback" << std::endl;
#endif
  return DB_FAILED;
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteExecfile(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteExecfile" << std::endl;
#endif
  string filename = ast->child_->val_;
  ifstream file(filename);
  if(!file.is_open()){
    cout << "fail to open '" << filename << "'" << endl;
    return DB_FAILED;
  }
  int buffer_size = 1024;
  char* buffer_ptr = new char[buffer_size];
  auto start_time = std::chrono::system_clock::now();
  while(1){
    char tmp_char;
    int cnt = 0;
    do{
      if(!file.get(tmp_char)){
        delete buffer_ptr;
        auto stop_time = std::chrono::system_clock::now();
        double duration_time = double((std::chrono::duration_cast<std::chrono::milliseconds>(stop_time - start_time)).count());
        cout << "Total time: (" << duration_time / 1000 << " sec)" << endl;
        return DB_SUCCESS;
      }
      buffer_ptr[cnt] = tmp_char;
      cnt++;
      if(cnt >= buffer_size){
        cout << "buffer overflow" << endl;
        return DB_FAILED;
      }
    }while(tmp_char != ';');
    buffer_ptr[cnt] = '\0';
    // create buffer for sql input
    TreeFileManagers syntax_tree_file_mgr("syntax_tree_");
    uint32_t syntax_tree_id = 0;
    YY_BUFFER_STATE bp = yy_scan_string(buffer_ptr);
    if (bp == nullptr) {
      LOG(ERROR) << "Failed to create yy buffer state." << std::endl;
      exit(1);
    }
    yy_switch_to_buffer(bp);

    // init parser module
    MinisqlParserInit();

    // parse
    yyparse();

    // parse result handle
    if (MinisqlParserGetError())
      printf("%s\n", MinisqlParserGetErrorMessage());
    else {
      cout<<"[INFO] Sql syntax parse ok!"<<endl;
      SyntaxTreePrinter printer(MinisqlGetParserRootNode());
      printer.PrintTree(syntax_tree_file_mgr[syntax_tree_id++]);
    }
    auto result = this->Execute(MinisqlGetParserRootNode());
    MinisqlParserFinish();
    yy_delete_buffer(bp);
    yylex_destroy();
    this->ExecuteInformation(result);
    if (result == DB_QUIT)
      break;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t ExecuteEngine::ExecuteQuit(pSyntaxNode ast, ExecuteContext *context) {
#ifdef ENABLE_EXECUTE_DEBUG
  LOG(INFO) << "ExecuteQuit" << std::endl;
#endif
 return DB_QUIT;
}
