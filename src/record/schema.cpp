#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  // replace with your code here
  MACH_WRITE_UINT32(buf, SCHEMA_MAGIC_NUM);
  uint32_t SerializedSize = sizeof(uint32_t);
  MACH_WRITE_UINT32(buf + SerializedSize, columns_.size());
  SerializedSize += sizeof(uint32_t);
  for (auto column : columns_) {
    SerializedSize += column->SerializeTo(buf + SerializedSize);
  }
  MACH_WRITE_TO(bool, buf + SerializedSize, is_manage_);
  SerializedSize += sizeof(bool);
  return SerializedSize;
}

uint32_t Schema::GetSerializedSize() const {
  // replace with your code here
  uint32_t SerializedSize = 0;
  SerializedSize += sizeof(uint32_t) * 2;
  for (auto column : columns_) {
    SerializedSize += column->GetSerializedSize();
  }
  SerializedSize += sizeof(bool);
  return SerializedSize;
  // return 0;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  // replace with your code here
  uint32_t magic_num = MACH_READ_UINT32(buf);
  ASSERT(magic_num == SCHEMA_MAGIC_NUM, "Wrong magic number.");
  uint32_t SerializedSize = sizeof(uint32_t);
  uint32_t column_size = MACH_READ_UINT32(buf + SerializedSize);
  SerializedSize += sizeof(uint32_t);
  std::vector<Column *> columns;
  for (uint32_t i = 0; i < column_size; i++) {
    Column *column = nullptr;
    SerializedSize += Column::DeserializeFrom(buf + SerializedSize, column);
    columns.push_back(column);
  }
  bool is_manage = MACH_READ_FROM(bool, buf + SerializedSize);
  SerializedSize += sizeof(bool);
  schema = new Schema(columns, is_manage);
  return SerializedSize;
  // return 0;
}