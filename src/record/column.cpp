#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  // replace with your code here
  uint32_t SerializedSize = 0;
  MACH_WRITE_UINT32(buf,COLUMN_MAGIC_NUM);
  SerializedSize += sizeof(uint32_t);
  uint32_t temp = name_.length();
  MACH_WRITE_UINT32(buf+SerializedSize,temp);
  SerializedSize += sizeof(uint32_t);
  MACH_WRITE_STRING(buf+SerializedSize,name_);
  SerializedSize += name_.length();
  MACH_WRITE_TO(TypeId,buf+SerializedSize,type_);
  SerializedSize += sizeof(type_);
  MACH_WRITE_UINT32(buf+SerializedSize,len_);
  SerializedSize += sizeof(uint32_t);
  MACH_WRITE_UINT32(buf+SerializedSize,table_ind_);
  SerializedSize += sizeof(uint32_t);
  MACH_WRITE_TO(bool,buf+SerializedSize,nullable_);
  SerializedSize += sizeof(bool);
  MACH_WRITE_TO(bool,buf+SerializedSize,unique_);
  SerializedSize += sizeof(bool);
  return SerializedSize;
  // return 0;
}

/** 
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  // replace with your code here
  // return 0;
  return sizeof(uint32_t) * 4 + name_.length() + sizeof(type_) + sizeof(bool) * 2;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  // replace with your code here
  uint32_t COLUMN_MAGIC_NUM_R=0;
  TypeId type;
  uint32_t len,table_ind;
  bool nullable,unique,SerializedSize=0;
  std::string column_name;
  COLUMN_MAGIC_NUM_R=MACH_READ_UINT32(buf);
  SerializedSize+=sizeof(uint32_t);
  if(COLUMN_MAGIC_NUM_R != COLUMN_MAGIC_NUM)
        LOG(ERROR) << "magic number is wrong" << std::endl;
  uint32_t strlen=MACH_READ_UINT32(buf+SerializedSize);
  SerializedSize+=sizeof(uint32_t);
  for(uint32_t i=0;i<strlen;i++)
  {
      char c=MACH_READ_FROM(char,buf+SerializedSize);
      SerializedSize+=sizeof(char);
      column_name+=c;
  }
  type=MACH_READ_FROM(TypeId,buf+SerializedSize);
  SerializedSize+=sizeof(TypeId);
  len=MACH_READ_UINT32(buf+SerializedSize);
  SerializedSize+=sizeof(uint32_t);
  table_ind=MACH_READ_UINT32(buf+SerializedSize);
  SerializedSize+=sizeof(uint32_t);
  nullable=MACH_READ_FROM(bool,buf+SerializedSize);
  SerializedSize+=sizeof(bool);
  unique=MACH_READ_FROM(bool,buf+SerializedSize);
  SerializedSize+=sizeof(bool);
  if (type == kTypeChar) {
    column = new Column(column_name, type, len, table_ind, nullable, unique);
  } else {
    column = new Column(column_name, type, table_ind, nullable, unique);
  }
  // return 0;
  return SerializedSize;
}
