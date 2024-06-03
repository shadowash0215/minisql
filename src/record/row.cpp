#include "record/row.h"
using namespace std;
/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t SerializedSize = 0;
  vector<bool> bits;
  for(auto field : fields_){
    bits.push_back(field->IsNull());
  }
  for(auto bit : bits){
    MACH_WRITE_TO(bool,buf+SerializedSize,bit);
    SerializedSize += sizeof(bool);
  }
  for(auto field:fields_){
    if(!field->IsNull()){
      field->SerializeTo(buf+SerializedSize);
      SerializedSize+=field->GetSerializedSize();
    }
  }
  return SerializedSize;
  // return 0;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // replace with your code here
  uint32_t SerializedSize = 0;
  vector<bool> bits;
  for(uint32_t i = 0; i < schema->GetColumnCount(); i++){
    bits.push_back(MACH_READ_FROM(bool,buf+SerializedSize));
    SerializedSize += sizeof(bool);
  }
  for(unsigned long i = 0; i < bits.size(); i++){
    auto* field_tmp=new Field(schema->GetColumn(i)->GetType());
    Field::DeserializeFrom(buf+SerializedSize, schema->GetColumn(i)->GetType(), &field_tmp,bits[i]);
    fields_.push_back(field_tmp);
    SerializedSize += fields_.back()->GetSerializedSize();
  }
  return SerializedSize;
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // replace with your code here
  uint32_t SerializedSize = 0;
  for(auto field : fields_){
    if(!field->IsNull()){
      SerializedSize += field->GetSerializedSize();
    }
  }
  SerializedSize += fields_.size()*sizeof(bool);
  return SerializedSize;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
