#include <cstring>
#include <db/Tuple.hpp>
#include <stdexcept>
#include <numeric>
#include <unordered_set>

using namespace db;

Tuple::Tuple(const std::vector<field_t> &fields) : fields(fields) {}

type_t Tuple::field_type(size_t i) const {
  const field_t &field = fields.at(i);
  if (std::holds_alternative<int>(field)) {
    return type_t::INT;
  }
  if (std::holds_alternative<double>(field)) {
    return type_t::DOUBLE;
  }
  if (std::holds_alternative<std::string>(field)) {
    return type_t::CHAR;
  }
  throw std::logic_error("Unknown field type");
}

size_t Tuple::size() const { return fields.size(); }

const field_t &Tuple::get_field(size_t i) const { return fields.at(i); }

TupleDesc::TupleDesc(const std::vector<type_t> &types, const std::vector<std::string> &names) {
  if (types.size() != names.size()) {
    throw std::logic_error("Mismatch between number of types and names");
  }

  std::unordered_set<std::string> uniqueNames(names.begin(), names.end());
  if (uniqueNames.size() != names.size()) {
    throw std::logic_error("Field names must be unique");
  }

  this->types = types;
  this->names = names;
}

bool TupleDesc::compatible(const Tuple &tuple) const {
  if (tuple.size() != types.size()) return false;

  for (size_t i = 0; i < types.size(); ++i) {
    if (tuple.field_type(i) != types[i]) return false;
  }
  return true;
}

size_t TupleDesc::index_of(const std::string &name) const {
  auto it = std::find(names.begin(), names.end(), name);
  if (it == names.end()) {
    throw std::runtime_error("Field name not found");
  }
  return std::distance(names.begin(), it);
}

size_t TupleDesc::offset_of(const size_t &index) const {
  if (index >= this->types.size()) {
    throw std::invalid_argument("TupleDesc::offset_of() - index out of range!");
  }

  size_t offset = 0;
  for (size_t i = 0; i < index; ++i) {
    switch (types[i]) {
    case type_t::INT:
      offset += sizeof(int);
      break;
    case type_t::DOUBLE:
      offset += sizeof(double);
      break;
    case type_t::CHAR:
      offset += CHAR_SIZE;  // Assuming fixed size for CHAR
      break;
    }
  }
  return offset;
}

size_t TupleDesc::length() const {
  size_t len = 0;
  for (const auto &type : types) {
    switch (type) {
    case type_t::INT:
      len += sizeof(int);
      break;
    case type_t::DOUBLE:
      len += sizeof(double);
      break;
    case type_t::CHAR:
      len += CHAR_SIZE;  // Assuming fixed size for CHAR
      break;
    }
  }
  return len;
}

size_t TupleDesc::size() const {
  return types.size();
}

Tuple TupleDesc::deserialize(const uint8_t *data) const {
  std::vector<field_t> fields;
  size_t offset = 0;

  for (const auto &type : types) {
    switch (type) {
    case type_t::INT: {
      int intValue;
      std::memcpy(&intValue, data + offset, sizeof(int));
      fields.push_back(intValue);
      offset += sizeof(int);
      break;
    }
    case type_t::DOUBLE: {
      double doubleValue;
      std::memcpy(&doubleValue, data + offset, sizeof(double));
      fields.push_back(doubleValue);
      offset += sizeof(double);
      break;
    }
    case type_t::CHAR: {
      std::string charValue(reinterpret_cast<const char *>(data + offset), CHAR_SIZE);
      // Trimming any '\0' from the char field to avoid extra padding characters
      charValue.erase(std::find(charValue.begin(), charValue.end(), '\0'), charValue.end());
      fields.push_back(charValue);
      offset += CHAR_SIZE;
      break;
    }
    }
  }
  return Tuple(fields);
}

void TupleDesc::serialize(uint8_t *data, const Tuple &t) const {
  size_t offset = 0;

  for (size_t i = 0; i < types.size(); ++i) {
    switch (types[i]) {
    case type_t::INT: {
      int intValue = std::get<int>(t.get_field(i));
      std::memcpy(data + offset, &intValue, sizeof(int));
      offset += sizeof(int);
      break;
    }
    case type_t::DOUBLE: {
      double doubleValue = std::get<double>(t.get_field(i));
      std::memcpy(data + offset, &doubleValue, sizeof(double));
      offset += sizeof(double);
      break;
    }
    case type_t::CHAR: {
      std::string charValue = std::get<std::string>(t.get_field(i));
      std::memcpy(data + offset, charValue.c_str(), CHAR_SIZE);
      offset += CHAR_SIZE;
      break;
    }
    }
  }
}

db::TupleDesc TupleDesc::merge(const TupleDesc &td1, const TupleDesc &td2) {
  std::vector<type_t> mergedTypes = td1.types;
  mergedTypes.insert(mergedTypes.end(), td2.types.begin(), td2.types.end());

  std::vector<std::string> mergedNames = td1.names;
  mergedNames.insert(mergedNames.end(), td2.names.begin(), td2.names.end());

  return TupleDesc(mergedTypes, mergedNames);
}
