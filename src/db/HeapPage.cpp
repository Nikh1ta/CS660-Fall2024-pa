#include <db/Database.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>

using namespace db;

HeapPage::HeapPage(Page &page, const TupleDesc &td) : td(td) {
  const size_t P = page.size();  // Total page size in bytes
  const size_t T = td.length();  // Tuple size in bytes

  this->capacity = (__CHAR_BIT__ * P) / (__CHAR_BIT__ * T + 1);  // Calculate the capacity of the page
  this->header = page.data();
  this->data = page.data() + P - capacity * T;  // Set data pointer after the header space
}

size_t HeapPage::begin() const {
  // Iterate through the page to find the first non-empty slot
  for (size_t slot = 0; slot < capacity; ++slot) {
    const size_t header_slot_index = slot / 8;
    const size_t header_slot_offset = slot - 8 * header_slot_index;
    if (header[header_slot_index] & (1 << (__CHAR_BIT__ - 1 - header_slot_offset))) {
      return slot;
    }
  }
  return capacity;  // No occupied slots found
}

size_t HeapPage::end() const {
  // The end is always the total capacity of the page
  return capacity;
}

bool HeapPage::insertTuple(const Tuple &t) {
  // Look for an empty slot to insert the tuple
  for (size_t slot = 0; slot < capacity; ++slot) {
    if (this->empty(slot)) {
      // Serialize the tuple into the data section
      size_t offset = slot * td.length();
      td.serialize(data + offset, t);

      // Mark the slot as used in the header
      const size_t header_slot_index = slot / 8;
      const size_t header_slot_offset = slot - 8 * header_slot_index;
      header[header_slot_index] |= (1 << (__CHAR_BIT__ - 1 - header_slot_offset));
      return true;
    }
  }
  return false;  // No empty slots, page is full
}

void HeapPage::deleteTuple(size_t slot) {
  // Ensure that the slot is not empty before attempting to delete
  if (this->empty(slot)) {
    throw std::invalid_argument("Cannot delete from an empty slot!");
  }

  // Mark the slot as empty in the header
  const size_t header_slot_index = slot / 8;
  const size_t header_slot_offset = slot - 8 * header_slot_index;
  header[header_slot_index] &= ~(1 << (__CHAR_BIT__ - 1 - header_slot_offset));
}

Tuple HeapPage::getTuple(size_t slot) const {
  // Ensure that the slot is not empty before retrieving the tuple
  if (this->empty(slot)) {
    throw std::invalid_argument("Cannot get tuple from an empty slot!");
  }

  size_t offset = slot * td.length();
  return td.deserialize(data + offset);
}

void HeapPage::next(size_t &slot) const {
  // Move to the next occupied slot
  while (slot < this->end()) {
    ++slot;
    if (!this->empty(slot)) {
      return;
    }
  }
  // No more occupied slots
  slot = capacity;
}

bool HeapPage::empty(size_t slot) const {
  // Check if the slot is empty by inspecting the corresponding bit in the header
  const size_t header_slot_index = slot / 8;
  const size_t header_slot_offset = slot - 8 * header_slot_index;
  return !(header[header_slot_index] & (1 << (__CHAR_BIT__ - 1 - header_slot_offset)));
}
