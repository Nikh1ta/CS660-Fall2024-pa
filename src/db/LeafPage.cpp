#include <db/LeafPage.hpp>
#include <stdexcept>
#include <cstring>

using namespace db;

LeafPage::LeafPage(Page &page, const TupleDesc &td, size_t key_index)
    : td(td), key_index(key_index) {
    header = reinterpret_cast<LeafPageHeader*>(page.data());
    size_t pageSize = page.size();
    size_t headerSize = sizeof(LeafPageHeader);
    size_t tupleSize = td.length();
    capacity = (pageSize - headerSize) / tupleSize;
    data = page.data() + headerSize;
}

bool LeafPage::insertTuple(const Tuple &t) {
    int key = std::get<int>(t.get_field(key_index));
    size_t insertPos = 0;
    while (insertPos < header->size) {
        Tuple currentTuple = getTuple(insertPos);
        int currentKey = std::get<int>(currentTuple.get_field(key_index));
        if (currentKey >= key) {
            break;
        }
        ++insertPos;
    }
    if (insertPos < header->size && std::get<int>(getTuple(insertPos).get_field(key_index)) == key) {
        td.serialize(data + (insertPos * td.length()), t);
        return header->size >= capacity;
    }
    if (header->size >= capacity) {
        return true;
    }
    std::memmove(data + ((insertPos + 1) * td.length()),
               data + (insertPos * td.length()),
               (header->size - insertPos) * td.length());

    td.serialize(data + (insertPos * td.length()), t);
    header->size++;
    return header->size >= capacity;
}

int LeafPage::split(LeafPage &new_page) {
    size_t splitPoint = header->size / 2;
    size_t tupleSize = td.length();

    std::memcpy(new_page.data, data + (splitPoint * tupleSize), (header->size - splitPoint) * tupleSize);

    new_page.header->size = header->size - splitPoint;
    header->size = splitPoint;

    Tuple middleTuple = new_page.getTuple(0);
    int splitKey = std::get<int>(middleTuple.get_field(key_index));

    new_page.header->next_leaf = header->next_leaf;
    header->next_leaf = new_page.header->next_leaf;
    return splitKey;
}

Tuple LeafPage::getTuple(size_t slot) const {
    if (slot >= header->size) {
      throw std::out_of_range("Slot index out of range");
    }
    size_t offset = slot * td.length();
    return td.deserialize(data + offset);
}
