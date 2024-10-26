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
    int key = std::get<int>(t.get_field(key_index));  // Get the key
    size_t insertPos = 0;

    // Find the position to insert the tuple
    while (insertPos < header->size && std::get<int>(getTuple(insertPos).get_field(key_index)) < key) {
        ++insertPos;
    }

    // Overwrite the existing tuple if the key matches
    if (insertPos < header->size && std::get<int>(getTuple(insertPos).get_field(key_index)) == key) {
        td.serialize(data + (insertPos * td.length()), t);
        return header->size >= capacity;  // Return whether the page is full
    }

    // If the page is full, return true to indicate a split is needed
    if (header->size >= capacity) {
        return true;
    }

    // Shift tuples to make space for the new tuple
    std::memmove(data + ((insertPos + 1) * td.length()), data + (insertPos * td.length()),
                 (header->size - insertPos) * td.length());

    // Insert the new tuple
    td.serialize(data + (insertPos * td.length()), t);
    header->size++;  // Increment the number of tuples

    return header->size >= capacity;  // Return whether the page is full
}

int LeafPage::split(LeafPage &new_page) {
    size_t splitPoint = header->size / 2;
    size_t tupleSize = td.length();

    // Move half the tuples to the new page
    std::memcpy(new_page.data, data + (splitPoint * tupleSize), (header->size - splitPoint) * tupleSize);

    // Update sizes
    new_page.header->size = header->size - splitPoint;
    header->size = splitPoint;

    // Return the key of the first tuple in the new page
    Tuple middleTuple = new_page.getTuple(0);
    return std::get<int>(middleTuple.get_field(key_index));
}

Tuple LeafPage::getTuple(size_t slot) const {
    if (slot >= header->size) {
        throw std::out_of_range("Invalid slot");
    }

    return td.deserialize(data + slot * td.length());
}
