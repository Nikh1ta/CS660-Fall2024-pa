#include <cstring>
#include <db/BTreeFile.hpp>
#include <db/Database.hpp>
#include <db/IndexPage.hpp>
#include <db/LeafPage.hpp>
#include <stdexcept>

using namespace db;

BTreeFile::BTreeFile(const std::string &name, const TupleDesc &td, size_t key_index)
    : DbFile(name, td), key_index(key_index) {}

void BTreeFile::insertTuple(const Tuple &t) {
    // Traverse to the correct leaf page based on the tuple's key
    PageId currentPid{name, root_id};  // Start with the root
    Page *currentPage = &getDatabase().getBufferPool().getPage(currentPid);

    // Traverse until we reach a leaf
    while (!reinterpret_cast<IndexPage*>(currentPage)->header->index_children) {
        currentPid.page = reinterpret_cast<IndexPage*>(currentPage)->children[0];  // Leftmost child
        currentPage = &getDatabase().getBufferPool().getPage(currentPid);
    }

    LeafPage leafPage(*currentPage, td, key_index);
    bool needToSplit = leafPage.insertTuple(t);

    // Handle splitting logic...
}

void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
    PageId pid{name, it.page};  // Get the correct page
    Page &page = getDatabase().getBufferPool().getPage(pid);  // Fetch the page from the buffer pool
    LeafPage leafPage(page, td, key_index);  // Create a LeafPage object for reading

    return leafPage.getTuple(it.slot);  // Return the tuple at the specified slot
}

void BTreeFile::next(Iterator &it) const {
    PageId pid{name, it.page};  // Get the current page
    Page &page = getDatabase().getBufferPool().getPage(pid);
    LeafPage leafPage(page, td, key_index);

    it.slot++;  // Move to the next slot in the current page

    // Instead of calling end(), compare against the number of tuples in the page
    if (it.slot >= leafPage.header->size) {  // Check if we've passed the last slot
        it.page++;  // Move to the next page
        it.slot = 0;  // Reset slot for the new page
    }
}

Iterator BTreeFile::begin() const {
    BufferPool &bufferPool = getDatabase().getBufferPool();
    PageId pid{name, root_id};  // Start at the root
    Page rootPage = bufferPool.getPage(pid);
    IndexPage ip(rootPage);

    // Traverse until we reach a leaf page
    while (ip.header->index_children) {
        pid.page = ip.children[0];  // Move to the leftmost child
        Page nextPage = bufferPool.getPage(pid);
        ip = IndexPage(nextPage);  // Continue traversal
    }

    // Now we're at a leaf page
    Page firstLeafPage = bufferPool.getPage(pid);
    LeafPage leafPage(firstLeafPage, td, key_index);

    return Iterator(*this, pid.page, 0);  // Start with the first tuple (slot 0)
}

Iterator BTreeFile::end() const {
    return Iterator(*this, getNumPages(), 0);  // End iterator, indicating no more tuples
}
