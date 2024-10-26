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
    BufferPool &bufferPool = getDatabase().getBufferPool();

    PageId currentPid{name, root_id};  // Start with the root
    Page &currentPage = bufferPool.getPage(currentPid);

    // Traverse to find the correct leaf page
    while (true) {
        IndexPage indexPage(currentPage);

        // Check if we reached a leaf
        if (!indexPage.header->index_children) {
            break;
        }

        // Find the next page to traverse to (for simplicity, going to the leftmost child here)
        currentPid.page = indexPage.children[0];
        currentPage = bufferPool.getPage(currentPid);
    }

    // We are now at a leaf page
    LeafPage leafPage(currentPage, td, key_index);
    bool needToSplit = leafPage.insertTuple(t);

    // If the leaf page needs to be split
    if (needToSplit) {
        // Create a new leaf page in the buffer pool for the split
        PageId newLeafPid{name, getNumPages()};
        Page &newLeafPage = bufferPool.getPage(newLeafPid);

        LeafPage newLeafPageObject(newLeafPage, td, key_index);
        int splitKey = leafPage.split(newLeafPageObject);

        // Handle parent node update (splitting index pages)
        // For now, you may need to work on inserting the `splitKey` into the parent index
    }
}

void BTreeFile::deleteTuple(const Iterator &it) {
  // Do not implement
}

Tuple BTreeFile::getTuple(const Iterator &it) const {
    PageId pid{name, it.page};
    Page &page = getDatabase().getBufferPool().getPage(pid);
    LeafPage leafPage(page, td, key_index);

    return leafPage.getTuple(it.slot);
}

void BTreeFile::next(Iterator &it) const {
    PageId pid{name, it.page};
    Page &page = getDatabase().getBufferPool().getPage(pid);
    LeafPage leafPage(page, td, key_index);

    it.slot++;

    if (it.slot >= leafPage.header->size) {
        it.page++;
        it.slot = 0;
    }
}

Iterator BTreeFile::begin() const {
    BufferPool &bufferPool = getDatabase().getBufferPool();
    PageId pid{name, root_id};
    Page rootPage = bufferPool.getPage(pid);
    IndexPage ip(rootPage);

    if (ip.header->size == 0) {
        return end();
    }

    while (ip.header->index_children) {
        pid.page = ip.children[0];
        Page nextPage = bufferPool.getPage(pid);
        ip = IndexPage(nextPage);
    }

    Page firstLeafPage = bufferPool.getPage(pid);
    return Iterator(*this, pid.page, 0);
}

Iterator BTreeFile::end() const {
    return Iterator(*this, getNumPages(), 0);
}
