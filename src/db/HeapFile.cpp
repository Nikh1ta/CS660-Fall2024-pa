#include <iostream>
#include <fstream>
#include <db/Database.hpp>
#include <db/HeapFile.hpp>
#include <db/HeapPage.hpp>
#include <stdexcept>
#include <filesystem>

using namespace db;

HeapFile::HeapFile(const std::string &name, const TupleDesc &td) : DbFile(name, td) {
    // Check if the file exists
    if (std::filesystem::exists(name)) {
        // If it exists, open the file and check its size
        std::ifstream infile(name, std::ios::binary | std::ios::ate);
        if (!infile) {
            throw std::runtime_error("Failed to open existing file for reading");
        }
        std::streamsize fileSize = infile.tellg();

        if (fileSize == 0) {
            // If the file is empty, we treat it as a new file
            this->numPages = 0;
        } else {
            // If the file is not empty, calculate the number of pages
            this->numPages = fileSize / DEFAULT_PAGE_SIZE;
        }
        infile.close();
    } else {
        // If the file doesn't exist, create a new one
        std::ofstream outfile(name, std::ios::binary);
        if (!outfile) {
            throw std::runtime_error("Failed to create a new file");
        }
        outfile.close();
        this->numPages = 0;  // Start with 0 pages for a new file
    }
}

void HeapFile::insertTuple(const Tuple &t) {
    // Check if the file is empty (no pages yet)
    if (this->numPages == 0) {
        // If no pages exist, initialize the first page
        this->numPages++;
        std::unique_ptr<Page> newPage = std::make_unique<Page>(); // New empty page
        HeapPage heapPage(*newPage, this->td); // Heap wrapper of the new page
        heapPage.insertTuple(t); // Insert the tuple into the new page
        this->writePage(*newPage, 0); // Write the page to disk at page 0
    } else {
        // If there is at least 1 page, read the last page and attempt to insert the tuple
        std::unique_ptr<Page> lastPage = std::make_unique<Page>();
        this->readPage(*lastPage, this->numPages - 1); // Read the last page
        HeapPage heapPage(*lastPage, this->td);

        if (heapPage.insertTuple(t)) {
            this->writePage(*lastPage, this->numPages - 1); // Write the page back to disk
        } else {
            // If the last page is full, create a new page and insert the tuple there
            this->numPages++;
            std::unique_ptr<Page> newPage = std::make_unique<Page>();
            HeapPage newHeapPage(*newPage, this->td);
            if (!newHeapPage.insertTuple(t)) {
                throw std::runtime_error("HeapFile::insertTuple() failed when the last page is full");
            }
            this->writePage(*newPage, this->numPages - 1); // Write the new page
        }
    }
}

void HeapFile::deleteTuple(const Iterator &it) {
    // Read the page where the tuple is located
    std::unique_ptr<Page> page = std::make_unique<Page>();
    this->readPage(*page, it.page);
    HeapPage heapPage(*page, this->td);
    heapPage.deleteTuple(it.slot); // Delete the tuple from the page
    this->writePage(*page, it.page); // Write the page back to disk
}

Tuple HeapFile::getTuple(const Iterator &it) const {
    // Read the page where the tuple is located
    std::unique_ptr<Page> page = std::make_unique<Page>();
    this->readPage(*page, it.page);
    HeapPage heapPage(*page, this->td);
    return heapPage.getTuple(it.slot); // Get and return the tuple
}

void HeapFile::next(Iterator &it) const {
    // Read the current page
    std::unique_ptr<Page> page = std::make_unique<Page>();
    this->readPage(*page, it.page);
    HeapPage heapPage(*page, this->td);
    heapPage.next(it.slot); // Move to the next slot in the current page

    // If we reach the end of the page, move to the next page
    while (it.slot == heapPage.end() && it.page < this->numPages) {
        it.page++;
        if (it.page == this->numPages) { // No more pages left
            it.slot = 0; // End of the file
            return;
        }
        // Load the next page and move to the first occupied slot
        std::unique_ptr<Page> nextPage = std::make_unique<Page>();
        this->readPage(*nextPage, it.page);
        HeapPage nextHeapPage(*nextPage, this->td);
        it.slot = nextHeapPage.begin();
    }
}

Iterator HeapFile::begin() const {
    // Iterate through all pages and return the first non-empty page
    for (size_t page = 0; page < this->numPages; page++) {
        std::unique_ptr<Page> p = std::make_unique<Page>();
        this->readPage(*p, page);
        HeapPage heapPage(*p, this->td);
        if (heapPage.begin() != heapPage.end()) { // If the page is not empty
            return {*this, page, heapPage.begin()};
        }
    }
    // If all pages are empty, return the end iterator
    return {*this, numPages, 0};
}

Iterator HeapFile::end() const {
    // Return the end iterator
    return {*this, numPages, 0};
}
