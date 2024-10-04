#include <db/DbFile.hpp>
#include <stdexcept>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

using namespace db;

const TupleDesc &DbFile::getTupleDesc() const { return td; }

DbFile::DbFile(const std::string &name, const TupleDesc &td) : name(name), td(td) {
  // TODO pa2: open file and initialize numPages
  // Hint: use open, fstat
  int fd = open(name.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    throw std::runtime_error("Failed to open the file");
  }

  struct stat fileStat;
  if (fstat(fd, &fileStat) < 0) {
    throw std::runtime_error("Failed to retrieve file stats");
  }

  numPages = fileStat.st_size / DEFAULT_PAGE_SIZE;

  if (numPages == 0) {
    numPages = 1;  // Ensure the file has at least one page
  }
  close(fd);
}


DbFile::~DbFile() {
  // TODO pa2: close file
  // Hind: use close
}

const std::string &DbFile::getName() const { return name; }

void DbFile::readPage(Page &page, const size_t id) const {
  reads.push_back(id);
  // TODO pa2: read page
  // Hint: use pread
  int fd = open(name.c_str(), O_RDONLY);
  if (fd < 0) {
    throw std::runtime_error("Failed to open the file for reading");
  }

  // Calculate the offset of the page in the file
  size_t offset = id * DEFAULT_PAGE_SIZE;
  if (pread(fd, page.data(), DEFAULT_PAGE_SIZE, offset) < 0) {
    throw std::runtime_error("Failed to read page from the file");
  }

  close(fd);
}

void DbFile::writePage(const Page &page, const size_t id) const {
  writes.push_back(id);
  // TODO pa2: write page
  // Hint: use pwrite
  int fd = open(name.c_str(), O_WRONLY);
  if (fd < 0) {
    throw std::runtime_error("Failed to open the file for writing");
  }

  // Calculate the offset of the page in the file
  size_t offset = id * DEFAULT_PAGE_SIZE;
  if (pwrite(fd, page.data(), DEFAULT_PAGE_SIZE, offset) < 0) {
    throw std::runtime_error("Failed to write page to the file");
  }

  close(fd);
}

const std::vector<size_t> &DbFile::getReads() const { return reads; }

const std::vector<size_t> &DbFile::getWrites() const { return writes; }

void DbFile::insertTuple(const Tuple &t) { throw std::runtime_error("Not implemented"); }

void DbFile::deleteTuple(const Iterator &it) { throw std::runtime_error("Not implemented"); }

Tuple DbFile::getTuple(const Iterator &it) const { throw std::runtime_error("Not implemented"); }

void DbFile::next(Iterator &it) const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::begin() const { throw std::runtime_error("Not implemented"); }

Iterator DbFile::end() const { throw std::runtime_error("Not implemented"); }

size_t DbFile::getNumPages() const { return numPages; }
