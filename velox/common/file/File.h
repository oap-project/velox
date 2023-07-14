/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Abstraction of a simplified file interface.
//
// Implementations are available in this file for local disk and in-memory.
//
// We implement only a small subset of the normal file operations, namely
// Append for writing data and PRead for reading data.
//
// All functions are not threadsafe -- external locking is required, even
// for const member functions.

#pragma once

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <string_view>

#include <folly/Range.h>
#include <folly/futures/Future.h>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox {

// A read-only file.  All methods in this object should be thread safe.
class ReadFile {
 public:
  struct Segment {
    // offset in the file to start reading from.
    uint64_t offset;

    // Buffer to save the data to. buffer.size() bytes will be read from the
    // file and stored in it.
    folly::Range<char*> buffer;

    // optional: label for caching purposes.
    // It should contain a label stating the column name or whatever logical
    // identifier of the segment of the file that is being read. The reader can
    // use this information to decide whether or not to cache this information
    // if this is a frequently accessed element across the table.
    std::string_view label;
  };

  virtual ~ReadFile() = default;

  // Reads the data at [offset, offset + length) into the provided pre-allocated
  // buffer 'buf'. The bytes are returned as a string_view pointing to 'buf'.
  //
  // This method should be thread safe.
  virtual std::string_view
  pread(uint64_t offset, uint64_t length, void* FOLLY_NONNULL buf) const = 0;

  // Same as above, but returns owned data directly.
  //
  // This method should be thread safe.
  virtual std::string pread(uint64_t offset, uint64_t length) const;

  // Reads starting at 'offset' into the memory referenced by the
  // Ranges in 'buffers'. The buffers are filled left to right. A
  // buffer with nullptr data will cause its size worth of bytes to be skipped.
  //
  // This method should be thread safe.
  virtual uint64_t preadv(
      uint64_t /*offset*/,
      const std::vector<folly::Range<char*>>& /*buffers*/) const;

  // Vectorized read API. Implementations can coalesce and parallelize.
  // It is different to the preadv above because we can add a label to the
  // segment and because the offsets don't need to be sorted.
  // In the preadv above offsets of buffers are always
  //
  // This method should be thread safe.
  virtual void preadv(const std::vector<Segment>& segments) const;

  // Like preadv but may execute asynchronously and returns the read
  // size or exception via SemiFuture. Use hasPreadvAsync() to check
  // if the implementation is in fact asynchronous.
  //
  // This method should be thread safe.
  virtual folly::SemiFuture<uint64_t> preadvAsync(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const {
    try {
      return folly::SemiFuture<uint64_t>(preadv(offset, buffers));
    } catch (const std::exception& e) {
      return folly::makeSemiFuture<uint64_t>(e);
    }
  }

  // Returns true if preadvAsync has a native implementation that is
  // asynchronous. The default implementation is synchronous.
  virtual bool hasPreadvAsync() const {
    return false;
  }

  // Whether preads should be coalesced where possible. E.g. remote disk would
  // set to true, in-memory to false.
  virtual bool shouldCoalesce() const = 0;

  // Number of bytes in the file.
  virtual uint64_t size() const = 0;

  // An estimate for the total amount of memory *this uses.
  virtual uint64_t memoryUsage() const = 0;

  // The total number of bytes *this had been used to read since creation or
  // the last resetBytesRead. We sum all the |length| variables passed to
  // preads, not the actual amount of bytes read (which might be less).
  virtual uint64_t bytesRead() const {
    return bytesRead_;
  }

  virtual void resetBytesRead() {
    bytesRead_ = 0;
  }

  virtual std::string getName() const = 0;

  //
  // Get the natural size for reads.
  // @return the number of bytes that should be read at once
  //
  virtual uint64_t getNaturalReadSize() const = 0;

 protected:
  mutable std::atomic<uint64_t> bytesRead_ = 0;
};

// A write-only file. Nothing written to the file should be read back until it
// is closed.
class WriteFile {
 public:
  virtual ~WriteFile() = default;

  // Appends data to the end of the file.
  virtual void append(std::string_view data) = 0;

  // Flushes any local buffers, i.e. ensures the backing medium received
  // all data that has been appended.
  virtual void flush() = 0;

  // Close the file. Any cleanup (disk flush, etc.) will be done here.
  virtual void close() = 0;

  // Current file size, i.e. the sum of all previous Appends.
  virtual uint64_t size() const = 0;
};

// We currently do a simple implementation for the in-memory files
// that simply resizes a string as needed. If there ever gets used in
// a performance sensitive path we'd probably want to move to a Cord-like
// implementation for underlying storage.

// We don't provide registration functions for the in-memory files, as they
// aren't intended for any robust use needing a filesystem.

class InMemoryReadFile : public ReadFile {
 public:
  explicit InMemoryReadFile(std::string_view file) : file_(file) {}

  explicit InMemoryReadFile(std::string file)
      : ownedFile_(std::move(file)), file_(ownedFile_) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* FOLLY_NONNULL buf) const override;

  std::string pread(uint64_t offset, uint64_t length) const override;

  uint64_t size() const final {
    return file_.size();
  }

  uint64_t memoryUsage() const final {
    return size();
  }

  // Mainly for testing. Coalescing isn't helpful for in memory data.
  void setShouldCoalesce(bool shouldCoalesce) {
    shouldCoalesce_ = shouldCoalesce;
  }
  bool shouldCoalesce() const final {
    return shouldCoalesce_;
  }

  std::string getName() const override {
    return "<InMemoryReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    return 1024;
  }

 private:
  const std::string ownedFile_;
  const std::string_view file_;
  bool shouldCoalesce_ = false;
};

class InMemoryWriteFile final : public WriteFile {
 public:
  explicit InMemoryWriteFile(std::string* FOLLY_NONNULL file) : file_(file) {}

  void append(std::string_view data) final;
  void flush() final {}
  void close() final {}
  uint64_t size() const final;

 private:
  std::string* FOLLY_NONNULL file_;
};

// TODO zuochunwei
struct HeapMemoryMock {
  HeapMemoryMock() = default;
  explicit HeapMemoryMock(void* memory, size_t capacity)
      : memory_(memory), capacity_(capacity) {}

  void reset() {
    memory_ = nullptr;
    size_ = 0;
    capacity_ = 0;
  }

  bool isValid() const {
    return memory_ != nullptr;
  }

  void write(const void* src, size_t len) {
    assert(len <= freeSize());
    memcpy(end(), src, len);
    size_ += len;
  }

  void read(void* dst, size_t len, size_t offset) {
    assert(offset + len <= size_);
    memcpy(dst, (char*)memory_ + offset, len);
  }

  auto size() const {
    return size_;
  }

  auto freeSize() const {
    return capacity_ - size_;
  }

  void* begin() {
    return memory_;
  }

  void* end() {
    return (char*)memory_ + size_;
  }

  void* memory_ = nullptr;
  size_t size_ = 0;
  size_t capacity_ = 0;
};

const size_t kHeapMemoryCapacity = 64 * 1024;

class HeapMemoryMockManager {
 public:
  static HeapMemoryMockManager& instance() {
    static HeapMemoryMockManager hmmm;
    return hmmm;
  }

  HeapMemoryMock alloc(size_t size) {
    HeapMemoryMock heapMemory;
    if (size_ + size <= kHeapMemoryCapacity) {
      heapMemory.memory_ = malloc(size);
      heapMemory.size_ = 0;
      heapMemory.capacity_ = size;
      size_ += size;
    }
    return heapMemory;
  }

  void free(HeapMemoryMock& heapMemory) {
    if (heapMemory.isValid()) {
      size_ -= heapMemory.size_;
      ::free(heapMemory.memory_);
      heapMemory.reset();
    }
  }

 private:
  std::atomic<std::size_t> size_;
};

inline HeapMemoryMock allocHeapMemory(size_t size) {
  return HeapMemoryMockManager::instance().alloc(size);
}

inline void freeHeapMemory(HeapMemoryMock& heapMemory) {
  HeapMemoryMockManager::instance().free(heapMemory);
}

class HeapMemoryReadFile : public ReadFile {
 public:
  explicit HeapMemoryReadFile(HeapMemoryMock& heapMemory)
      : heapMemory_(heapMemory) {}

  std::string_view pread(
      uint64_t offset,
      uint64_t length,
      void* FOLLY_NONNULL buf) const override {
    bytesRead_ += length;
    heapMemory_.read(buf, length, offset);
    return {static_cast<char*>(buf), length};
  }

  std::string pread(uint64_t offset, uint64_t length) const override {
    bytesRead_ += length;
    assert(offset + lenght <= heapMemory_.size());
    return std::string((char*)heapMemory_.begin() + offset, length);
  }

  uint64_t size() const final {
    return heapMemory_.size();
  }

  uint64_t memoryUsage() const final {
    return size();
  }

  // Mainly for testing. Coalescing isn't helpful for in memory data.
  void setShouldCoalesce(bool shouldCoalesce) {
    shouldCoalesce_ = shouldCoalesce;
  }
  bool shouldCoalesce() const final {
    return shouldCoalesce_;
  }

  std::string getName() const override {
    return "<HeapMemoryReadFile>";
  }

  uint64_t getNaturalReadSize() const override {
    return 1024;
  }

 private:
  HeapMemoryMock& heapMemory_;
  bool shouldCoalesce_ = false;
};

class HeapMemoryWriteFile final : public WriteFile {
 public:
  explicit HeapMemoryWriteFile(HeapMemoryMock& heapMemory)
      : heapMemory_(heapMemory) {}

  void append(std::string_view data) final {
    heapMemory_.write(data.data(), data.length());
  }

  void flush() final {}

  void close() final {}

  uint64_t size() const final {
    return heapMemory_.size_;
  }

 private:
  HeapMemoryMock& heapMemory_;
};

// Current implementation for the local version is quite simple (e.g. no
// internal arenaing), as local disk writes are expected to be cheap. Local
// files match against any filepath starting with '/'.

class LocalReadFile final : public ReadFile {
 public:
  explicit LocalReadFile(std::string_view path);

  explicit LocalReadFile(int32_t fd);

  ~LocalReadFile();

  std::string_view
  pread(uint64_t offset, uint64_t length, void* FOLLY_NONNULL buf) const final;

  uint64_t size() const final;

  uint64_t preadv(
      uint64_t offset,
      const std::vector<folly::Range<char*>>& buffers) const final;

  uint64_t memoryUsage() const final;

  bool shouldCoalesce() const final {
    return false;
  }

  std::string getName() const override {
    if (path_.empty()) {
      return "<LocalReadFile>";
    }
    return path_;
  }

  uint64_t getNaturalReadSize() const override {
    return 10 << 20;
  }

 private:
  void preadInternal(uint64_t offset, uint64_t length, char* FOLLY_NONNULL pos)
      const;

  std::string path_;
  int32_t fd_;
  long size_;
};

class LocalWriteFile final : public WriteFile {
 public:
  // An error is thrown is a file already exists at |path|.
  explicit LocalWriteFile(std::string_view path);
  ~LocalWriteFile();

  void append(std::string_view data) final;
  void flush() final;
  void close() final;
  uint64_t size() const final;

 private:
  FILE* FOLLY_NONNULL file_;
  mutable long size_;
  bool closed_{false};
};

} // namespace facebook::velox
