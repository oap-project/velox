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

#include "velox/vector/arrow/Bridge.h"

#include <arrow/c/bridge.h> // @manual
#include <arrow/record_batch.h>
#include <arrow/table.h> // @manual
#include <parquet/arrow/writer.h> // @manual
#include "velox/dwio/parquet/writer/Writer.h"

namespace facebook::velox::parquet {

// Utility for capturing Arrow output into a DataBuffer.
class ArrowDataBufferSink : public arrow::io::OutputStream {
 public:
  explicit ArrowDataBufferSink(
      std::unique_ptr<dwio::common::DataSink> sink,
      memory::MemoryPool& pool,
      double growRatio = 1)
      : sink_(std::move(sink)), growRatio_(growRatio), buffer_(pool) {}

  arrow::Status Write(const std::shared_ptr<arrow::Buffer>& data) override {
    auto requestCapacity = buffer_.size() + data->size();
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(
        buffer_.size(),
        reinterpret_cast<const char*>(data->data()),
        data->size());
    return arrow::Status::OK();
  }

  arrow::Status Write(const void* data, int64_t nbytes) override {
    auto requestCapacity = buffer_.size() + nbytes;
    if (requestCapacity > buffer_.capacity()) {
      buffer_.reserve(growRatio_ * (requestCapacity));
    }
    buffer_.append(buffer_.size(), reinterpret_cast<const char*>(data), nbytes);
    return arrow::Status::OK();
  }

  arrow::Status Flush() override {
    bytesFlushed_ += buffer_.size();
    sink_->write(std::move(buffer_));
    return arrow::Status::OK();
  }

  arrow::Result<int64_t> Tell() const override {
    return bytesFlushed_ + buffer_.size();
  }

  arrow::Status Close() override {
    ARROW_RETURN_NOT_OK(Flush());
    sink_->close();
    return arrow::Status::OK();
  }

  bool closed() const override {
    return sink_->isClosed();
  }

  dwio::common::DataBuffer<char>& dataBuffer() {
    return buffer_;
  }

 private:
  std::unique_ptr<dwio::common::DataSink> sink_;
  const double growRatio_ = 1;
  dwio::common::DataBuffer<char> buffer_;
  int64_t bytesFlushed_ = 0;
};

struct ArrowContext {
  std::unique_ptr<::parquet::arrow::FileWriter> writer;
  std::shared_ptr<arrow::Schema> schema;
  std::shared_ptr<::parquet::WriterProperties> properties;
  // columns, Arrays
  std::vector<std::vector<std::shared_ptr<arrow::Array>>> stagingChunks;
};

::parquet::Compression::type getArrowParquetCompression(
    dwio::common::CompressionKind compression) {
  if (compression == dwio::common::CompressionKind_SNAPPY) {
    return ::parquet::Compression::SNAPPY;
  } else if (compression == dwio::common::CompressionKind_GZIP) {
    return ::parquet::Compression::GZIP;
  } else if (compression == dwio::common::CompressionKind_ZSTD) {
    return ::parquet::Compression::ZSTD;
  } else if (compression == dwio::common::CompressionKind_NONE) {
    return ::parquet::Compression::UNCOMPRESSED;
  } else {
    VELOX_FAIL("Unsupported compression {}", compression);
  }
}

std::shared_ptr<::parquet::WriterProperties> getArrowParquetWriterOptions(
    const parquet::WriterOptions& options) {
  auto builder = ::parquet::WriterProperties::Builder();
  ::parquet::WriterProperties::Builder* properties = &builder;
  if (!options.enableDictionary) {
    properties = properties->disable_dictionary();
  }
  properties =
      properties->compression(getArrowParquetCompression(options.compression));
  properties = properties->data_pagesize(options.dataPageSize);
  properties = properties->max_row_group_length(options.maxRowGroupLength);
  return properties->build();
}

Writer::Writer(
    std::unique_ptr<dwio::common::DataSink> sink,
    const WriterOptions& options,
    std::shared_ptr<memory::MemoryPool> pool,
    std::shared_ptr<arrow::Schema> schema)
    : rowsInRowGroup_(options.rowsInRowGroup),
      bytesInRowGroup_(options.bytesInRowGroup),
      bufferGrowRatio_(options.bufferGrowRatio),
      pool_(std::move(pool)),
      generalPool_{pool_->addLeafChild(".general")},
      stream_(std::make_shared<ArrowDataBufferSink>(
          std::move(sink),
          *generalPool_,
          bufferGrowRatio_)) {
  arrowContext_ = std::make_shared<ArrowContext>();
  arrowContext_->properties = getArrowParquetWriterOptions(options);
  arrowContext_->schema = schema;

  if (arrowContext_->schema) {
    // If the input iterator is empty, the writer will do nothing and build a
    // empty file. We should at least write the parquet magic header so the
    // reader can regonize it is a valid parquet file. So, we initialize the
    // writer at first even there is no data.
    auto arrowProperties = ::parquet::ArrowWriterProperties::Builder().build();
    PARQUET_ASSIGN_OR_THROW(
        arrowContext_->writer,
        ::parquet::arrow::FileWriter::Open(
            *arrowContext_->schema.get(),
            arrow::default_memory_pool(),
            stream_,
            arrowContext_->properties,
            arrowProperties));
  }
}

Writer::Writer(
    std::unique_ptr<dwio::common::DataSink> sink,
    const WriterOptions& options,
    std::shared_ptr<arrow::Schema> schema)
    : Writer{
          std::move(sink),
          options,
          options.memoryPool->addAggregateChild(fmt::format(
              "writer_node_{}",
              folly::to<std::string>(folly::Random::rand64())))} {}

void Writer::flush() {
  if (stagingRows_ > 0) {
    if (!arrowContext_->writer) {
      auto arrowProperties =
          ::parquet::ArrowWriterProperties::Builder().build();
      PARQUET_ASSIGN_OR_THROW(
          arrowContext_->writer,
          ::parquet::arrow::FileWriter::Open(
              *arrowContext_->schema.get(),
              arrow::default_memory_pool(),
              stream_,
              arrowContext_->properties,
              arrowProperties));
    }

    auto fields = arrowContext_->schema->fields();
    std::vector<std::shared_ptr<arrow::ChunkedArray>> chunks;
    for (int colIdx = 0; colIdx < fields.size(); colIdx++) {
      auto dataType = fields.at(colIdx)->type();
      auto chunk =
          arrow::ChunkedArray::Make(
              std::move(arrowContext_->stagingChunks.at(colIdx)), dataType)
              .ValueOrDie();
      chunks.push_back(chunk);
    }
    auto table = arrow::Table::Make(
        arrowContext_->schema, std::move(chunks), stagingRows_);
    PARQUET_THROW_NOT_OK(
        arrowContext_->writer->WriteTable(*table, rowsInRowGroup_));
    PARQUET_THROW_NOT_OK(stream_->Flush());
    for (auto& chunk : arrowContext_->stagingChunks) {
      chunk.clear();
    }
    stagingRows_ = 0;
    stagingBytes_ = 0;
  }
}

/**
 * This method would cache input `ColumnarBatch` to make the size of row group
 * big. It would flush when:
 * - the cached numRows bigger than `rowsInRowGroup_`
 * - the cached bytes bigger than `bytesInRowGroup_`
 *
 * This method assumes each input `ColumnarBatch` have same schema.
 */
void Writer::write(const VectorPtr& data) {
  ArrowArray array;
  exportToArrow(data, array, generalPool_.get());
  std::shared_ptr<arrow::RecordBatch> recordBatch;
  if (arrowContext_->schema) {
    PARQUET_ASSIGN_OR_THROW(
        recordBatch, arrow::ImportRecordBatch(&array, arrowContext_->schema));
  } else {
    ArrowSchema schema;
    exportToArrow(data, schema);
    PARQUET_ASSIGN_OR_THROW(
        recordBatch, arrow::ImportRecordBatch(&array, &schema));
  }

  if (arrowContext_->stagingChunks.empty()) {
    if (!arrowContext_->schema) {
      arrowContext_->schema = recordBatch->schema();
    }

    for (int colIdx = 0; colIdx < arrowContext_->schema->num_fields();
         colIdx++) {
      arrowContext_->stagingChunks.push_back(
          std::vector<std::shared_ptr<arrow::Array>>());
    }
  }

  auto bytes = data->estimateFlatSize();
  auto numRows = data->size();
  if (stagingBytes_ + bytes > bytesInRowGroup_ ||
      stagingRows_ + numRows > rowsInRowGroup_) {
    flush();
  }

  for (int colIdx = 0; colIdx < recordBatch->num_columns(); colIdx++) {
    auto array = recordBatch->column(colIdx);
    arrowContext_->stagingChunks.at(colIdx).push_back(array);
  }
  stagingRows_ += numRows;
  stagingBytes_ += bytes;
}

bool Writer::isArrowCodecAvailable(dwio::common::CompressionKind compression) {
  return arrow::util::Codec::IsAvailable(
      getArrowParquetCompression(compression));
}

void Writer::newRowGroup(int32_t numRows) {
  PARQUET_THROW_NOT_OK(arrowContext_->writer->NewRowGroup(numRows));
}

void Writer::close() {
  flush();

  if (arrowContext_->writer) {
    PARQUET_THROW_NOT_OK(arrowContext_->writer->Close());
    arrowContext_->writer.reset();
  }

  PARQUET_THROW_NOT_OK(stream_->Close());

  arrowContext_->stagingChunks.clear();
}

parquet::WriterOptions getParquetOptions(
    const dwio::common::WriterOptions& options) {
  parquet::WriterOptions parquetOptions;
  parquetOptions.memoryPool = options.memoryPool;
  return parquetOptions;
}

std::unique_ptr<dwio::common::Writer> ParquetWriterFactory::createWriter(
    std::unique_ptr<dwio::common::DataSink> sink,
    const dwio::common::WriterOptions& options) {
  auto parquetOptions = getParquetOptions(options);
  return std::make_unique<Writer>(std::move(sink), parquetOptions);
}

} // namespace facebook::velox::parquet
