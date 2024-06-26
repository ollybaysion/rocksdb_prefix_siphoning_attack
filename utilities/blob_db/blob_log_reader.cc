//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
#ifndef ROCKSDB_LITE

#include "utilities/blob_db/blob_log_reader.h"

#include <cstdio>
#include "rocksdb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"

namespace rocksdb {
namespace blob_db {

Reader::Reader(std::shared_ptr<Logger> info_log,
               unique_ptr<SequentialFileReader>&& _file)
    : info_log_(info_log), file_(std::move(_file)), buffer_(), next_byte_(0) {
  backing_store_.resize(kBlockSize);
}

Reader::~Reader() {}

Status Reader::ReadHeader(BlobLogHeader* header) {
  assert(file_.get() != nullptr);
  assert(next_byte_ == 0);
  Status status =
      file_->Read(BlobLogHeader::kHeaderSize, &buffer_, GetReadBuffer());
  next_byte_ += buffer_.size();
  if (!status.ok()) return status;

  if (buffer_.size() != BlobLogHeader::kHeaderSize) {
    return Status::IOError("EOF reached before file header");
  }

  status = header->DecodeFrom(buffer_);
  return status;
}

Status Reader::ReadRecord(BlobLogRecord* record, ReadLevel level,
                          uint64_t* blob_offset) {
  record->Clear();
  buffer_.clear();
  backing_store_[0] = '\0';

  Status status =
      file_->Read(BlobLogRecord::kHeaderSize, &buffer_, GetReadBuffer());
  next_byte_ += buffer_.size();
  if (!status.ok()) return status;
  if (buffer_.size() != BlobLogRecord::kHeaderSize) {
    return Status::IOError("EOF reached before record header");
  }

  status = record->DecodeHeaderFrom(buffer_);
  if (!status.ok()) {
    return status;
  }

  uint32_t header_crc = 0;
  uint32_t blob_crc = 0;
  size_t crc_data_size = BlobLogRecord::kHeaderSize - 2 * sizeof(uint32_t);
  header_crc = crc32c::Extend(header_crc, buffer_.data(), crc_data_size);

  uint64_t kb_size = record->GetKeySize() + record->GetBlobSize();
  if (blob_offset != nullptr) {
    *blob_offset = next_byte_ + record->GetKeySize();
  }
  switch (level) {
    case kReadHeader:
      file_->Skip(kb_size);
      next_byte_ += kb_size;
      __attribute__((fallthrough));

    case kReadHeaderKey:
      record->ResizeKeyBuffer(record->GetKeySize());
      status = file_->Read(record->GetKeySize(), &record->key_,
                           record->GetKeyBuffer());
      next_byte_ += record->key_.size();
      if (!status.ok()) return status;
      if (record->key_.size() != record->GetKeySize()) {
        return Status::IOError("EOF reached before key read");
      }

      header_crc =
          crc32c::Extend(header_crc, record->key_.data(), record->GetKeySize());
      header_crc = crc32c::Mask(header_crc);
      if (header_crc != record->header_cksum_) {
        return Status::Corruption("Record Checksum mismatch: header_cksum");
      }

      file_->Skip(record->GetBlobSize());
      next_byte_ += record->GetBlobSize();
      __attribute__((fallthrough));

    case kReadHeaderKeyBlob:
      record->ResizeKeyBuffer(record->GetKeySize());
      status = file_->Read(record->GetKeySize(), &record->key_,
                           record->GetKeyBuffer());
      next_byte_ += record->key_.size();
      if (!status.ok()) return status;
      if (record->key_.size() != record->GetKeySize()) {
        return Status::IOError("EOF reached before key read");
      }

      header_crc =
          crc32c::Extend(header_crc, record->key_.data(), record->GetKeySize());
      header_crc = crc32c::Mask(header_crc);
      if (header_crc != record->header_cksum_) {
        return Status::Corruption("Record Checksum mismatch: header_cksum");
      }

      record->ResizeBlobBuffer(record->GetBlobSize());
      status = file_->Read(record->GetBlobSize(), &record->blob_,
                           record->GetBlobBuffer());
      next_byte_ += record->blob_.size();
      if (!status.ok()) return status;
      if (record->blob_.size() != record->GetBlobSize()) {
        return Status::IOError("EOF reached during blob read");
      }

      blob_crc =
          crc32c::Extend(blob_crc, record->blob_.data(), record->blob_.size());
      blob_crc = crc32c::Mask(blob_crc);
      if (blob_crc != record->checksum_) {
        return Status::Corruption("Blob Checksum mismatch");
      }
  }
  return status;
}

}  // namespace blob_db
}  // namespace rocksdb
#endif  // ROCKSDB_LITE
