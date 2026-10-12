#include "storage/easystore/recovery/undo_manager.h"

#include <algorithm>
#include <array>
#include <fstream>

#include "storage/disk/disk_manager.h"
#include "storage/easystore/recovery/log_manager.h"

namespace easydb {

UndoManager::UndoManager(DiskManager *disk_manager)
    : disk_manager_(disk_manager),
      path_(disk_manager->GetDatabasePath() / UNDO_LOG_FILE_NAME) {
  std::ofstream create(path_, std::ios::binary | std::ios::app);
}

auto UndoManager::Append(LogRecord *record) -> lsn_t {
  std::scoped_lock lock(latch_);
  std::vector<char> bytes(record->log_tot_len_);
  record->serialize(bytes.data());
  std::ofstream output(path_, std::ios::binary | std::ios::app);
  output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  output.flush();
  return record->lsn_;
}

auto UndoManager::ReadAll() const
    -> std::vector<std::unique_ptr<LogRecord>> {
  std::scoped_lock lock(latch_);
  std::vector<std::unique_ptr<LogRecord>> records;
  std::ifstream input(path_, std::ios::binary);
  if (!input.is_open()) return records;
  while (true) {
    std::array<char, LOG_HEADER_SIZE> header{};
    input.read(header.data(), static_cast<std::streamsize>(header.size()));
    if (input.gcount() != static_cast<std::streamsize>(header.size())) break;
    LogRecord header_record;
    header_record.deserialize(header.data());
    if (header_record.log_tot_len_ < LOG_HEADER_SIZE ||
        header_record.log_type_ > DELETE) {
      break;
    }
    std::vector<char> bytes(header_record.log_tot_len_);
    std::copy(header.begin(), header.end(), bytes.begin());
    input.read(bytes.data() + LOG_HEADER_SIZE,
               static_cast<std::streamsize>(bytes.size() - LOG_HEADER_SIZE));
    if (input.gcount() !=
        static_cast<std::streamsize>(bytes.size() - LOG_HEADER_SIZE)) {
      break;
    }
    auto record = std::make_unique<TupleLogRecord>();
    record->deserialize(bytes.data());
    records.emplace_back(std::move(record));
  }
  return records;
}

void UndoManager::Purge(const std::unordered_set<lsn_t> &obsolete_lsns) {
  if (obsolete_lsns.empty()) return;
  std::scoped_lock lock(latch_);
  std::ifstream input(path_, std::ios::binary);
  if (!input.is_open()) return;
  const auto temporary = path_.string() + ".purge";
  std::ofstream output(temporary, std::ios::binary | std::ios::trunc);
  while (true) {
    std::array<char, LOG_HEADER_SIZE> header{};
    input.read(header.data(), static_cast<std::streamsize>(header.size()));
    if (input.gcount() != static_cast<std::streamsize>(header.size())) break;
    LogRecord header_record;
    header_record.deserialize(header.data());
    if (header_record.log_tot_len_ < LOG_HEADER_SIZE) break;
    std::vector<char> bytes(header_record.log_tot_len_);
    std::copy(header.begin(), header.end(), bytes.begin());
    input.read(bytes.data() + LOG_HEADER_SIZE,
               static_cast<std::streamsize>(bytes.size() - LOG_HEADER_SIZE));
    if (input.gcount() !=
        static_cast<std::streamsize>(bytes.size() - LOG_HEADER_SIZE)) {
      break;
    }
    if (obsolete_lsns.count(header_record.lsn_) == 0) {
      output.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
    }
  }
  output.close();
  input.close();
  std::filesystem::rename(temporary, path_);
}

}  // namespace easydb
