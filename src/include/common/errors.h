//
// Created by ziyang on 24-10-24.
//

#ifndef ERRORS_HPP
#define ERRORS_HPP
/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 *
 *-------------------------------------------------------------------------
 */

#pragma once

#include <cerrno>
#include <cstring>
#include <string>
#include <vector>

namespace easydb {
class EASYDBError : public std::exception {
 public:
  EASYDBError() : _msg("Error: ") {}

  EASYDBError(const std::string &msg) : _msg("Error: " + msg) {}

  const char *what() const noexcept override { return _msg.c_str(); }

  int get_msg_len() { return _msg.length(); }

  std::string _msg;
};

class InternalError : public EASYDBError {
 public:
  InternalError(const std::string &msg) : EASYDBError(msg) {}
};

// PF errors
class UnixError : public EASYDBError {
 public:
  UnixError() : EASYDBError(strerror(errno)) {}
};

class FileNotOpenError : public EASYDBError {
 public:
  FileNotOpenError(int fd) : EASYDBError("Invalid file descriptor: " + std::to_string(fd)) {}
};

class FileNotClosedError : public EASYDBError {
 public:
  FileNotClosedError(const std::string &filename) : EASYDBError("File is opened: " + filename) {}
};

class FileExistsError : public EASYDBError {
 public:
  FileExistsError(const std::string &filename) : EASYDBError("File already exists: " + filename) {}
};

class FileNotFoundError : public EASYDBError {
 public:
  FileNotFoundError(const std::string &filename) : EASYDBError("File not found: " + filename) {}
};

// RM errors
class RecordNotFoundError : public EASYDBError {
 public:
  RecordNotFoundError(int page_no, int slot_no)
      : EASYDBError("Record not found: (" + std::to_string(page_no) + "," + std::to_string(slot_no) + ")") {}
};

class InvalidRecordSizeError : public EASYDBError {
 public:
  InvalidRecordSizeError(int record_size) : EASYDBError("Invalid record size: " + std::to_string(record_size)) {}
};

// IX errors
class InvalidColLengthError : public EASYDBError {
 public:
  InvalidColLengthError(int col_len) : EASYDBError("Invalid column length: " + std::to_string(col_len)) {}
};

class IndexEntryNotFoundError : public EASYDBError {
 public:
  IndexEntryNotFoundError() : EASYDBError("Index entry not found") {}
};

// SM errors
class DatabaseNotFoundError : public EASYDBError {
 public:
  DatabaseNotFoundError(const std::string &db_name) : EASYDBError("Database not found: " + db_name) {}
};

class DatabaseExistsError : public EASYDBError {
 public:
  DatabaseExistsError(const std::string &db_name) : EASYDBError("Database already exists: " + db_name) {}
};

class TableNotFoundError : public EASYDBError {
 public:
  TableNotFoundError(const std::string &tab_name) : EASYDBError("Table not found: " + tab_name) {}
};

class TableExistsError : public EASYDBError {
 public:
  TableExistsError(const std::string &tab_name) : EASYDBError("Table already exists: " + tab_name) {}
};

class ColumnNotFoundError : public EASYDBError {
 public:
  ColumnNotFoundError(const std::string &col_name) : EASYDBError("Column not found: " + col_name) {}
};

class IndexNotFoundError : public EASYDBError {
 public:
  IndexNotFoundError(const std::string &tab_name, const std::vector<std::string> &col_names) {
    _msg += "Index not found: " + tab_name + ".(";
    for (size_t i = 0; i < col_names.size(); ++i) {
      if (i > 0) _msg += ", ";
      _msg += col_names[i];
    }
    _msg += ")";
  }
};

class IndexExistsError : public EASYDBError {
 public:
  IndexExistsError(const std::string &tab_name, const std::vector<std::string> &col_names) {
    _msg += "Index already exists: " + tab_name + ".(";
    for (size_t i = 0; i < col_names.size(); ++i) {
      if (i > 0) _msg += ", ";
      _msg += col_names[i];
    }
    _msg += ")";
  }
};

// QL errors
class InvalidValueCountError : public EASYDBError {
 public:
  InvalidValueCountError() : EASYDBError("Invalid value count") {}
};

class StringOverflowError : public EASYDBError {
 public:
  StringOverflowError() : EASYDBError("String is too long") {}
};

class IncompatibleTypeError : public EASYDBError {
 public:
  IncompatibleTypeError(const std::string &lhs, const std::string &rhs)
      : EASYDBError("Incompatible type error: lhs " + lhs + ", rhs " + rhs) {}
};

class AmbiguousColumnError : public EASYDBError {
 public:
  AmbiguousColumnError(const std::string &col_name) : EASYDBError("Ambiguous column: " + col_name) {}
};

class PageNotExistError : public EASYDBError {
 public:
  PageNotExistError(const std::string &table_name, int page_no)
      : EASYDBError("Page " + std::to_string(page_no) + " in table " + table_name + "not exits") {}
};

class AggregationIllegalError : public EASYDBError {
 public:
  AggregationIllegalError() : EASYDBError("Select query with illegal aggregation conditions") {}
};
}  // namespace easydb
#endif  // ERRORS_HPP
