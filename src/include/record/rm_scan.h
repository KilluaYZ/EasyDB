/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_defs.h
 *
 * Identification: src/include/record/rm_defs.h
 *
 *-------------------------------------------------------------------------
 */

#pragma once
#include "common/rid.h"
#include "rm_defs.h"

namespace easydb {
class RecScan {
 public:
  virtual ~RecScan() = default;

  virtual void next() = 0;

  virtual bool is_end() const = 0;

  virtual RID rid() const = 0;
};

class RmFileHandle;

class RmScan : public RecScan {
  const RmFileHandle *file_handle_;
  RID rid_;

 public:
  RmScan(const RmFileHandle *file_handle);

  void next() override;

  bool is_end() const override;

  RID rid() const override;
};

}  // namespace easydb
