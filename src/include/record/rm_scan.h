/*-------------------------------------------------------------------------
 *
 * EasyDB
 *
 * rm_scan.h
 *
 * Identification: src/include/record/rm_scan.h
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

  virtual void Next() = 0;

  virtual bool IsEnd() const = 0;

  virtual RID rid() const = 0;
};

class RmFileHandle;

class RmScan : public RecScan {
  const RmFileHandle *file_handle_;
  RID rid_;

 public:
  RmScan(const RmFileHandle *file_handle);

  void Next() override;

  bool IsEnd() const override;

  RID rid() const override;
};

}  // namespace easydb
