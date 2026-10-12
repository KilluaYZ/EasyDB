#include "storage/easystore/table/clustered_table_handle.h"

#include <cstring>

namespace easydb {

auto ClusteredTableHandle::MakeKey(const Tuple &tuple) const
    -> std::vector<char> {
  const auto key = tuple.KeyFromTuple(*schema_, *key_schema_, key_attrs_);
  return {key.GetData(), key.GetData() + key.GetLength()};
}

auto ClusteredTableHandle::Insert(const Tuple &tuple) -> bool {
  return Insert(tuple, TupleMeta{});
}

auto ClusteredTableHandle::Insert(const Tuple &tuple, const TupleMeta &meta)
    -> bool {
  if (index_ == nullptr || schema_ == nullptr || key_schema_ == nullptr) {
    return false;
  }
  return index_->Insert(MakeKey(tuple),
                        {tuple.GetData(), tuple.GetData() + tuple.GetLength()},
                        meta);
}

auto ClusteredTableHandle::Get(const Tuple &key_tuple) -> std::optional<Tuple> {
  const auto record = GetRecord(key_tuple);
  if (!record.has_value()) return std::nullopt;
  return Tuple(record->tuple);
}

auto ClusteredTableHandle::GetRecord(const Tuple &key_tuple)
    -> std::optional<ClusteredLeafEntry> {
  if (index_ == nullptr || schema_ == nullptr || key_schema_ == nullptr) {
    return std::nullopt;
  }
  return index_->GetRecord(MakeKey(key_tuple));
}

auto ClusteredTableHandle::Update(const Tuple &key_tuple, const Tuple &tuple)
    -> bool {
  const auto record = GetRecord(key_tuple);
  if (!record.has_value()) return false;
  return Update(key_tuple, tuple, record->meta);
}

auto ClusteredTableHandle::Update(const Tuple &key_tuple, const Tuple &tuple,
                                  const TupleMeta &meta) -> bool {
  if (index_ == nullptr || schema_ == nullptr || key_schema_ == nullptr) {
    return false;
  }
  const auto old_key = MakeKey(key_tuple);
  const auto new_key = MakeKey(tuple);
  if (old_key != new_key) return false;
  return index_->Update(old_key,
                        {tuple.GetData(), tuple.GetData() + tuple.GetLength()},
                        meta);
}

auto ClusteredTableHandle::Delete(const Tuple &key_tuple) -> bool {
  if (index_ == nullptr || schema_ == nullptr || key_schema_ == nullptr) {
    return false;
  }
  return index_->Delete(MakeKey(key_tuple));
}

auto ClusteredTableHandle::Scan() -> std::vector<Tuple> {
  std::vector<Tuple> result;
  if (index_ == nullptr) return result;
  for (const auto &entry : index_->ScanAll()) {
    result.emplace_back(entry.tuple);
  }
  return result;
}

}  // namespace easydb
