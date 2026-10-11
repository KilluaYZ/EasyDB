add_library(
    easydb_storage_index
    OBJECT
    ix_index_handle.cpp
    ix_extendible_hash_index_handle.cpp
    ix_scan.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_storage_index>
    PARENT_SCOPE)
