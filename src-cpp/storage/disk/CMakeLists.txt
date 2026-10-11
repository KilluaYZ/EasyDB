add_library(
    easydb_storage_disk 
    OBJECT
    disk_manager.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_storage_disk>
    PARENT_SCOPE)
