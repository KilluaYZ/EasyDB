add_library(
    easydb_record
    OBJECT
    rm_file_handle.cpp
    rm_scan.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_record>
    PARENT_SCOPE)
