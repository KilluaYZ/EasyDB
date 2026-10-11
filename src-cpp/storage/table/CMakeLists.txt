add_library(
    easydb_storage_table
    OBJECT
    tuple.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_storage_table>
    PARENT_SCOPE)
