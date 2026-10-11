add_library(
    easydb_storage_page
    OBJECT
    table_page.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_storage_page>
    PARENT_SCOPE)
