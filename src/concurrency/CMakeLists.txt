add_library(
    easydb_concurrency
    OBJECT
    lock_manager.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_concurrency>
    PARENT_SCOPE)
