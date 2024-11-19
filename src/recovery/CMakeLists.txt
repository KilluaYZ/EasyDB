add_library(
    easydb_recovery 
    OBJECT
    log_manager.cpp
    log_recovery.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_recovery>
    PARENT_SCOPE)
