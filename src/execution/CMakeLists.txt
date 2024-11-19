add_library(
    easydb_execution
    OBJECT
    execution_manager.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_execution>
    PARENT_SCOPE)
