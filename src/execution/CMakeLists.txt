add_library(
    easydb_execution
    OBJECT
    execution_manager.cpp
    execution_sort.cpp
    executor_index_scan.cpp
    executor_merge_join.cpp
    executor_projection.cpp
    executor_seq_scan.cpp
    executor_nestedloop_join.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_execution>
    PARENT_SCOPE)
