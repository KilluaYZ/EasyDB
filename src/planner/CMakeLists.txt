add_library(
    easydb_planner 
    OBJECT
    planner.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_planner>
    PARENT_SCOPE)
