add_library(
    easydb_system 
    OBJECT
    sm_manager.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_system>
    PARENT_SCOPE)
