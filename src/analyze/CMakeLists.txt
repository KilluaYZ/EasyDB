add_library(
    easydb_analyze
    OBJECT
    analyze.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_analyze>
    PARENT_SCOPE)
