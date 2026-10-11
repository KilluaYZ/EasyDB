add_library(
        easydb_buffer
        OBJECT
        buffer_pool_manager.cpp
        lru_replacer.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_buffer>
        PARENT_SCOPE)
