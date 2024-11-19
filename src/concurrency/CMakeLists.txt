set(SOURCES lock_manager.cpp)
add_library(easydb_concurrency STATIC ${SOURCES})
target_link_libraries(easydb_concurrency system pthread)