set(SOURCES log_manager.cpp log_recovery.cpp)
add_library(easydb_recovery STATIC ${SOURCES})
target_link_libraries(easydb_recovery system pthread)
