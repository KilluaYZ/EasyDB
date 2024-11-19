set(SOURCES transaction_manager.cpp)
add_library(easydb_transaction STATIC ${SOURCES})
target_link_libraries(easydb_transaction system recovery pthread)