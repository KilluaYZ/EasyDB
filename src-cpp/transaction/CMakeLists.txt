add_library(
    easydb_transaction
    OBJECT
    transaction_manager.cpp
    )

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_transaction>
    PARENT_SCOPE)
