add_library(
  easydb_catalog
  OBJECT
  column.cpp
  schema.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_catalog>
    PARENT_SCOPE)
