add_library(
  easydb_common
  OBJECT
  config.cpp)

set(ALL_OBJECT_FILES
  ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:easydb_common>
  PARENT_SCOPE)
