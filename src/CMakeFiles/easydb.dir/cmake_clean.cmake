file(REMOVE_RECURSE
  "libeasydb.a"
  "libeasydb.pdb"
)

# Per-language clean rules from dependency scanning.
foreach(lang CXX)
  include(CMakeFiles/easydb.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
