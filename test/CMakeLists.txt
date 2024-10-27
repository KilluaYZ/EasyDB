include(GoogleTest)
file(GLOB_RECURSE EASYDB_TEST_SOURCES "${PROJECT_SOURCE_DIR}/test/*/*test.cpp")
#file(GLOB_RECURSE EASYDB_TEST_HEADERS "${PROJECT_SOURCE_DIR}/test/*/*test.hpp")
include_directories(${PROJECT_SOURCE_DIR}/test/include)

# #####################################################################################################################
# MAKE TARGETS
# #####################################################################################################################

# #########################################
# "make check-tests"
# #########################################
foreach (easydb_test_source ${EASYDB_TEST_SOURCES})
    get_filename_component(easydb_test_filename ${easydb_test_source} NAME)
    string(REPLACE ".cpp" "" easydb_test_name ${easydb_test_filename})
    add_executable(${easydb_test_name} ${easydb_test_source}
            include/common/tb_reader.h
            include/common/errors.h
            include/common/common.h
            include/common/defs.h
            include/common/record.h)
endforeach ()