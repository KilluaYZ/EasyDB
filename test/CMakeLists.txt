cmake_minimum_required(VERSION 3.10)

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
add_custom_target(build-tests COMMAND ${CMAKE_CTEST_COMMAND} --show-only)
add_custom_target(check-tests COMMAND ${CMAKE_CTEST_COMMAND} --verbose)

foreach (easydb_test_source ${EASYDB_TEST_SOURCES})
    get_filename_component(easydb_test_filename ${easydb_test_source} NAME)
    string(REPLACE ".cpp" "" easydb_test_name ${easydb_test_filename})
    add_executable(${easydb_test_name} ${easydb_test_source})

    add_dependencies(build-tests ${easydb_test_name})
    add_dependencies(check-tests ${easydb_test_name})

    # gtest_discover_tests(${easydb_test_name}
    # EXTRA_ARGS
    # --gtest_output=xml:${CMAKE_BINARY_DIR}/test/${easydb_test_name}.xml
    # --gtest_catch_exceptions=0
    # DISCOVERY_TIMEOUT 120
    # PROPERTIES
    # TIMEOUT 120
    # )

    target_link_libraries(${easydb_test_name} easydb gtest gmock_main)

    # Set test target properties and dependencies.
    set_target_properties(${easydb_test_name}
            PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/test"
            COMMAND ${easydb_test_name}
            )
endforeach ()