if(EXISTS "D:/my project/bustub/build.mingw/test/txn_executor_test.exe")
  if(NOT EXISTS "D:/my project/bustub/build.mingw/test/txn_executor_test[1]_tests.cmake" OR
     NOT "D:/my project/bustub/build.mingw/test/txn_executor_test[1]_tests.cmake" IS_NEWER_THAN "D:/my project/bustub/build.mingw/test/txn_executor_test.exe" OR
     NOT "D:/my project/bustub/build.mingw/test/txn_executor_test[1]_tests.cmake" IS_NEWER_THAN "${CMAKE_CURRENT_LIST_FILE}")
    include("C:/Program Files/CMake/share/cmake-4.2/Modules/GoogleTestAddTests.cmake")
    gtest_discover_tests_impl(
      TEST_EXECUTABLE [==[D:/my project/bustub/build.mingw/test/txn_executor_test.exe]==]
      TEST_EXECUTOR [==[]==]
      TEST_WORKING_DIR [==[D:/my project/bustub/build.mingw/test]==]
      TEST_EXTRA_ARGS [==[--gtest_output=xml:D:/my project/bustub/build.mingw/test/txn_executor_test.xml;--gtest_catch_exceptions=0]==]
      TEST_PROPERTIES [==[TIMEOUT;120]==]
      TEST_PREFIX [==[]==]
      TEST_SUFFIX [==[]==]
      TEST_FILTER [==[]==]
      NO_PRETTY_TYPES [==[FALSE]==]
      NO_PRETTY_VALUES [==[FALSE]==]
      TEST_LIST [==[txn_executor_test_TESTS]==]
      CTEST_FILE [==[D:/my project/bustub/build.mingw/test/txn_executor_test[1]_tests.cmake]==]
      TEST_DISCOVERY_TIMEOUT [==[120]==]
      TEST_DISCOVERY_EXTRA_ARGS [==[]==]
      TEST_XML_OUTPUT_DIR [==[]==]
    )
  endif()
  include("D:/my project/bustub/build.mingw/test/txn_executor_test[1]_tests.cmake")
else()
  add_test(txn_executor_test_NOT_BUILT txn_executor_test_NOT_BUILT)
endif()
