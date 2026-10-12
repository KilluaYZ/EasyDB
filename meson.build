project(
  'EasyDB',
  ['c', 'cpp'],
  version: '0.1.0',
  meson_version: '>=1.2.0',
  default_options: [
    'cpp_std=gnu++17',
    'warning_level=3',
    'buildtype=debug',
    'b_lto=false',
  ],
)

cpp = meson.get_compiler('cpp')
add_project_arguments('-fpermissive', language: 'cpp')
threads_dep = dependency('threads')
nlohmann_json_dep = dependency('nlohmann_json')
readline_dep = dependency('readline')

if get_option('easydb_enable_asan')
  asan_args = ['-fsanitize=address', '-fno-omit-frame-pointer',
               '-fno-optimize-sibling-calls']
  add_project_arguments(asan_args, language: ['c', 'cpp'])
  add_project_link_arguments(asan_args, language: ['c', 'cpp'])
endif

src_inc = include_directories('src', 'src/include', 'deps')
test_inc = include_directories('test/include')

murmur3 = static_library(
  'easydb_murmur3',
  'deps/murmur3/MurmurHash3.cpp',
  include_directories: src_inc,
  install: false,
)

bison = find_program('bison')
flex = find_program('flex')
parser_sources = custom_target(
  'easydb_parser_sources',
  input: 'src/parser/yacc.y',
  output: ['yacc.tab.cpp', 'yacc.tab.h'],
  command: [bison, '-d', '--defines=@OUTPUT1@', '-o', '@OUTPUT0@', '@INPUT@'],
  build_by_default: true,
)

lexer_source = custom_target(
  'easydb_lexer_source',
  input: 'src/parser/lex.l',
  output: 'lex.yy.cpp',
  command: [flex, '-o', '@OUTPUT@', '@INPUT@'],
  depends: parser_sources,
  build_by_default: true,
)

easydb_sources = files(
  'src/catalog/column.cpp',
  'src/catalog/schema.cpp',
  'src/common/config.cpp',
  'src/type/bigint_type.cpp',
  'src/type/decimal_type.cpp',
  'src/type/integer_parent_type.cpp',
  'src/type/integer_type.cpp',
  'src/type/timestamp_type.cpp',
  'src/type/type.cpp',
  'src/type/value.cpp',
  'src/type/varlen_type.cpp',
  'src/storage/disk/disk_manager.cpp',
  'src/storage/easystore/index/clustered_leaf_page.cpp',
  'src/storage/easystore/index/clustered_index_handle.cpp',
  'src/storage/easystore/table/clustered_table_handle.cpp',
  'src/storage/easystore/table/clustered_table_manager.cpp',
  'src/storage/easystore/table/tuple.cpp',
  'src/buffer/buffer_pool_manager.cpp',
  'src/buffer/lru_replacer.cpp',
  'src/planner/planner.cpp',
  'src/execution/execution_manager.cpp',
  'src/execution/executor_sort.cpp',
  'src/execution/executor_aggregation.cpp',
  'src/execution/executor_delete.cpp',
  'src/execution/executor_hash_join.cpp',
  'src/execution/executor_index_scan.cpp',
  'src/execution/executor_insert.cpp',
  'src/execution/executor_merge_join.cpp',
  'src/execution/executor_nestedloop_join.cpp',
  'src/execution/executor_projection.cpp',
  'src/execution/executor_seq_scan.cpp',
  'src/execution/executor_update.cpp',
  'src/storage/easystore/concurrency/lock_manager.cpp',
  'src/storage/easystore/transaction/transaction_manager.cpp',
  'src/storage/easystore/transaction/rollback_manager.cpp',
  'src/storage/easystore/recovery/log_manager.cpp',
  'src/storage/easystore/recovery/recovery_manager.cpp',
  'src/storage/easystore/catalog/ddl_manager.cpp',
  'src/storage/easystore/catalog/load_manager.cpp',
  'src/analyze/analyze.cpp',
  'src/parser/ast.cpp',
  'src/storage/easystore/easystore.cpp',
)

easydb = static_library(
  'easydb',
  easydb_sources,
  parser_sources[0],
  lexer_source,
  include_directories: [src_inc, include_directories('.')],
  dependencies: [threads_dep, nlohmann_json_dep],
  link_with: murmur3,
  install: true,
)

easydb_server = executable(
  'easydb_server',
  'src/easydb.cpp',
  include_directories: src_inc,
  link_with: easydb,
  dependencies: [threads_dep, nlohmann_json_dep],
  install: true,
)

easydb_client = executable(
  'easydb_client',
  'client/main.cpp',
  include_directories: src_inc,
  dependencies: [readline_dep, threads_dep],
  override_options: ['cpp_std=gnu++20'],
  install: true,
)

gtest_dep = dependency('gtest')
gmock_main_dep = dependency('gmock_main')
test_sources = files(
  'test/buffer/buffer_pool_manager.cpp',
  'test/buffer/buffer_test.cpp',
  'test/buffer/lru_replacer_test.cpp',
  'test/buffer/lru_test.cpp',
  'test/execution/hash_join_test.cpp',
  'test/parser/parser_ast_printer.cpp',
  'test/parser/parser_test.cpp',
  'test/storage/disk/disk_manager_test.cpp',
  'test/storage/index/clustered_leaf_page_test.cpp',
  'test/storage/index/clustered_index_handle_test.cpp',
  'test/type/type_test.cpp',
)
test_names = [
  'buffer_pool_manager',
  'buffer_test',
  'lru_replacer_test',
  'lru_test',
  'hash_join_test',
  'parser_ast_printer',
  'parser_test',
  'disk_manager_test',
  'clustered_leaf_page_test',
  'clustered_index_handle_test',
  'type_test',
]

foreach i : range(test_sources.length())
  test_source = test_sources[i]
  test_name = test_names[i]
  test_exe = executable(
    test_name,
    test_source,
    include_directories: [src_inc, test_inc],
    dependencies: [gtest_dep, gmock_main_dep, threads_dep],
    link_with: easydb,
    override_options: ['warning_level=0'],
  )
  # Parser printer is a build artifact needing CLI arguments.
  if test_name != 'parser_ast_printer'
    test(test_name, test_exe)
  endif
endforeach

summary({
  'AddressSanitizer': get_option('easydb_enable_asan'),
  'tests': true,
}, section: 'EasyDB build options')
