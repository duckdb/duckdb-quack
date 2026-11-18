# Testing this extension
This directory contains all the tests for this extension. The `sql` directory holds tests that are written as [SQLLogicTests](https://duckdb.org/dev/sqllogictest/intro.html). DuckDB aims to have most its tests in this format as SQL statements, so for the quack extension, this should probably be the goal too.

The root makefile contains targets to build and run all of these tests. To run the SQLLogicTests:
```bash
make test
```
or 
```bash
make test_debug
```

## C++ tests

`make test` / `make test_release`, `make test_debug`, and `make test_reldebug`
automatically build and run the C++ tests before the SQL tests.
The distribution CI calls these same targets. A native build or test failure
fails the test target. If `BUILD_UNITTESTS` is disabled or `httpfs` is not
configured, the C++ check target reports that it is skipped. SQL tests retain
their existing dependency checks. The usual `SKIP_TESTS=1` setting skips both
kinds of tests.

After configuring a build with the usual `httpfs` test dependency, run just the
native tests with:

```bash
make test_quack_release
```

C++ tests use DuckDB's bundled Catch framework and a shared `quack_cpp_tests`
executable. To add a test, create a source file containing `TEST_CASE` and
`REQUIRE` assertions, then add it to `QUACK_CPP_TEST_SOURCES` in
`test/cpp/CMakeLists.txt`. Do not add another `main()`. The root CMakeLists and
Makefile need no changes.

The `quack_cpp_check` target builds and runs the shared executable. Catch reports
each case and returns nonzero if any test fails. To select cancellation tests:

```bash
build/release/extension/quack/test/cpp/quack_cpp_tests '[cancel]'
```

### Cancellation regression

`early_cancel.cpp` calls the server's PREPARE and CANCEL handlers directly. One holds the
existing execution lock until cancellation has set the stream error and the
DuckDB interrupt flag. The other uses a test-only `QueryEnd` callback to pause
after the first INSERT commits and cancel before the second INSERT starts.
They check the number of committed INSERTs, the INTERRUPT response, and reuse
of the same connection. No network timing or production test hooks are needed.
Each wait has a five-second deadline.
