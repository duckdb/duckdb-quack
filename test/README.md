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

## Cancellation before query startup

After a Release build with the usual `httpfs` test dependency, run the native
regression test:

```bash
cmake --build build/release --target quack_cancel_tests httpfs_loadable_extension
./build/release/extension/quack/quack_cancel_tests
```

This test calls the server's PREPARE and CANCEL handlers directly. It holds the
existing execution lock until cancellation has set the stream error and the
DuckDB interrupt flag. It then checks that the cancelled SQL did not execute any
INSERTs and that the same connection accepts a new query. No network timing or
production test hooks are needed. Each wait has a five-second deadline.
