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

## Cancellation regression tests

`make test` / `make test_release`, `make test_debug`, and `make test_reldebug`
automatically build and run the native cancellation tests before the SQL tests.
The distribution CI calls these same targets. A native build or test failure
fails the test target. The usual `SKIP_TESTS=1` setting skips both kinds of tests.

After configuring a build with the usual `httpfs` test dependency, run just the
native tests with:

```bash
make test_quack_release
```

The tests call the server's PREPARE and CANCEL handlers directly. One holds the
existing execution lock until cancellation has set the stream error and the
DuckDB interrupt flag. The other uses a test-only `QueryEnd` callback to pause
after the first INSERT commits and cancel before the second INSERT starts.
They check the number of committed INSERTs, the INTERRUPT response, and reuse
of the same connection. No network timing or production test hooks are needed.
Each wait has a five-second deadline.
