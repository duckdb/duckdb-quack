# The Quack Client/Server Protocol for DuckDB

> Quack is released as a pre-release extension and is currently experimental. If you encounter any issues, please file them [via GitHub](https://github.com/duckdb/duckdb-quack/issues).

The `quack` extension adds a client-server protocol to DuckDB. With this extension, DuckDB can act as both a server and a client to communicate over a network. For more details, please see
the [announcement page](https://duckdb.org/quack),
the [blog post](https://duckdb.org/2026/05/12/quack-remote-protocol)
and the [documentation](https://duckdb.org/docs/current/quack/overview).

## Usage Example

We have to install the extension in all involved DuckDB instances:

```sql
INSTALL quack FROM core_nightly;
```

Then, we can use one DuckDB instance as a server like so:

```sql
LOAD quack;
CALL quack_serve('quack:localhost', token = 'super_secret');
CREATE TABLE hello AS FROM VALUES ('world') v(s);
```

And talk to this server from another instance:

```sql
LOAD quack;
CREATE SECRET (TYPE quack, TOKEN 'super_secret');
ATTACH 'quack:localhost' AS remote;
FROM remote.hello;
```

This should show the content of the remote table `hello` on the client side.

We can also copy data from client to server:

```sql
-- on client
CREATE TABLE remote.hello2 AS FROM VALUES ('world2')v(s);
```

```sql
-- on server
FROM hello2;
```

## Development

### Managing dependencies

DuckDB extensions use VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:

```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps

Now to build the extension, run:

```bash
make
```

The main binaries that will be built are:

```bash
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/quack/quack.duckdb_extension
```

- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `quack.duckdb_extension` is the loadable binary as it would be distributed.

### Running the tests

Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:

```bash
make test
```
