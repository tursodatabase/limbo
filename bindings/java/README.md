# Limbo JDBC Driver

The Limbo JDBC driver is a library for accessing and creating Limbo database files using Java.

## Project Status

The project is actively developed. Feel free to open issues and contribute. 

To view related works, visit this [issue](https://github.com/tursodatabase/limbo/issues/615).

## Development

### How to Run Tests

To run tests, use the following command:

```shell
$ make test
``` 

### Code Formatting

To unify Java's formatting style, we use Spotless. To apply the formatting style, run:

```shell
$ make lint_apply
```

To apply the formatting style for Rust, run the following command:

```shell 
$ cargo fmt 
``` 

## Concepts

Note that this project is actively developed, so the concepts might change in the future.

- `LimboDB` represents a Limbo database.
- `LimboConnection` represents a connection to `LimboDB`. Multiple `LimboConnections` can be created on the same
  `LimboDB`.
- `LimboStatement` represents a Limbo database statement. Multiple `LimboStatements` can be created on the same
  `LimboConnection`.
- `LimboResultSet` represents the result of `LimboStatement` execution. It is one-to-one mapped to `LimboStatement`.