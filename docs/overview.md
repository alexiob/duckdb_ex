# DuckDB Elixir Overview

DuckdbEx is an Elixir NIF wrapper for DuckDB, providing a high-performance analytical SQL database for Elixir applications. DuckDB is an in-process database that's perfect for analytical workloads, data processing, and embedded analytics.

## Key Features

- **High Performance**: Native C interface with dirty NIFs for concurrent access
- **Full SQL Support**: Complete SQL:2016 compliance with advanced analytical functions
- **Multiple Data APIs**: Regular query API and high-performance chunked data API
- **Type Safety**: Comprehensive type conversion between Elixir and DuckDB types
- **Bulk Loading**: High-performance appender API for fast data insertion
- **Transactions**: ACID-compliant transaction support
- **Prepared Statements**: Efficient query execution with parameter binding
- **Extensions**: Support for DuckDB extensions
- **Configuration**: Flexible database configuration options

## Quick Start

```elixir
# Add to your mix.exs dependencies
def deps do
  [
    {:duckdb_ex, "~> 0.4.0"}
  ]
end

# Basic usage
{:ok, db} = DuckdbEx.open()
{:ok, conn} = DuckdbEx.connect(db)

{:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer")
DuckdbEx.rows(result) # [[42]]

DuckdbEx.close_connection(conn)
DuckdbEx.close_database(db)
```

## Architecture

DuckdbEx is built as a Native Implemented Function (NIF) that provides:

1. **Safe Memory Management**: Automatic resource cleanup with Erlang VM garbage collection
2. **Concurrent Access**: Dirty NIFs allow multiple processes to access the database safely
3. **Type Conversion**: Automatic translation between Elixir and DuckDB data types
4. **Error Handling**: Comprehensive error reporting with meaningful messages

## Performance Characteristics

- **Regular API**: Good for ad-hoc queries and small result sets
- **Chunked API**: Optimized for large result sets and analytical workloads
- **Appender API**: Maximum performance for bulk data insertion
- **Prepared Statements**: Efficient for repeated query execution

## Documentation Structure

- [Getting Started](getting_started.md) - Installation and basic setup
- [Configuration](configuration.md) - Database configuration options and settings
- [Query API](query_api.md) - Regular query interface
- [Chunked API](chunked_api.md) - High-performance data streaming
- [Data Types](data_types.md) - Supported types and conversions
- [Transactions](transactions.md) - Transaction management
- [Prepared Statements](prepared_statements.md) - Efficient query execution
- [Bulk Loading](bulk_loading.md) - High-performance data insertion
- [Configuration](configuration.md) - Database configuration options
- [Extensions](extensions.md) - Using DuckDB extensions
- [Examples](examples.md) - Practical usage examples
