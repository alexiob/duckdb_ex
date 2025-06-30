# Documentation Index

Welcome to the comprehensive documentation for DuckdbEx, the Elixir interface to DuckDB. This documentation provides complete coverage of all APIs, configuration options, and best practices.

## Quick Navigation

### Getting Started

- **[Getting Started](getting_started.md)** - Installation, basic usage, and quick examples

### Core APIs

- **[Query API](query_api.md)** - Standard SQL query execution
- **[Chunked API](chunked_api.md)** - High-performance streaming for large datasets
- **[Bulk Loading](bulk_loading.md)** - Appender API for efficient data insertion

### Advanced Features

- **[Transactions](transactions.md)** - Transaction management and patterns
- **[Prepared Statements](prepared_statements.md)** - Parameterized queries and batch operations
- **[Configuration](configuration.md)** - Database configuration and tuning
- **[Extensions](extensions.md)** - DuckDB extensions and their usage

### Performance and Best Practices

- **[Performance Guide](performance.md)** - Optimization strategies and monitoring
- **[Examples and Use Cases](examples.md)** - Real-world examples and patterns

### Data Types and Interoperability

- **[Data Types](data_types.md)** - Type mappings and conversion utilities

## API Quick Reference

### Connection Management

```elixir
# Open connection
{:ok, conn} = DuckdbEx.open(":memory:")
{:ok, conn} = DuckdbEx.open("/path/to/database.db")

# Close connection
:ok = DuckdbEx.close(conn)
```

### Basic Queries

```elixir
# Simple query
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM users")

# Parameterized query
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM users WHERE id = ?", [user_id])
```

### Chunked Processing (Large Results)

```elixir
# Process large datasets efficiently
{:ok, chunk_stream} = DuckdbEx.query_chunked(conn, "SELECT * FROM large_table")
chunk_stream |> Stream.each(&process_chunk/1) |> Stream.run()
```

### Bulk Loading (High Performance)

```elixir
# Fastest way to load data
{:ok, appender} = DuckdbEx.Appender.create(conn, nil, "table_name")
# ... append data ...
:ok = DuckdbEx.Appender.close(appender)
:ok = DuckdbEx.Appender.destroy(appender)
```

### Prepared Statements

```elixir
# Reusable parameterized queries
{:ok, stmt} = DuckdbEx.prepare(conn, "INSERT INTO users (name, email) VALUES (?, ?)")
{:ok, result} = DuckdbEx.execute(stmt, ["Alice", "alice@example.com"])
:ok = DuckdbEx.close(stmt)
```

### Transactions

```elixir
# Atomic operations
{:ok, result} = DuckdbEx.transaction(conn, fn conn ->
  DuckdbEx.query(conn, "INSERT INTO table1 VALUES (?)", [value1])
  DuckdbEx.query(conn, "INSERT INTO table2 VALUES (?)", [value2])
  :ok
end)
```

## Common Use Cases by Performance Requirements

### High-Performance Scenarios

- **Bulk Data Loading**: Use [Appender API](bulk_loading.md) (10-100x faster than SQL INSERTs)
- **Large Result Sets**: Use [Chunked API](chunked_api.md) for memory-efficient processing
- **Repeated Queries**: Use [Prepared Statements](prepared_statements.md) for optimal performance
- **Analytics Workloads**: Configure for [analytical performance](performance.md#configure-for-analytics)

### Standard Scenarios

- **Web Applications**: Use [regular Query API](query_api.md) with connection pooling
- **Reports**: Use [Query API](query_api.md) with appropriate [configuration](configuration.md)
- **Data Processing**: Combine multiple APIs based on operation type

### Development and Testing

- **Test Data**: Use [examples](examples.md#testing-and-development) for generating realistic test datasets
- **Development**: Use `:memory:` databases for fast iteration

## Error Handling Patterns

### Basic Error Handling

```elixir
case DuckdbEx.query(conn, sql) do
  {:ok, result} -> handle_success(result)
  {:error, %DuckdbEx.Error{} = error} -> handle_db_error(error)
  {:error, reason} -> handle_other_error(reason)
end
```

### Resource Management

```elixir
# Always clean up resources
try do
  {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "table")
  # ... operations ...
after
  DuckdbEx.Appender.destroy(appender)
end
```

### Transaction Error Handling

```elixir
case DuckdbEx.transaction(conn, fn conn ->
  # ... operations ...
end) do
  {:ok, result} -> {:ok, result}
  {:error, reason} -> {:error, "Transaction failed: #{reason}"}
end
```

## Configuration Quick Start

### Memory and Performance

```elixir
# Configure for analytical workload
DuckdbEx.query(conn, "SET threads = #{System.schedulers_online()}")
DuckdbEx.query(conn, "SET memory_limit = '2GB'")
DuckdbEx.query(conn, "SET enable_parallel_execution = true")
```

### Popular Extensions

```elixir
# Load common extensions
DuckdbEx.query(conn, "INSTALL httpfs")   # HTTP/S3 access
DuckdbEx.query(conn, "LOAD httpfs")

DuckdbEx.query(conn, "INSTALL parquet")  # Parquet support
DuckdbEx.query(conn, "LOAD parquet")

DuckdbEx.query(conn, "INSTALL json")     # JSON functions
DuckdbEx.query(conn, "LOAD json")
```

## Best Practices Summary

### ✅ DO

- Use the [Appender API](bulk_loading.md) for bulk data loading
- Process large results with the [Chunked API](chunked_api.md)
- Use [Prepared Statements](prepared_statements.md) for repeated queries
- Configure connections for your [workload type](performance.md#configuration-tuning)
- Always clean up resources (appenders, prepared statements)
- Use [transactions](transactions.md) for atomic operations
- Monitor [performance](performance.md#monitoring-and-profiling) for optimization opportunities

### ❌ DON'T

- Use individual INSERTs for bulk data (use Appender instead)
- Load entire large result sets into memory (use chunked processing)
- Forget to destroy appenders and close prepared statements
- Ignore connection configuration (leads to suboptimal performance)
- Use string concatenation for dynamic queries (SQL injection risk)

## Getting Help

1. **Read the Documentation**: Each section provides detailed explanations and examples
2. **Check Examples**: The [examples guide](examples.md) shows real-world usage patterns
3. **Performance Issues**: Consult the [performance guide](performance.md) for optimization strategies
4. **API Reference**: Each API guide includes complete function references and parameters

## Contributing to Documentation

This documentation is designed to be comprehensive and practical. If you find areas that need improvement:

1. **Missing Examples**: Add practical code examples for common use cases
2. **Performance Tips**: Share optimization discoveries and benchmarks
3. **Error Scenarios**: Document common error cases and solutions
4. **Integration Patterns**: Show how DuckdbEx works with other Elixir libraries

---

**Next Steps**: Start with [Getting Started](getting_started.md) if you're new to DuckdbEx, or jump to the specific API guide you need.
