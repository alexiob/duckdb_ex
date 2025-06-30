# Bulk Loading with Appender

The DuckDB Appender API provides the most efficient way to load large amounts of data into DuckDB from Elixir. It significantly outperforms prepared statements or individual `INSERT` statements for bulk data operations.

## Table of Contents

- [Overview](#overview)
- [Basic Usage](#basic-usage)
- [Batch Operations](#batch-operations)
- [Data Type Handling](#data-type-handling)
- [Error Handling](#error-handling)
- [Performance Considerations](#performance-considerations)
- [Advanced Patterns](#advanced-patterns)
- [Best Practices](#best-practices)

## Overview

The Appender API is designed for high-performance bulk data insertion. It bypasses much of the overhead associated with SQL parsing and execution, making it ideal for:

- Loading large CSV files
- Migrating data from other databases
- Real-time data ingestion
- ETL operations
- Initial data population

### Key Benefits

- **Performance**: Up to 10x faster than prepared statements for bulk operations
- **Memory Efficiency**: Streams data without loading entire datasets into memory
- **Type Safety**: Explicit type-specific append functions prevent type errors
- **Transaction Support**: Can be used within transactions for atomic operations

## Basic Usage

### Creating an Appender

```elixir
# Connect to database
{:ok, conn} = DuckdbEx.open(":memory:")

# Create a table first
{:ok, _result} = DuckdbEx.query(conn, """
  CREATE TABLE users (
    id INTEGER,
    name VARCHAR,
    email VARCHAR,
    age INTEGER,
    created_at TIMESTAMP
  )
""")

# Create an appender for the table
{:ok, appender} = DuckdbEx.Appender.create(conn, nil, "users")
```

### Appending Data Row by Row

```elixir
# Append first row
:ok = DuckdbEx.Appender.append_int32(appender, 1)
:ok = DuckdbEx.Appender.append_varchar(appender, "Alice Johnson")
:ok = DuckdbEx.Appender.append_varchar(appender, "alice@example.com")
:ok = DuckdbEx.Appender.append_int32(appender, 30)
:ok = DuckdbEx.Appender.append_timestamp(appender, ~N[2023-01-15 10:30:00])
:ok = DuckdbEx.Appender.end_row(appender)

# Append second row
:ok = DuckdbEx.Appender.append_int32(appender, 2)
:ok = DuckdbEx.Appender.append_varchar(appender, "Bob Smith")
:ok = DuckdbEx.Appender.append_varchar(appender, "bob@example.com")
:ok = DuckdbEx.Appender.append_int32(appender, 25)
:ok = DuckdbEx.Appender.append_timestamp(appender, ~N[2023-01-15 11:00:00])
:ok = DuckdbEx.Appender.end_row(appender)

# Finalize and cleanup
:ok = DuckdbEx.Appender.close(appender)
:ok = DuckdbEx.Appender.destroy(appender)
```

### Complete Example with Proper Resource Management

```elixir
defmodule BulkLoader do
  def load_users(conn, user_data) do
    # Create appender
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "users")

    try do
      # Append all rows
      Enum.each(user_data, fn {id, name, email, age, created_at} ->
        :ok = DuckdbEx.Appender.append_int32(appender, id)
        :ok = DuckdbEx.Appender.append_varchar(appender, name)
        :ok = DuckdbEx.Appender.append_varchar(appender, email)
        :ok = DuckdbEx.Appender.append_int32(appender, age)
        :ok = DuckdbEx.Appender.append_timestamp(appender, created_at)
        :ok = DuckdbEx.Appender.end_row(appender)
      end)

      # Finalize
      :ok = DuckdbEx.Appender.close(appender)
      {:ok, :loaded}
    rescue
      error ->
        # Cleanup on error
        DuckdbEx.Appender.destroy(appender)
        {:error, error}
    after
      # Always destroy the appender
      DuckdbEx.Appender.destroy(appender)
    end
  end
end

# Usage
user_data = [
  {1, "Alice Johnson", "alice@example.com", 30, ~N[2023-01-15 10:30:00]},
  {2, "Bob Smith", "bob@example.com", 25, ~N[2023-01-15 11:00:00]},
  {3, "Carol Davis", "carol@example.com", 35, ~N[2023-01-15 12:00:00]}
]

{:ok, :loaded} = BulkLoader.load_users(conn, user_data)
```

## Batch Operations

### Using append_rows/2 for Convenience

For simpler use cases, you can use the batch append function:

```elixir
{:ok, appender} = DuckdbEx.Appender.create(conn, nil, "users")

rows = [
  [1, "Alice", "alice@example.com", 30, ~N[2023-01-15 10:30:00]],
  [2, "Bob", "bob@example.com", 25, ~N[2023-01-15 11:00:00]],
  [3, "Carol", "carol@example.com", 35, ~N[2023-01-15 12:00:00]]
]

try do
  :ok = DuckdbEx.Appender.append_rows(appender, rows)
  :ok = DuckdbEx.Appender.close(appender)
after
  DuckdbEx.Appender.destroy(appender)
end
```

### Processing Large Datasets in Chunks

```elixir
defmodule ChunkedLoader do
  @chunk_size 1000

  def load_large_dataset(conn, data_stream) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "large_table")

    try do
      data_stream
      |> Stream.chunk_every(@chunk_size)
      |> Enum.each(fn chunk ->
        Enum.each(chunk, fn row ->
          append_row(appender, row)
        end)

        # Optionally flush periodically for very large datasets
        # This ensures data is written and memory is freed
        if rem(length(chunk), 5000) == 0 do
          DuckdbEx.Appender.flush(appender)
        end
      end)

      :ok = DuckdbEx.Appender.close(appender)
      {:ok, :completed}
    rescue
      error ->
        {:error, error}
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp append_row(appender, {id, value, timestamp}) do
    :ok = DuckdbEx.Appender.append_int64(appender, id)
    :ok = DuckdbEx.Appender.append_double(appender, value)
    :ok = DuckdbEx.Appender.append_timestamp(appender, timestamp)
    :ok = DuckdbEx.Appender.end_row(appender)
  end
end
```

## Data Type Handling

### Available Append Functions

The Appender provides type-specific functions for optimal performance:

```elixir
# Integer types
:ok = DuckdbEx.Appender.append_int8(appender, 127)
:ok = DuckdbEx.Appender.append_int16(appender, 32767)
:ok = DuckdbEx.Appender.append_int32(appender, 2147483647)
:ok = DuckdbEx.Appender.append_int64(appender, 9223372036854775807)

# Unsigned integer types
:ok = DuckdbEx.Appender.append_uint8(appender, 255)
:ok = DuckdbEx.Appender.append_uint16(appender, 65535)
:ok = DuckdbEx.Appender.append_uint32(appender, 4294967295)
:ok = DuckdbEx.Appender.append_uint64(appender, 18446744073709551615)

# Floating point types
:ok = DuckdbEx.Appender.append_float(appender, 3.14)
:ok = DuckdbEx.Appender.append_double(appender, 3.141592653589793)

# String types
:ok = DuckdbEx.Appender.append_varchar(appender, "Hello, World!")

# Boolean type
:ok = DuckdbEx.Appender.append_bool(appender, true)

# Date and time types
:ok = DuckdbEx.Appender.append_date(appender, ~D[2023-01-15])
:ok = DuckdbEx.Appender.append_time(appender, ~T[14:30:00])
:ok = DuckdbEx.Appender.append_timestamp(appender, ~N[2023-01-15 14:30:00])

# NULL values
:ok = DuckdbEx.Appender.append_null(appender)
```

### Handling Complex Types

```elixir
# Working with DECIMAL type
{:ok, _} = DuckdbEx.query(conn, "CREATE TABLE financial (id INT, amount DECIMAL(10,2))")
{:ok, appender} = DuckdbEx.Appender.create(conn, nil, "financial")

# Convert Decimal to string for appending
amount = Decimal.new("1234.56")
:ok = DuckdbEx.Appender.append_int32(appender, 1)
:ok = DuckdbEx.Appender.append_varchar(appender, Decimal.to_string(amount))
:ok = DuckdbEx.Appender.end_row(appender)
```

### Working with Lists and Arrays

```elixir
# For array/list columns, you may need to use JSON or convert to string
{:ok, _} = DuckdbEx.query(conn, "CREATE TABLE data (id INT, tags VARCHAR[])")
{:ok, appender} = DuckdbEx.Appender.create(conn, nil, "data")

tags = ["elixir", "database", "duckdb"]
tags_array = "[" <> Enum.join(Enum.map(tags, &"'#{&1}'"), ",") <> "]"

:ok = DuckdbEx.Appender.append_int32(appender, 1)
:ok = DuckdbEx.Appender.append_varchar(appender, tags_array)
:ok = DuckdbEx.Appender.end_row(appender)
```

## Error Handling

### Common Errors and Solutions

```elixir
defmodule SafeAppender do
  def safe_append(conn, table_name, data) do
    case DuckdbEx.Appender.create(conn, nil, table_name) do
      {:ok, appender} ->
        try do
          load_data(appender, data)
        rescue
          error ->
            handle_append_error(error)
        after
          DuckdbEx.Appender.destroy(appender)
        end

      {:error, reason} ->
        {:error, "Failed to create appender: #{reason}"}
    end
  end

  defp load_data(appender, data) do
    Enum.with_index(data, 1)
    |> Enum.reduce_while(:ok, fn {row, index}, _acc ->
      case append_single_row(appender, row) do
        :ok -> {:cont, :ok}
        error -> {:halt, {:error, "Row #{index}: #{inspect(error)}"}}
      end
    end)
    |> case do
      :ok ->
        :ok = DuckdbEx.Appender.close(appender)
        {:ok, :success}
      error ->
        error
    end
  end

  defp append_single_row(appender, {id, name, value}) do
    with :ok <- DuckdbEx.Appender.append_int32(appender, id),
         :ok <- DuckdbEx.Appender.append_varchar(appender, name),
         :ok <- DuckdbEx.Appender.append_double(appender, value),
         :ok <- DuckdbEx.Appender.end_row(appender) do
      :ok
    end
  end

  defp handle_append_error(%DuckdbEx.Error{} = error) do
    {:error, "DuckDB error: #{error.message}"}
  end

  defp handle_append_error(error) do
    {:error, "Unexpected error: #{inspect(error)}"}
  end
end
```

### Validation Before Appending

```elixir
defmodule ValidatedAppender do
  def append_with_validation(conn, table_name, rows) do
    with {:ok, schema} <- get_table_schema(conn, table_name),
         :ok <- validate_rows(rows, schema),
         {:ok, appender} <- DuckdbEx.Appender.create(conn, nil, table_name) do

      try do
        Enum.each(rows, fn row ->
          append_validated_row(appender, row, schema)
        end)

        :ok = DuckdbEx.Appender.close(appender)
        {:ok, length(rows)}
      after
        DuckdbEx.Appender.destroy(appender)
      end
    end
  end

  defp get_table_schema(conn, table_name) do
    query = "DESCRIBE #{table_name}"
    case DuckdbEx.query(conn, query) do
      {:ok, %{rows: rows}} ->
        schema = Enum.map(rows, fn [name, type, null, _key, _default, _extra] ->
          {name, type, null == "YES"}
        end)
        {:ok, schema}
      error ->
        error
    end
  end

  defp validate_rows(rows, schema) do
    # Add your validation logic here
    if Enum.all?(rows, &(length(&1) == length(schema))) do
      :ok
    else
      {:error, "Row length mismatch with schema"}
    end
  end

  defp append_validated_row(appender, row, schema) do
    Enum.zip(row, schema)
    |> Enum.each(fn {value, {_name, type, nullable}} ->
      append_typed_value(appender, value, type, nullable)
    end)

    :ok = DuckdbEx.Appender.end_row(appender)
  end

  defp append_typed_value(appender, nil, _type, true) do
    DuckdbEx.Appender.append_null(appender)
  end

  defp append_typed_value(appender, value, "INTEGER", _) do
    DuckdbEx.Appender.append_int32(appender, value)
  end

  defp append_typed_value(appender, value, "VARCHAR", _) do
    DuckdbEx.Appender.append_varchar(appender, to_string(value))
  end

  defp append_typed_value(appender, value, "DOUBLE", _) do
    DuckdbEx.Appender.append_double(appender, value * 1.0)
  end

  # Add more type mappings as needed
end
```

## Performance Considerations

### Optimizing Append Performance

```elixir
defmodule PerformantLoader do
  # Use larger transactions for better performance
  def load_with_transaction(conn, data) do
    DuckdbEx.transaction(conn, fn conn ->
      {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "performance_table")

      try do
        # Process in larger batches
        data
        |> Stream.chunk_every(10_000)
        |> Enum.each(fn batch ->
          load_batch(appender, batch)

          # Periodic flush for memory management
          DuckdbEx.Appender.flush(appender)
        end)

        :ok = DuckdbEx.Appender.close(appender)
        {:ok, length(data)}
      after
        DuckdbEx.Appender.destroy(appender)
      end
    end)
  end

  defp load_batch(appender, batch) do
    Enum.each(batch, fn row ->
      append_optimized_row(appender, row)
    end)
  end

  # Pre-calculate values and minimize function calls
  defp append_optimized_row(appender, {id, name, value, timestamp}) do
    :ok = DuckdbEx.Appender.append_int64(appender, id)
    :ok = DuckdbEx.Appender.append_varchar(appender, name)
    :ok = DuckdbEx.Appender.append_double(appender, value)
    :ok = DuckdbEx.Appender.append_timestamp(appender, timestamp)
    :ok = DuckdbEx.Appender.end_row(appender)
  end
end
```

### Memory Management for Large Datasets

```elixir
defmodule MemoryEfficientLoader do
  @chunk_size 5_000
  @flush_interval 50_000

  def load_large_file(conn, file_path) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "large_data")

    try do
      file_path
      |> File.stream!()
      |> CSV.decode!()  # Assuming you're using a CSV library
      |> Stream.with_index()
      |> Stream.chunk_every(@chunk_size)
      |> Enum.reduce(0, fn chunk, total_rows ->
        process_chunk(appender, chunk)
        new_total = total_rows + length(chunk)

        # Flush periodically to manage memory
        if rem(new_total, @flush_interval) == 0 do
          DuckdbEx.Appender.flush(appender)
          IO.puts("Processed #{new_total} rows...")
        end

        new_total
      end)

      :ok = DuckdbEx.Appender.close(appender)
      {:ok, :completed}
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp process_chunk(appender, chunk) do
    Enum.each(chunk, fn {row, _index} ->
      [id, name, value] = row
      :ok = DuckdbEx.Appender.append_int32(appender, String.to_integer(id))
      :ok = DuckdbEx.Appender.append_varchar(appender, name)
      :ok = DuckdbEx.Appender.append_double(appender, String.to_float(value))
      :ok = DuckdbEx.Appender.end_row(appender)
    end)
  end
end
```

## Advanced Patterns

### Concurrent Loading with Multiple Appenders

```elixir
defmodule ConcurrentLoader do
  def parallel_load(conn, data_partitions) do
    # Create separate connections for each partition
    tasks = Enum.map(data_partitions, fn {table_suffix, data} ->
      Task.async(fn ->
        {:ok, worker_conn} = DuckdbEx.open(":memory:")

        # Copy schema to worker connection
        setup_worker_table(worker_conn, table_suffix)

        # Load data
        load_partition(worker_conn, "temp_table_#{table_suffix}", data)
      end)
    end)

    # Wait for all tasks to complete
    results = Task.await_many(tasks, :infinity)

    # Merge results back to main connection if needed
    merge_results(conn, results)
  end

  defp setup_worker_table(conn, suffix) do
    DuckdbEx.query(conn, "CREATE TABLE temp_table_#{suffix} (id INT, data VARCHAR)")
  end

  defp load_partition(conn, table_name, data) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, table_name)

    try do
      Enum.each(data, fn {id, value} ->
        :ok = DuckdbEx.Appender.append_int32(appender, id)
        :ok = DuckdbEx.Appender.append_varchar(appender, value)
        :ok = DuckdbEx.Appender.end_row(appender)
      end)

      :ok = DuckdbEx.Appender.close(appender)
      {:ok, length(data)}
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp merge_results(conn, results) do
    # Implementation depends on your merge strategy
    Enum.reduce(results, 0, fn {:ok, count}, acc -> acc + count end)
  end
end
```

### Schema Evolution with Appenders

```elixir
defmodule SchemaEvolutionLoader do
  def load_with_schema_evolution(conn, data, target_schema_version) do
    current_version = get_schema_version(conn)

    if current_version < target_schema_version do
      migrate_schema(conn, current_version, target_schema_version)
    end

    load_data_for_version(conn, data, target_schema_version)
  end

  defp get_schema_version(conn) do
    case DuckdbEx.query(conn, "SELECT version FROM schema_version") do
      {:ok, %{rows: [[version]]}} -> version
      _ -> 1  # Default version
    end
  end

  defp migrate_schema(conn, from_version, to_version) do
    # Add migration logic here
    if from_version == 1 and to_version == 2 do
      DuckdbEx.query(conn, "ALTER TABLE users ADD COLUMN last_login TIMESTAMP")
    end
  end

  defp load_data_for_version(conn, data, version) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "users")

    try do
      Enum.each(data, fn user_data ->
        append_user_for_version(appender, user_data, version)
      end)

      :ok = DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp append_user_for_version(appender, {id, name, email}, 1) do
    :ok = DuckdbEx.Appender.append_int32(appender, id)
    :ok = DuckdbEx.Appender.append_varchar(appender, name)
    :ok = DuckdbEx.Appender.append_varchar(appender, email)
    :ok = DuckdbEx.Appender.end_row(appender)
  end

  defp append_user_for_version(appender, {id, name, email, last_login}, 2) do
    :ok = DuckdbEx.Appender.append_int32(appender, id)
    :ok = DuckdbEx.Appender.append_varchar(appender, name)
    :ok = DuckdbEx.Appender.append_varchar(appender, email)
    :ok = DuckdbEx.Appender.append_timestamp(appender, last_login)
    :ok = DuckdbEx.Appender.end_row(appender)
  end
end
```

## Best Practices

### 1. Always Use Proper Resource Management

```elixir
# ✅ Good: Always ensure cleanup
defmodule GoodLoader do
  def load_data(conn, data) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "table")

    try do
      # Your loading logic here
      load_rows(appender, data)
      :ok = DuckdbEx.Appender.close(appender)
      {:ok, :success}
    rescue
      error -> {:error, error}
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end
end
```

### 2. Use Transactions for Large Operations

```elixir
# ✅ Good: Wrap in transaction for atomicity
defmodule TransactionalLoader do
  def load_safely(conn, data) do
    DuckdbEx.transaction(conn, fn conn ->
      load_with_appender(conn, data)
    end)
  end
end
```

### 3. Validate Data Before Appending

```elixir
# ✅ Good: Validate first, then load
defmodule ValidatingLoader do
  def load_with_validation(conn, data) do
    case validate_data(data) do
      :ok -> load_data(conn, data)
      error -> error
    end
  end

  defp validate_data(data) do
    # Your validation logic
    :ok
  end
end
```

### 4. Handle Large Datasets Efficiently

```elixir
# ✅ Good: Process in chunks with periodic flushes
defmodule EfficientLoader do
  @chunk_size 1000

  def load_large_dataset(conn, data_stream) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "large_table")

    try do
      data_stream
      |> Stream.chunk_every(@chunk_size)
      |> Stream.with_index()
      |> Enum.each(fn {chunk, index} ->
        load_chunk(appender, chunk)

        # Flush every 10 chunks
        if rem(index, 10) == 0 do
          DuckdbEx.Appender.flush(appender)
        end
      end)

      :ok = DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end
end
```

### 5. Use Type-Specific Append Functions

```elixir
# ✅ Good: Use specific type functions
defmodule TypeSafeLoader do
  defp append_row(appender, %{id: id, name: name, score: score, active: active}) do
    :ok = DuckdbEx.Appender.append_int32(appender, id)          # Not append_varchar
    :ok = DuckdbEx.Appender.append_varchar(appender, name)      # Not append_int32
    :ok = DuckdbEx.Appender.append_double(appender, score)      # Not append_varchar
    :ok = DuckdbEx.Appender.append_bool(appender, active)       # Not append_int32
    :ok = DuckdbEx.Appender.end_row(appender)
  end
end
```

### Common Pitfalls to Avoid

```elixir
# ❌ Bad: Not cleaning up resources
defmodule BadLoader do
  def load_data(conn, data) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "table")
    # Loading logic...
    # Missing: close() and destroy() calls
  end
end

# ❌ Bad: Not handling errors
defmodule UnsafeLoader do
  def load_data(conn, data) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "table")
    # This might crash without cleanup
    Enum.each(data, fn row -> append_row(appender, row) end)
  end
end

# ❌ Bad: Loading everything into memory
defmodule MemoryHogLoader do
  def load_file(conn, huge_file) do
    # Don't do this for large files!
    all_data = File.read!(huge_file) |> parse_all_at_once()
    load_data(conn, all_data)
  end
end
```

---

The Appender API is a powerful tool for high-performance data loading in DuckDB. By following these patterns and best practices, you can efficiently load large datasets while maintaining data integrity and optimal performance.
