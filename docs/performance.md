# Performance Guide

This guide covers performance optimization strategies for DuckdbEx across all APIs and use cases. DuckDB is designed for high performance, but following these best practices will help you achieve optimal performance in your Elixir applications.

## Table of Contents

- [Performance Overview](#performance-overview)
- [Connection Management](#connection-management)
- [Query Optimization](#query-optimization)
- [Data Loading Performance](#data-loading-performance)
- [Memory Management](#memory-management)
- [Concurrency and Parallelism](#concurrency-and-parallelism)
- [Configuration Tuning](#configuration-tuning)
- [Monitoring and Profiling](#monitoring-and-profiling)
- [Best Practices Summary](#best-practices-summary)

## Performance Overview

DuckDB is an in-memory analytical database optimized for OLAP workloads. Understanding its strengths and characteristics is key to achieving optimal performance:

### DuckDB Strengths

- **Columnar Storage**: Optimized for analytical queries
- **Vectorized Execution**: Processes data in batches for better CPU utilization
- **Parallel Execution**: Automatic parallelization of queries
- **Zero-Copy Operations**: Minimal data movement where possible
- **Adaptive Query Processing**: Dynamic optimization during execution

### Performance Hierarchy (Fastest to Slowest)

1. **Appender API**: Bulk loading (10-100x faster than SQL INSERTs)
2. **Prepared Statements**: Reusable parameterized queries
3. **Direct SQL**: Regular query execution
4. **Individual INSERTs**: Slowest for bulk operations

## Connection Management

### Connection Pooling Strategy

```elixir
defmodule MyApp.DuckDBPool do
  use GenServer

  @pool_size 10
  @max_overflow 5

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def get_connection do
    case :poolboy.checkout(__MODULE__, false, 5000) do
      :full -> {:error, :pool_exhausted}
      conn -> {:ok, conn}
    end
  end

  def return_connection(conn) do
    :poolboy.checkin(__MODULE__, conn)
  end

  def init(opts) do
    poolboy_config = [
      {:name, {:local, __MODULE__}},
      {:worker_module, MyApp.DuckDBWorker},
      {:size, @pool_size},
      {:max_overflow, @max_overflow}
    ]

    children = [
      :poolboy.child_spec(__MODULE__, poolboy_config, opts)
    ]

    {:ok, children}
  end
end

defmodule MyApp.DuckDBWorker do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    database_path = Keyword.get(opts, :database_path, ":memory:")
    {:ok, conn} = DuckdbEx.open(database_path)

    # Configure connection for optimal performance
    DuckdbEx.query(conn, "SET threads TO 4")
    DuckdbEx.query(conn, "SET memory_limit = '1GB'")

    {:ok, %{conn: conn}}
  end
end
```

### Connection Configuration

```elixir
defmodule PerformanceConfig do
  def configure_connection(conn, workload_type \\ :analytical) do
    base_config = [
      "SET enable_profiling = 'query_tree_optimizer'",
      "SET profiling_output = '/tmp/duckdb_profile.json'",
      "SET preserve_insertion_order = false"  # Better performance for analytical workloads
    ]

    workload_config = case workload_type do
      :analytical ->
        [
          "SET threads = #{System.schedulers_online()}",
          "SET memory_limit = '#{get_memory_limit()}MB'",
          "SET max_memory = '#{get_memory_limit()}MB'",
          "SET temp_directory = '/tmp/duckdb_temp'"
        ]

      :transactional ->
        [
          "SET threads = 2",  # Lower parallelism for OLTP
          "SET memory_limit = '512MB'",
          "SET checkpoint_threshold = '16MB'"
        ]

      :mixed ->
        [
          "SET threads = #{div(System.schedulers_online(), 2)}",
          "SET memory_limit = '1GB'"
        ]
    end

    Enum.each(base_config ++ workload_config, fn sql ->
      {:ok, _} = DuckdbEx.query(conn, sql)
    end)

    conn
  end

  defp get_memory_limit do
    # Use 50% of available system memory
    {memory_kb, _} = :memsup.get_system_memory_data()[:available_memory]
    div(memory_kb, 2048)  # Convert to MB and take half
  end
end
```

## Query Optimization

### Efficient Query Patterns

```elixir
defmodule QueryOptimization do
  # ✅ Good: Use prepared statements for repeated queries
  def efficient_user_lookup(conn) do
    {:ok, stmt} = DuckdbEx.prepare(conn, "SELECT * FROM users WHERE id = ?")

    # Reuse the same prepared statement
    user_ids = [1, 2, 3, 4, 5]
    users = Enum.map(user_ids, fn id ->
      {:ok, result} = DuckdbEx.execute(stmt, [id])
      result.rows |> List.first()
    end)

    DuckdbEx.close(stmt)
    users
  end

  # ✅ Good: Use batch operations
  def efficient_batch_insert(conn, user_data) do
    # Much faster than individual INSERTs
    placeholders = user_data
    |> Enum.map(fn _ -> "(?, ?, ?)" end)
    |> Enum.join(", ")

    values = Enum.flat_map(user_data, fn {name, email, age} -> [name, email, age] end)

    sql = "INSERT INTO users (name, email, age) VALUES #{placeholders}"
    {:ok, _} = DuckdbEx.query(conn, sql, values)
  end

  # ✅ Good: Use efficient WHERE clauses
  def efficient_filtering(conn, date_range, status_list) do
    # Use parameterized queries with efficient operators
    placeholders = Enum.map(status_list, fn _ -> "?" end) |> Enum.join(", ")

    sql = """
    SELECT * FROM orders
    WHERE created_at BETWEEN ? AND ?
      AND status IN (#{placeholders})
      AND amount > 0  -- Filter early
    ORDER BY created_at DESC
    LIMIT 1000  -- Always limit large result sets
    """

    params = [date_range.start_date, date_range.end_date] ++ status_list
    DuckdbEx.query(conn, sql, params)
  end

  # ✅ Good: Use appropriate aggregations
  def efficient_analytics(conn) do
    # Leverage DuckDB's analytical capabilities
    sql = """
    SELECT
      DATE_TRUNC('day', created_at) as day,
      status,
      COUNT(*) as order_count,
      SUM(amount) as total_amount,
      AVG(amount) as avg_amount,
      -- Use window functions for advanced analytics
      SUM(amount) OVER (ORDER BY DATE_TRUNC('day', created_at)) as running_total
    FROM orders
    WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'
    GROUP BY DATE_TRUNC('day', created_at), status
    ORDER BY day DESC, status
    """

    DuckdbEx.query(conn, sql)
  end
end
```

### Query Performance Anti-patterns

```elixir
defmodule QueryAntiPatterns do
  # ❌ Bad: N+1 queries
  def inefficient_user_orders(conn, user_ids) do
    # Don't do this!
    Enum.map(user_ids, fn user_id ->
      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM orders WHERE user_id = ?", [user_id])
      result.rows
    end)
  end

  # ✅ Good: Single query with JOIN or IN clause
  def efficient_user_orders(conn, user_ids) do
    placeholders = Enum.map(user_ids, fn _ -> "?" end) |> Enum.join(", ")
    sql = "SELECT * FROM orders WHERE user_id IN (#{placeholders})"
    {:ok, result} = DuckdbEx.query(conn, sql, user_ids)
    result.rows
  end

  # ❌ Bad: Unfiltered large table scans
  def inefficient_large_scan(conn) do
    # Scans entire table
    DuckdbEx.query(conn, "SELECT * FROM large_table ORDER BY created_at DESC")
  end

  # ✅ Good: Filtered and limited queries
  def efficient_recent_data(conn) do
    sql = """
    SELECT * FROM large_table
    WHERE created_at >= CURRENT_DATE - INTERVAL '7 days'
    ORDER BY created_at DESC
    LIMIT 100
    """
    DuckdbEx.query(conn, sql)
  end
end
```

## Data Loading Performance

### Bulk Loading Strategies

```elixir
defmodule BulkLoadingPerformance do
  # ✅ Fastest: Use Appender API for bulk inserts
  def fastest_bulk_load(conn, large_dataset) do
    # Disable autocommit for better performance
    {:ok, _} = DuckdbEx.query(conn, "BEGIN TRANSACTION")

    try do
      {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "target_table")

      try do
        # Process in chunks to manage memory
        large_dataset
        |> Stream.chunk_every(10_000)
        |> Stream.with_index()
        |> Enum.each(fn {chunk, index} ->
          load_chunk_with_appender(appender, chunk)

          # Flush periodically
          if rem(index, 10) == 0 do
            DuckdbEx.Appender.flush(appender)
            IO.puts("Loaded #{(index + 1) * 10_000} records...")
          end
        end)

        DuckdbEx.Appender.close(appender)
        {:ok, _} = DuckdbEx.query(conn, "COMMIT")

      after
        DuckdbEx.Appender.destroy(appender)
      end
    rescue
      error ->
        DuckdbEx.query(conn, "ROLLBACK")
        {:error, error}
    end
  end

  # ✅ Good: Batch INSERT for medium datasets
  def batch_insert(conn, medium_dataset) when length(medium_dataset) < 10_000 do
    batch_size = 1000

    medium_dataset
    |> Enum.chunk_every(batch_size)
    |> Enum.each(fn batch ->
      placeholders = Enum.map(batch, fn _ -> "(?, ?, ?)" end) |> Enum.join(", ")
      values = Enum.flat_map(batch, fn {a, b, c} -> [a, b, c] end)

      sql = "INSERT INTO target_table (col1, col2, col3) VALUES #{placeholders}"
      {:ok, _} = DuckdbEx.query(conn, sql, values)
    end)
  end

  # ✅ Good: Use COPY for CSV files
  def load_from_csv(conn, csv_file_path) do
    sql = """
    COPY target_table FROM '#{csv_file_path}' (
      FORMAT CSV,
      HEADER true,
      DELIMITER ',',
      QUOTE '"'
    )
    """
    DuckdbEx.query(conn, sql)
  end

  defp load_chunk_with_appender(appender, chunk) do
    Enum.each(chunk, fn {col1, col2, col3} ->
      :ok = DuckdbEx.Appender.append_int32(appender, col1)
      :ok = DuckdbEx.Appender.append_varchar(appender, col2)
      :ok = DuckdbEx.Appender.append_double(appender, col3)
      :ok = DuckdbEx.Appender.end_row(appender)
    end)
  end
end
```

### Parallel Loading

```elixir
defmodule ParallelLoading do
  def parallel_bulk_load(base_conn, data_partitions) do
    # Create separate connections for parallel loading
    tasks = Enum.map(data_partitions, fn {partition_id, data} ->
      Task.async(fn ->
        {:ok, worker_conn} = DuckdbEx.open(":memory:")

        # Copy schema to worker
        setup_worker_schema(worker_conn, partition_id)

        # Load partition data
        load_partition_data(worker_conn, "temp_table_#{partition_id}", data)

        # Return temporary table info for merging
        {:ok, partition_id, worker_conn}
      end)
    end)

    # Wait for all partitions to load
    results = Task.await_many(tasks, :infinity)

    # Merge results back to main connection
    merge_partitioned_data(base_conn, results)
  end

  defp setup_worker_schema(conn, partition_id) do
    # Create temporary table with same schema
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE temp_table_#{partition_id} AS
      SELECT * FROM target_table WHERE 1=0
    """)
  end

  defp load_partition_data(conn, table_name, data) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, table_name)

    try do
      Enum.each(data, fn row ->
        append_row(appender, row)
      end)

      DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp merge_partitioned_data(base_conn, partition_results) do
    # Use ATTACH DATABASE to merge results
    Enum.each(partition_results, fn {:ok, partition_id, worker_conn} ->
      # Export partition data and import to main connection
      # Implementation depends on your specific use case
    end)
  end
end
```

## Memory Management

### Memory-Efficient Processing

```elixir
defmodule MemoryManagement do
  def process_large_result_set(conn, query) do
    # Use chunked API for large result sets
    case DuckdbEx.query_chunked(conn, query) do
      {:ok, chunk_stream} ->
        chunk_stream
        |> Stream.map(&process_chunk/1)
        |> Stream.run()  # Process without accumulating in memory

      error -> error
    end
  end

  def memory_conscious_aggregation(conn) do
    # Break large aggregations into smaller chunks
    date_ranges = generate_date_ranges(~D[2023-01-01], ~D[2023-12-31], 30)

    results = Enum.reduce(date_ranges, [], fn {start_date, end_date}, acc ->
      sql = """
      SELECT
        '#{start_date}' as period_start,
        COUNT(*) as count,
        SUM(amount) as total
      FROM large_table
      WHERE date_column BETWEEN '#{start_date}' AND '#{end_date}'
      """

      {:ok, result} = DuckdbEx.query(conn, sql)
      acc ++ result.rows
    end)

    # Combine results
    combine_aggregation_results(results)
  end

  defp process_chunk(chunk) do
    # Process chunk data without storing
    chunk.rows
    |> Enum.each(fn row ->
      # Your processing logic here
      IO.puts("Processing row: #{inspect(row)}")
    end)
  end

  defp generate_date_ranges(start_date, end_date, chunk_days) do
    start_date
    |> Date.range(end_date)
    |> Enum.chunk_every(chunk_days)
    |> Enum.map(fn chunk -> {List.first(chunk), List.last(chunk)} end)
  end

  defp combine_aggregation_results(results) do
    # Combine the chunked aggregation results
    Enum.reduce(results, {0, 0}, fn [_period, count, total], {acc_count, acc_total} ->
      {acc_count + count, acc_total + total}
    end)
  end
end
```

### Resource Cleanup

```elixir
defmodule ResourceManagement do
  def with_managed_resources(conn, table_name, operation) do
    # Create appender
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, table_name)

    # Create prepared statement
    {:ok, stmt} = DuckdbEx.prepare(conn, "SELECT COUNT(*) FROM #{table_name}")

    try do
      # Perform operations
      result = operation.(appender, stmt)

      # Proper cleanup order
      :ok = DuckdbEx.Appender.close(appender)
      {:ok, result}
    rescue
      error -> {:error, error}
    after
      # Always cleanup resources
      DuckdbEx.Appender.destroy(appender)
      DuckdbEx.close(stmt)
    end
  end

  def with_transaction_retry(conn, operation, max_retries \\ 3) do
    Enum.reduce_while(1..max_retries, {:error, :not_attempted}, fn attempt, _acc ->
      case DuckdbEx.transaction(conn, operation) do
        {:ok, result} -> {:halt, {:ok, result}}
        {:error, reason} when attempt < max_retries ->
          # Wait before retry (exponential backoff)
          :timer.sleep(100 * :math.pow(2, attempt))
          {:cont, {:error, reason}}
        error -> {:halt, error}
      end
    end)
  end
end
```

## Concurrency and Parallelism

### Task-Based Parallelism

```elixir
defmodule ConcurrentProcessing do
  def parallel_query_execution(queries) do
    # Execute multiple independent queries concurrently
    tasks = Enum.map(queries, fn {name, sql} ->
      Task.async(fn ->
        {:ok, conn} = DuckdbEx.open(":memory:")

        try do
          {:ok, result} = DuckdbEx.query(conn, sql)
          {name, result}
        after
          DuckdbEx.close(conn)
        end
      end)
    end)

    # Collect results
    Task.await_many(tasks, 30_000)
  end

  def concurrent_data_processing(conn, data_chunks) do
    # Process data chunks concurrently with shared connection
    # Note: DuckDB connections are not thread-safe, so use separate connections

    supervisor_opts = [
      strategy: :one_for_one,
      max_restarts: 3,
      max_seconds: 5
    ]

    {:ok, supervisor} = Task.Supervisor.start_link(supervisor_opts)

    try do
      tasks = Enum.map(data_chunks, fn chunk ->
        Task.Supervisor.async(supervisor, fn ->
          {:ok, worker_conn} = DuckdbEx.open(":memory:")

          try do
            process_data_chunk(worker_conn, chunk)
          after
            DuckdbEx.close(worker_conn)
          end
        end)
      end)

      Task.await_many(tasks, :infinity)
    after
      Task.Supervisor.stop(supervisor)
    end
  end

  defp process_data_chunk(conn, chunk) do
    # Setup tables
    setup_chunk_schema(conn)

    # Load data
    load_chunk_data(conn, chunk)

    # Process and return results
    {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM chunk_table")
    result
  end

  defp setup_chunk_schema(conn) do
    DuckdbEx.query(conn, "CREATE TABLE chunk_table (id INT, data VARCHAR)")
  end

  defp load_chunk_data(conn, chunk) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "chunk_table")

    try do
      Enum.each(chunk, fn {id, data} ->
        :ok = DuckdbEx.Appender.append_int32(appender, id)
        :ok = DuckdbEx.Appender.append_varchar(appender, data)
        :ok = DuckdbEx.Appender.end_row(appender)
      end)

      DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end
end
```

## Configuration Tuning

### Performance-Oriented Configuration

```elixir
defmodule PerformanceTuning do
  def configure_for_analytics(conn) do
    # Optimize for analytical workloads
    performance_settings = [
      # Memory settings
      "SET memory_limit = '#{get_optimal_memory_limit()}MB'",
      "SET max_memory = '#{get_optimal_memory_limit()}MB'",
      "SET temp_directory = '/tmp/duckdb_temp'",

      # Threading
      "SET threads = #{System.schedulers_online()}",
      "SET enable_parallel_execution = true",

      # Query optimization
      "SET enable_optimizer = true",
      "SET enable_profiling = 'query_tree_optimizer'",
      "SET force_parallelism = true",

      # I/O optimization
      "SET preserve_insertion_order = false",
      "SET enable_progress_bar = false",

      # Checkpoint settings for persistence
      "SET checkpoint_threshold = '64MB'",
      "SET wal_autocheckpoint = 1000"
    ]

    Enum.each(performance_settings, fn setting ->
      {:ok, _} = DuckdbEx.query(conn, setting)
    end)
  end

  def configure_for_transactional(conn) do
    # Optimize for transactional workloads
    transactional_settings = [
      # Lower memory usage
      "SET memory_limit = '512MB'",
      "SET max_memory = '512MB'",

      # Reduced parallelism
      "SET threads = 2",
      "SET enable_parallel_execution = false",

      # Faster checkpointing
      "SET checkpoint_threshold = '16MB'",
      "SET wal_autocheckpoint = 100",

      # Preserve order for consistency
      "SET preserve_insertion_order = true"
    ]

    Enum.each(transactional_settings, fn setting ->
      {:ok, _} = DuckdbEx.query(conn, setting)
    end)
  end

  defp get_optimal_memory_limit do
    # Use 70% of available system memory
    case :memsup.get_system_memory_data() do
      memory_data when is_list(memory_data) ->
        available = Keyword.get(memory_data, :available_memory, 1_000_000)
        div(available * 7, 10_240)  # Convert to MB and take 70%

      _ -> 1024  # Default 1GB
    end
  end
end
```

## Monitoring and Profiling

### Performance Monitoring

```elixir
defmodule PerformanceMonitoring do
  def monitor_query_performance(conn, query, params \\ []) do
    # Enable profiling
    {:ok, _} = DuckdbEx.query(conn, "SET enable_profiling = 'query_tree_optimizer'")

    # Measure execution time
    start_time = System.monotonic_time(:millisecond)

    result = case params do
      [] -> DuckdbEx.query(conn, query)
      params -> DuckdbEx.query(conn, query, params)
    end

    end_time = System.monotonic_time(:millisecond)
    execution_time = end_time - start_time

    # Get profiling information
    {:ok, profile_result} = DuckdbEx.query(conn, "SELECT * FROM pragma_last_profiling_output()")

    case result do
      {:ok, query_result} ->
        performance_info = %{
          execution_time_ms: execution_time,
          rows_returned: length(query_result.rows),
          profile_data: profile_result.rows
        }

        log_performance(query, performance_info)
        {:ok, query_result, performance_info}

      error -> error
    end
  end

  def benchmark_operations(conn, operations) do
    results = Enum.map(operations, fn {name, operation} ->
      {execution_time, result} = :timer.tc(fn -> operation.(conn) end)

      {name, %{
        execution_time_microseconds: execution_time,
        execution_time_ms: div(execution_time, 1000),
        result: result
      }}
    end)

    # Sort by execution time
    results
    |> Enum.sort_by(fn {_name, %{execution_time_microseconds: time}} -> time end)
    |> Enum.each(fn {name, %{execution_time_ms: time}} ->
      IO.puts("#{name}: #{time}ms")
    end)

    results
  end

  defp log_performance(query, performance_info) do
    if performance_info.execution_time_ms > 1000 do
      Logger.warn("Slow query detected",
        query: String.slice(query, 0, 100),
        execution_time: performance_info.execution_time_ms,
        rows: performance_info.rows_returned
      )
    end
  end
end
```

### Memory Usage Tracking

```elixir
defmodule MemoryTracking do
  def track_memory_usage(operation) do
    initial_memory = get_memory_usage()

    result = operation.()

    final_memory = get_memory_usage()
    memory_diff = final_memory - initial_memory

    IO.puts("Memory usage change: #{memory_diff} bytes")

    if memory_diff > 100_000_000 do  # 100MB
      Logger.warn("High memory usage detected: #{div(memory_diff, 1_000_000)}MB")
    end

    result
  end

  defp get_memory_usage do
    :erlang.memory(:total)
  end
end
```

## Best Practices Summary

### 🚀 DO: Performance Best Practices

```elixir
# ✅ Use connection pooling for concurrent applications
{:ok, conn} = MyApp.ConnectionPool.get_connection()

# ✅ Use prepared statements for repeated queries
{:ok, stmt} = DuckdbEx.prepare(conn, "SELECT * FROM users WHERE active = ?")

# ✅ Use Appender API for bulk data loading
{:ok, appender} = DuckdbEx.Appender.create(conn, nil, "bulk_table")

# ✅ Process large results with chunked API
{:ok, stream} = DuckdbEx.query_chunked(conn, "SELECT * FROM large_table")

# ✅ Use transactions for atomic operations
DuckdbEx.transaction(conn, fn conn ->
  # Multiple operations
end)

# ✅ Configure connection for your workload
PerformanceTuning.configure_for_analytics(conn)

# ✅ Clean up resources properly
try do
  # operations
after
  DuckdbEx.Appender.destroy(appender)
  DuckdbEx.close(stmt)
end
```

### ❌ DON'T: Performance Anti-patterns

```elixir
# ❌ Don't use individual INSERTs for bulk data
Enum.each(large_dataset, fn row ->
  DuckdbEx.query(conn, "INSERT INTO table VALUES (?, ?)", row)
end)

# ❌ Don't load entire large result sets into memory
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM massive_table")
all_rows = result.rows  # OOM risk!

# ❌ Don't ignore connection configuration
{:ok, conn} = DuckdbEx.open(":memory:")
# No configuration = suboptimal performance

# ❌ Don't forget resource cleanup
{:ok, appender} = DuckdbEx.Appender.create(conn, nil, "table")
# Missing: destroy(appender)

# ❌ Don't use string concatenation for dynamic queries
sql = "SELECT * FROM table WHERE id = #{user_input}"  # SQL injection risk!
```

### Performance Measurement Template

```elixir
defmodule PerformanceTemplate do
  def measure_and_optimize(conn, operation_name, operation) do
    # Before optimization
    {time_before, result_before} = :timer.tc(operation)

    # Apply optimizations
    apply_optimizations(conn)

    # After optimization
    {time_after, result_after} = :timer.tc(operation)

    improvement = ((time_before - time_after) / time_before) * 100

    IO.puts("""
    Performance Results for #{operation_name}:
    - Before: #{div(time_before, 1000)}ms
    - After: #{div(time_after, 1000)}ms
    - Improvement: #{Float.round(improvement, 2)}%
    """)

    result_after
  end

  defp apply_optimizations(conn) do
    # Apply relevant optimizations
    PerformanceTuning.configure_for_analytics(conn)
  end
end
```

---

By following these performance guidelines, you can achieve optimal performance with DuckdbEx across all use cases, from high-throughput analytical workloads to efficient transactional processing.
