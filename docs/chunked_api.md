# Chunked API

The Chunked API provides high-performance streaming access to query results, making it ideal for processing large datasets that don't fit comfortably in memory. Unlike the regular Query API, which loads all results at once, the Chunked API processes data in manageable chunks.

## When to Use the Chunked API

- **Large Result Sets**: When queries return millions of rows
- **Memory Constraints**: When you need to limit memory usage
- **Streaming Processing**: When you want to process data as it's retrieved
- **Complex Data Types**: For optimal handling of structs, lists, and maps
- **ETL Operations**: When transforming data from one format to another

## Basic Usage

### Setting Up Chunked Processing

```elixir
{:ok, db} = DuckdbEx.open()
{:ok, conn} = DuckdbEx.connect(db)

# Create a large table for demonstration
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE large_dataset AS
  SELECT
    i as id,
    'user_' || i as username,
    random() * 100 as score,
    CASE WHEN i % 3 = 0 THEN 'premium' ELSE 'basic' END as tier
  FROM range(1000000) t(i)
""")

# Execute query and get result handle
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM large_dataset WHERE score > 80")
```

### Processing Chunks

```elixir
defmodule ChunkProcessor do
  def process_large_result(result, connection) do
    process_chunks(result, connection, 0)
  end

  defp process_chunks(result, connection, processed_count) do
    case DuckdbEx.fetch_chunk(result, connection) do
      {:ok, chunk_data} when chunk_data != [] ->
        # Process this chunk
        chunk_size = length(chunk_data)
        process_chunk(chunk_data)

        IO.puts("Processed chunk with #{chunk_size} rows")

        # Continue with next chunk
        process_chunks(result, connection, processed_count + chunk_size)

      {:ok, []} ->
        # No more data
        IO.puts("Finished processing #{processed_count} total rows")
        {:ok, processed_count}

      {:error, reason} ->
        {:error, "Chunk processing failed: #{reason}"}
    end
  end

  defp process_chunk(rows) do
    # Process each row in the chunk
    Enum.each(rows, fn [id, username, score, tier] ->
      # Your processing logic here
      if score > 95 do
        IO.puts("High scorer: #{username} with #{score}")
      end
    end)
  end
end

# Use the processor
{:ok, total_processed} = ChunkProcessor.process_large_result(result, conn)
```

## Advanced Chunked Processing

### Chunk Size Optimization

The chunk size is determined by DuckDB internally, but you can influence it through configuration:

```elixir
# Configure for larger chunks (uses more memory but fewer round trips)
config = %{
  "default_block_size" => "262144",  # 256KB
  "default_vector_size" => "2048"    # More rows per vector
}

{:ok, db} = DuckdbEx.open("optimized.db", config)
{:ok, conn} = DuckdbEx.connect(db)
```

### Streaming Transformations

```elixir
defmodule StreamProcessor do
  def transform_data(input_query, output_table, connection) do
    {:ok, result} = DuckdbEx.query(connection, input_query)

    # Create output table
    {:ok, _} = DuckdbEx.query(connection, """
      CREATE TABLE #{output_table} (
        processed_id INTEGER,
        category VARCHAR,
        score_bucket VARCHAR
      )
    """)

    # Create appender for fast output
    {:ok, appender} = DuckdbEx.Appender.create(connection, nil, output_table)

    process_and_insert(result, connection, appender)
  end

  defp process_and_insert(result, connection, appender) do
    case DuckdbEx.fetch_chunk(result, connection) do
      {:ok, chunk_data} when chunk_data != [] ->
        # Transform chunk data
        transformed = Enum.map(chunk_data, &transform_row/1)

        # Bulk insert transformed data
        :ok = DuckdbEx.Appender.append_rows(appender, transformed)

        # Continue with next chunk
        process_and_insert(result, connection, appender)

      {:ok, []} ->
        # Finalize output
        :ok = DuckdbEx.Appender.close(appender)
        :ok = DuckdbEx.Appender.destroy(appender)
        IO.puts("Transformation complete")

      {:error, reason} ->
        DuckdbEx.Appender.destroy(appender)
        {:error, reason}
    end
  end

  defp transform_row([id, _username, score, tier]) do
    category = case tier do
      "premium" -> "VIP"
      "basic" -> "Standard"
    end

    score_bucket = cond do
      score >= 90 -> "A"
      score >= 80 -> "B"
      score >= 70 -> "C"
      true -> "D"
    end

    [id * 1000, category, score_bucket]
  end
end

# Use the transformer
StreamProcessor.transform_data(
  "SELECT * FROM large_dataset WHERE score > 70",
  "processed_data",
  conn
)
```

## Complex Data Types in Chunks

The Chunked API provides the best support for DuckDB's complex data types:

### Working with Arrays and Lists

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE array_data AS
  SELECT
    i as id,
    [i, i*2, i*3] as numbers,
    {'name': 'item_' || i, 'value': i * 10} as metadata
  FROM range(100) t(i)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM array_data")

defmodule ComplexTypeProcessor do
  def process_complex_data(result, connection) do
    case DuckdbEx.fetch_chunk(result, connection) do
      {:ok, chunk_data} when chunk_data != [] ->
        Enum.each(chunk_data, fn [id, numbers, metadata] ->
          IO.puts("ID: #{id}")
          IO.puts("Numbers: #{inspect(numbers)}")
          IO.puts("Metadata: #{inspect(metadata)}")
        end)

        process_complex_data(result, connection)

      {:ok, []} ->
        IO.puts("Processing complete")

      {:error, reason} ->
        IO.puts("Error: #{reason}")
    end
  end
end

ComplexTypeProcessor.process_complex_data(result, conn)
```

### Working with Nested Structures

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE nested_data AS
  SELECT
    i as id,
    {
      'user': {
        'id': i,
        'profile': {
          'name': 'User ' || i,
          'preferences': ['pref1', 'pref2']
        }
      },
      'stats': [i*10, i*20, i*30]
    } as data
  FROM range(50) t(i)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM nested_data")

# The chunked API will properly deserialize nested structures
case DuckdbEx.fetch_chunk(result, conn) do
  {:ok, [[id, nested_data] | _rest]} ->
    IO.puts("ID: #{id}")
    IO.puts("Nested data: #{inspect(nested_data, pretty: true)}")
end
```

## Memory Management

### Controlling Memory Usage

```elixir
defmodule MemoryEfficientProcessor do
  def process_with_memory_limit(query, connection, max_chunks \\ 10) do
    {:ok, result} = DuckdbEx.query(connection, query)

    process_limited_chunks(result, connection, 0, max_chunks)
  end

  defp process_limited_chunks(result, connection, processed, max_chunks) do
    if processed >= max_chunks do
      IO.puts("Reached maximum chunk limit (#{max_chunks})")
      :ok
    else
      case DuckdbEx.fetch_chunk(result, connection) do
        {:ok, chunk_data} when chunk_data != [] ->
          # Process chunk
          process_chunk_efficiently(chunk_data)

          # Force garbage collection to free memory
          :erlang.garbage_collect()

          process_limited_chunks(result, connection, processed + 1, max_chunks)

        {:ok, []} ->
          IO.puts("All data processed")
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp process_chunk_efficiently(chunk_data) do
    # Process data without storing intermediate results
    chunk_data
    |> Stream.filter(fn [_id, _username, score, _tier] -> score > 85 end)
    |> Stream.map(fn [id, username, score, tier] -> "#{username}: #{score}" end)
    |> Enum.each(&IO.puts/1)
  end
end
```

## Error Handling in Chunked Processing

```elixir
defmodule RobustChunkProcessor do
  def safe_process(result, connection) do
    try do
      process_chunks_safely(result, connection, 0)
    rescue
      exception ->
        IO.puts("Exception during processing: #{Exception.message(exception)}")
        {:error, :processing_exception}
    end
  end

  defp process_chunks_safely(result, connection, chunk_count) do
    case DuckdbEx.fetch_chunk(result, connection) do
      {:ok, chunk_data} when chunk_data != [] ->
        case process_chunk_with_validation(chunk_data, chunk_count) do
          :ok ->
            process_chunks_safely(result, connection, chunk_count + 1)
          {:error, reason} ->
            IO.puts("Chunk #{chunk_count} processing failed: #{reason}")
            # Continue with next chunk or fail based on your needs
            process_chunks_safely(result, connection, chunk_count + 1)
        end

      {:ok, []} ->
        {:ok, chunk_count}

      {:error, reason} ->
        IO.puts("Chunk fetch failed: #{reason}")
        {:error, reason}
    end
  end

  defp process_chunk_with_validation(chunk_data, chunk_number) do
    try do
      # Validate chunk data structure
      if is_list(chunk_data) and length(chunk_data) > 0 do
        # Process the chunk
        Enum.each(chunk_data, &validate_and_process_row/1)
        :ok
      else
        {:error, "Invalid chunk structure"}
      end
    rescue
      exception ->
        {:error, "Processing error in chunk #{chunk_number}: #{Exception.message(exception)}"}
    end
  end

  defp validate_and_process_row(row) do
    case row do
      [id, username, score, tier] when is_integer(id) and is_binary(username) ->
        # Process valid row
        :ok
      _ ->
        IO.puts("Warning: Invalid row structure: #{inspect(row)}")
    end
  end
end
```

## Performance Comparison

### Chunked API vs Regular API

```elixir
defmodule PerformanceComparison do
  def compare_apis(connection) do
    # Create test data
    {:ok, _} = DuckdbEx.query(connection, """
      CREATE OR REPLACE TABLE perf_test AS
      SELECT i as id, random() as value
      FROM range(1000000) t(i)
    """)

    query = "SELECT * FROM perf_test WHERE value > 0.8"

    # Test regular API
    {time_regular, _result} = :timer.tc(fn ->
      {:ok, result} = DuckdbEx.query(connection, query)
      rows = DuckdbEx.rows(result)
      length(rows)
    end)

    # Test chunked API
    {time_chunked, _result} = :timer.tc(fn ->
      {:ok, result} = DuckdbEx.query(connection, query)
      count_chunks(result, connection, 0)
    end)

    IO.puts("Regular API: #{time_regular / 1000} ms")
    IO.puts("Chunked API: #{time_chunked / 1000} ms")
    IO.puts("Memory usage will be significantly lower with chunked API")
  end

  defp count_chunks(result, connection, total) do
    case DuckdbEx.fetch_chunk(result, connection) do
      {:ok, chunk_data} when chunk_data != [] ->
        count_chunks(result, connection, total + length(chunk_data))
      {:ok, []} ->
        total
      {:error, _} ->
        total
    end
  end
end

PerformanceComparison.compare_apis(conn)
```

## Best Practices

1. **Use for Large Datasets**: Only use chunked API when you actually need it
2. **Process Incrementally**: Don't accumulate chunks in memory
3. **Handle Errors Gracefully**: Always implement proper error handling
4. **Consider Chunk Size**: Understand that chunk size is managed by DuckDB
5. **Combine with Appender**: Use with bulk loading for ETL operations
6. **Memory Management**: Be conscious of memory usage in your processing logic

## Next Steps

- Learn about [Bulk Loading](bulk_loading.md) for efficient data insertion
- Explore [Data Types](data_types.md) to understand complex type handling
- See [Examples](examples.md) for real-world chunked processing scenarios
