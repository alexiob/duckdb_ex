# Configuration

DuckDB provides numerous configuration options to optimize performance, control resource usage, and customize behavior. DuckdbEx exposes these configuration options through the `DuckdbEx.Config` module and accepts configuration maps when opening databases.

## Configuration Methods

### Using Configuration Maps

The simplest way to configure DuckDB is by passing a map to `DuckdbEx.open/2`:

```elixir
config = %{
  "memory_limit" => "1GB",
  "threads" => "4",
  "access_mode" => "READ_ONLY"
}

{:ok, db} = DuckdbEx.open("my_database.db", config)
{:ok, conn} = DuckdbEx.connect(db)
```

### Using Config Objects

For more control and validation, use the `DuckdbEx.Config` module:

```elixir
{:ok, config} = DuckdbEx.Config.new()
config = config
         |> DuckdbEx.Config.set("memory_limit", "2GB")
         |> DuckdbEx.Config.set("threads", "8")
         |> DuckdbEx.Config.set("default_block_size", "262144")

{:ok, db} = DuckdbEx.open("optimized.db", config)
```

## Core Configuration Options

### Memory Management

```elixir
# Memory limit configuration
memory_config = %{
  # Maximum memory DuckDB can use
  "memory_limit" => "2GB",

  # Percentage of available system memory to use
  "max_memory" => "80%",

  # Size of buffer manager buffer pool
  "buffer_manager" => "1GB",

  # Memory limit for hash tables in joins
  "max_temp_directory_size" => "512MB"
}

{:ok, db} = DuckdbEx.open("memory_optimized.db", memory_config)
{:ok, conn} = DuckdbEx.connect(db)

# Verify memory settings
{:ok, result} = DuckdbEx.query(conn, "SELECT current_setting('memory_limit')")
[[memory_limit]] = DuckdbEx.rows(result)
IO.puts("Memory limit: #{memory_limit}")
```

### Threading and Parallelism

```elixir
# Threading configuration
thread_config = %{
  # Number of threads for parallel execution
  "threads" => "#{System.schedulers_online()}",

  # Enable/disable parallelism
  "enable_progress_bar" => "false",
  "enable_profiling" => "false",

  # Worker thread configuration
  "worker_threads" => "#{div(System.schedulers_online(), 2)}"
}

{:ok, db} = DuckdbEx.open(":memory:", thread_config)
{:ok, conn} = DuckdbEx.connect(db)

# Check thread configuration
{:ok, result} = DuckdbEx.query(conn, "SELECT current_setting('threads')")
[[thread_count]] = DuckdbEx.rows(result)
IO.puts("Thread count: #{thread_count}")
```

### Storage and I/O

```elixir
# Storage configuration
storage_config = %{
  # Database access mode
  "access_mode" => "READ_WRITE",  # or "READ_ONLY"

  # Default block size for storage
  "default_block_size" => "262144",  # 256KB

  # Default vector size for columnar operations
  "default_vector_size" => "2048",

  # Enable/disable checkpointing
  "checkpoint_threshold" => "16MB",

  # WAL automatic checkpoint
  "wal_autocheckpoint" => "1000"
}

{:ok, db} = DuckdbEx.open("storage_optimized.db", storage_config)
```

### Query Optimization

```elixir
# Query optimization settings
optimizer_config = %{
  # Enable/disable query optimizer
  "enable_optimizer" => "true",

  # Optimizer timeout
  "optimizer_timeout" => "60000",  # 60 seconds

  # Join reordering
  "enable_join_reorder" => "true",

  # Cardinality estimation
  "enable_sampling" => "true",

  # Filter pushdown
  "enable_filter_pushdown" => "true"
}

{:ok, db} = DuckdbEx.open(":memory:", optimizer_config)
```

## Advanced Configuration

### Performance Tuning

```elixir
defmodule DatabaseConfigurator do
  def create_analytical_config() do
    %{
      # Memory settings for analytical workloads
      "memory_limit" => "4GB",
      "max_memory" => "80%",

      # Increase block and vector sizes for better analytical performance
      "default_block_size" => "1048576",  # 1MB blocks
      "default_vector_size" => "8192",    # Larger vectors

      # More threads for parallel processing
      "threads" => "#{System.schedulers_online()}",

      # Optimize for read-heavy workloads
      "checkpoint_threshold" => "64MB",

      # Enable all optimizations
      "enable_optimizer" => "true",
      "enable_join_reorder" => "true",
      "enable_filter_pushdown" => "true"
    }
  end

  def create_transactional_config() do
    %{
      # Conservative memory settings
      "memory_limit" => "1GB",
      "max_memory" => "50%",

      # Smaller block sizes for transactional workloads
      "default_block_size" => "65536",   # 64KB blocks
      "default_vector_size" => "1024",   # Smaller vectors

      # Frequent checkpointing for durability
      "checkpoint_threshold" => "4MB",
      "wal_autocheckpoint" => "100",

      # Conservative threading
      "threads" => "#{max(1, div(System.schedulers_online(), 2))}"
    }
  end

  def create_memory_constrained_config() do
    %{
      # Minimal memory usage
      "memory_limit" => "256MB",
      "max_memory" => "30%",

      # Small block and vector sizes
      "default_block_size" => "32768",   # 32KB blocks
      "default_vector_size" => "512",    # Small vectors

      # Single threaded to reduce overhead
      "threads" => "1",

      # Aggressive checkpointing to free memory
      "checkpoint_threshold" => "1MB"
    }
  end
end

# Example usage for different workload types
analytical_config = DatabaseConfigurator.create_analytical_config()
{:ok, analytical_db} = DuckdbEx.open("analytics.db", analytical_config)

transactional_config = DatabaseConfigurator.create_transactional_config()
{:ok, transactional_db} = DuckdbEx.open("transactions.db", transactional_config)

memory_config = DatabaseConfigurator.create_memory_constrained_config()
{:ok, memory_db} = DuckdbEx.open("memory_limited.db", memory_config)
```

### Extension Configuration

```elixir
# Configure extensions
extension_config = %{
  # Auto-load extensions
  "autoload_known_extensions" => "true",
  "autoinstall_known_extensions" => "false",

  # Extension directory
  "extension_directory" => "./duckdb_extensions",

  # Specific extension settings
  "enable_external_access" => "true",  # For httpfs, etc.
  "enable_object_cache" => "true"
}

{:ok, db} = DuckdbEx.open("extensions.db", extension_config)
{:ok, conn} = DuckdbEx.connect(db)

# Install and load extensions
{:ok, _} = DuckdbEx.query(conn, "INSTALL httpfs")
{:ok, _} = DuckdbEx.query(conn, "LOAD httpfs")

# Verify extension is loaded
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM duckdb_extensions() WHERE loaded = true")
loaded_extensions = DuckdbEx.rows(result)
IO.puts("Loaded extensions: #{inspect(loaded_extensions)}")
```

### Security Configuration

```elixir
# Security settings
security_config = %{
  # Disable external access for security
  "enable_external_access" => "false",

  # Read-only mode for safe data access
  "access_mode" => "READ_ONLY",

  # Disable potentially dangerous functions
  "allow_unsigned_extensions" => "false",

  # Resource limits
  "memory_limit" => "1GB",
  "query_timeout" => "300000"  # 5 minutes
}

{:ok, secure_db} = DuckdbEx.open("secure_readonly.db", security_config)
```

## Configuration Validation and Debugging

### Viewing Current Configuration

```elixir
defmodule ConfigInspector do
  def inspect_all_settings(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT name, value, description
      FROM duckdb_settings()
      ORDER BY name
    """)

    settings = DuckdbEx.rows(result)

    IO.puts("DuckDB Configuration Settings:")
    IO.puts("=" <> String.duplicate("=", 50))

    Enum.each(settings, fn [name, value, description] ->
      IO.puts("#{name}: #{value}")
      IO.puts("  Description: #{description}")
      IO.puts("")
    end)
  end

  def inspect_memory_settings(conn) do
    memory_settings = [
      "memory_limit",
      "max_memory",
      "buffer_manager",
      "default_block_size"
    ]

    IO.puts("Memory Configuration:")
    IO.puts("-" <> String.duplicate("-", 30))

    Enum.each(memory_settings, fn setting ->
      {:ok, result} = DuckdbEx.query(conn, "SELECT current_setting('#{setting}')")
      [[value]] = DuckdbEx.rows(result)
      IO.puts("#{setting}: #{value}")
    end)
  end

  def inspect_performance_settings(conn) do
    performance_settings = [
      "threads",
      "enable_optimizer",
      "enable_join_reorder",
      "checkpoint_threshold"
    ]

    IO.puts("Performance Configuration:")
    IO.puts("-" <> String.duplicate("-", 30))

    Enum.each(performance_settings, fn setting ->
      {:ok, result} = DuckdbEx.query(conn, "SELECT current_setting('#{setting}')")
      [[value]] = DuckdbEx.rows(result)
      IO.puts("#{setting}: #{value}")
    end)
  end
end

# Usage
{:ok, db} = DuckdbEx.open()
{:ok, conn} = DuckdbEx.connect(db)

ConfigInspector.inspect_memory_settings(conn)
ConfigInspector.inspect_performance_settings(conn)
```

### Configuration Validation

```elixir
defmodule ConfigValidator do
  def validate_config(config) do
    with :ok <- validate_memory_limit(config),
         :ok <- validate_threads(config),
         :ok <- validate_access_mode(config),
         :ok <- validate_block_size(config) do
      {:ok, config}
    else
      {:error, reason} -> {:error, "Configuration validation failed: #{reason}"}
    end
  end

  defp validate_memory_limit(config) do
    case Map.get(config, "memory_limit") do
      nil -> :ok
      limit when is_binary(limit) ->
        if String.match?(limit, ~r/^\d+(\.\d+)?(MB|GB|KB|%)$/) do
          :ok
        else
          {:error, "Invalid memory_limit format: #{limit}"}
        end
      _ -> {:error, "memory_limit must be a string"}
    end
  end

  defp validate_threads(config) do
    case Map.get(config, "threads") do
      nil -> :ok
      threads when is_binary(threads) ->
        case Integer.parse(threads) do
          {count, ""} when count > 0 and count <= 64 -> :ok
          _ -> {:error, "threads must be between 1 and 64"}
        end
      _ -> {:error, "threads must be a string"}
    end
  end

  defp validate_access_mode(config) do
    case Map.get(config, "access_mode") do
      nil -> :ok
      mode when mode in ["READ_ONLY", "READ_WRITE"] -> :ok
      mode -> {:error, "Invalid access_mode: #{mode}"}
    end
  end

  defp validate_block_size(config) do
    case Map.get(config, "default_block_size") do
      nil -> :ok
      size when is_binary(size) ->
        case Integer.parse(size) do
          {bytes, ""} when bytes >= 4096 and bytes <= 1048576 -> :ok
          _ -> {:error, "default_block_size must be between 4096 and 1048576 bytes"}
        end
      _ -> {:error, "default_block_size must be a string"}
    end
  end
end

# Example validation
test_config = %{
  "memory_limit" => "2GB",
  "threads" => "4",
  "access_mode" => "READ_WRITE",
  "default_block_size" => "262144"
}

case ConfigValidator.validate_config(test_config) do
  {:ok, validated_config} ->
    IO.puts("Configuration is valid")
    {:ok, db} = DuckdbEx.open("validated.db", validated_config)

  {:error, reason} ->
    IO.puts("Configuration error: #{reason}")
end
```

## Environment-Specific Configuration

### Development Configuration

```elixir
defmodule EnvironmentConfig do
  def development_config() do
    %{
      # Debug settings
      "enable_profiling" => "true",
      "enable_progress_bar" => "true",

      # Generous resource limits for development
      "memory_limit" => "2GB",
      "threads" => "#{System.schedulers_online()}",

      # Frequent checkpointing for safety
      "checkpoint_threshold" => "16MB",

      # Enable all optimizations for realistic performance
      "enable_optimizer" => "true",
      "enable_join_reorder" => "true"
    }
  end

  def test_config() do
    %{
      # Minimal resources for fast test execution
      "memory_limit" => "256MB",
      "threads" => "1",

      # Disable profiling for speed
      "enable_profiling" => "false",
      "enable_progress_bar" => "false",

      # Small checkpoint threshold for deterministic behavior
      "checkpoint_threshold" => "1MB"
    }
  end

  def production_config() do
    %{
      # Production memory settings
      "memory_limit" => "#{get_production_memory_limit()}",
      "max_memory" => "70%",

      # Optimal threading for production workload
      "threads" => "#{get_production_thread_count()}",

      # Production checkpoint settings
      "checkpoint_threshold" => "64MB",
      "wal_autocheckpoint" => "1000",

      # All optimizations enabled
      "enable_optimizer" => "true",
      "enable_join_reorder" => "true",
      "enable_filter_pushdown" => "true",

      # Security settings
      "enable_external_access" => "false"
    }
  end

  defp get_production_memory_limit() do
    # Calculate based on available system memory
    # This is a simplified example
    total_memory_gb = System.mem_info()[:total_memory] / (1024 * 1024 * 1024)
    production_limit = round(total_memory_gb * 0.6)
    "#{production_limit}GB"
  end

  defp get_production_thread_count() do
    # Use most but not all CPU cores in production
    max(1, System.schedulers_online() - 1) |> to_string()
  end

  def get_config_for_environment(env) do
    case env do
      :dev -> development_config()
      :test -> test_config()
      :prod -> production_config()
      _ -> %{}
    end
  end
end

# Usage based on Mix environment
config = EnvironmentConfig.get_config_for_environment(Mix.env())
{:ok, db} = DuckdbEx.open("app_#{Mix.env()}.db", config)
```

### Configuration from Environment Variables

```elixir
defmodule EnvConfigLoader do
  def load_from_env() do
    %{
      "memory_limit" => get_env("DUCKDB_MEMORY_LIMIT", "1GB"),
      "threads" => get_env("DUCKDB_THREADS", "#{System.schedulers_online()}"),
      "access_mode" => get_env("DUCKDB_ACCESS_MODE", "READ_WRITE"),
      "checkpoint_threshold" => get_env("DUCKDB_CHECKPOINT_THRESHOLD", "16MB"),
      "enable_external_access" => get_env("DUCKDB_EXTERNAL_ACCESS", "true")
    }
    |> Enum.filter(fn {_key, value} -> value != nil end)
    |> Map.new()
  end

  defp get_env(var_name, default) do
    System.get_env(var_name, default)
  end

  def load_and_validate() do
    config = load_from_env()

    case ConfigValidator.validate_config(config) do
      {:ok, validated_config} ->
        IO.puts("Loaded configuration from environment:")
        Enum.each(validated_config, fn {key, value} ->
          IO.puts("  #{key}: #{value}")
        end)
        {:ok, validated_config}

      {:error, reason} ->
        IO.puts("Environment configuration invalid: #{reason}")
        IO.puts("Using default configuration")
        {:ok, %{}}
    end
  end
end

# Load configuration from environment
{:ok, env_config} = EnvConfigLoader.load_and_validate()
{:ok, db} = DuckdbEx.open("env_configured.db", env_config)
```

## Monitoring and Tuning

### Performance Monitoring

```elixir
defmodule PerformanceMonitor do
  def benchmark_configuration(base_config, test_query, iterations \\ 100) do
    configs_to_test = [
      {"default", %{}},
      {"base", base_config},
      {"optimized", optimize_config(base_config)},
      {"memory_heavy", increase_memory(base_config)}
    ]

    results = Enum.map(configs_to_test, fn {name, config} ->
      {name, benchmark_config(config, test_query, iterations)}
    end)

    IO.puts("Performance Benchmark Results:")
    IO.puts("=" <> String.duplicate("=", 40))

    Enum.each(results, fn {name, {time_ms, memory_mb}} ->
      IO.puts("#{name}: #{time_ms} ms, #{memory_mb} MB peak memory")
    end)

    # Find best performing configuration
    {best_name, {best_time, _}} = Enum.min_by(results, fn {_, {time, _}} -> time end)
    IO.puts("\nBest configuration: #{best_name} (#{best_time} ms)")

    results
  end

  defp benchmark_config(config, test_query, iterations) do
    {:ok, db} = DuckdbEx.open(":memory:", config)
    {:ok, conn} = DuckdbEx.connect(db)

    # Setup test data
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE test_data AS
      SELECT i as id, random() as value
      FROM range(100000) t(i)
    """)

    # Warm up
    {:ok, _} = DuckdbEx.query(conn, test_query)

    # Benchmark
    {time_us, _} = :timer.tc(fn ->
      Enum.each(1..iterations, fn _ ->
        {:ok, _} = DuckdbEx.query(conn, test_query)
      end)
    end)

    # Get memory usage (simplified - would need actual memory monitoring)
    memory_mb = get_approximate_memory_usage(conn)

    DuckdbEx.close_connection(conn)
    DuckdbEx.close_database(db)

    {time_us / 1000, memory_mb}
  end

  defp optimize_config(base_config) do
    Map.merge(base_config, %{
      "enable_optimizer" => "true",
      "enable_join_reorder" => "true",
      "enable_filter_pushdown" => "true",
      "default_vector_size" => "4096"
    })
  end

  defp increase_memory(base_config) do
    Map.merge(base_config, %{
      "memory_limit" => "4GB",
      "default_block_size" => "1048576"
    })
  end

  defp get_approximate_memory_usage(conn) do
    {:ok, result} = DuckdbEx.query(conn, "SELECT current_setting('memory_limit')")
    [[limit_str]] = DuckdbEx.rows(result)

    # Parse memory limit as rough approximation
    case Regex.run(~r/(\d+)(\w+)/, limit_str) do
      [_, num_str, unit] ->
        num = String.to_integer(num_str)
        case unit do
          "GB" -> num * 1024
          "MB" -> num
          _ -> 1024
        end
      _ -> 1024
    end
  end
end

# Example benchmark
base_config = %{
  "memory_limit" => "1GB",
  "threads" => "4"
}

test_query = "SELECT COUNT(*), AVG(value) FROM test_data WHERE value > 0.5"

PerformanceMonitor.benchmark_configuration(base_config, test_query, 10)
```

## Best Practices

1. **Start with Defaults**: DuckDB's defaults are well-tuned for most use cases
2. **Measure Before Optimizing**: Always benchmark your specific workload
3. **Environment-Specific Configs**: Use different configurations for dev/test/prod
4. **Memory Limits**: Set appropriate memory limits to prevent OOM errors
5. **Thread Count**: Don't always use all CPU cores; sometimes fewer threads perform better
6. **Validate Configuration**: Always validate configuration values before use
7. **Monitor Performance**: Regularly benchmark and adjust configuration as needed

## Troubleshooting Configuration Issues

### Common Configuration Problems

```elixir
# Problem: Invalid memory limit format
bad_config = %{"memory_limit" => "2 GB"}  # Space not allowed
case DuckdbEx.open(":memory:", bad_config) do
  {:error, reason} -> IO.puts("Config error: #{reason}")
end

# Problem: Too many threads
bad_config = %{"threads" => "1000"}  # Too many threads
case DuckdbEx.open(":memory:", bad_config) do
  {:error, reason} -> IO.puts("Config error: #{reason}")
end

# Problem: Invalid access mode
bad_config = %{"access_mode" => "WRITE_ONLY"}  # Invalid mode
case DuckdbEx.open(":memory:", bad_config) do
  {:error, reason} -> IO.puts("Config error: #{reason}")
end
```

## Next Steps

- Learn about [Extensions](extensions.md) for extending DuckDB functionality
- Explore [Performance Optimization](performance.md) for advanced tuning
- See [Examples](examples.md) for real-world configuration scenarios
