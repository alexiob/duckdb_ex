# Prepared Statements

Prepared statements provide an efficient way to execute SQL queries multiple times with different parameters. They offer better performance for repeated queries and help prevent SQL injection attacks.

## Overview

DuckDB prepared statements are compiled once and can be executed multiple times with different parameter values. This provides several benefits:

- **Performance**: Query parsing and planning happens only once
- **Security**: Parameters are safely bound, preventing SQL injection
- **Type Safety**: Parameter types are validated at preparation time
- **Memory Efficiency**: Reduced overhead for repeated executions

## Basic Usage

### Creating and Executing Prepared Statements

```elixir
{:ok, db} = DuckdbEx.open()
{:ok, conn} = DuckdbEx.connect(db)

# Create test table
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    age INTEGER,
    email VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
""")

# Prepare a statement with parameters
{:ok, stmt} = DuckdbEx.PreparedStatement.prepare(conn, """
  INSERT INTO users (id, name, age, email)
  VALUES ($1, $2, $3, $4)
""")

# Execute with different parameter sets
{:ok, result1} = DuckdbEx.PreparedStatement.execute(stmt, [1, "Alice", 30, "alice@example.com"])
{:ok, result2} = DuckdbEx.PreparedStatement.execute(stmt, [2, "Bob", 25, "bob@example.com"])
{:ok, result3} = DuckdbEx.PreparedStatement.execute(stmt, [3, "Charlie", 35, "charlie@example.com"])

# Cleanup (automatic when stmt goes out of scope, but can be explicit)
:ok = DuckdbEx.PreparedStatement.destroy(stmt)

# Verify insertions
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM users ORDER BY id")
rows = DuckdbEx.rows(result)
IO.puts("Inserted users: #{inspect(rows)}")
```

### Query Prepared Statements

```elixir
# Prepare a SELECT statement
{:ok, select_stmt} = DuckdbEx.PreparedStatement.prepare(conn, """
  SELECT id, name, age
  FROM users
  WHERE age >= $1 AND name LIKE $2
""")

# Execute with different parameters
{:ok, result1} = DuckdbEx.PreparedStatement.execute(select_stmt, [25, "%a%"])
users_with_a = DuckdbEx.rows(result1)
IO.puts("Users with 'a' in name, age >= 25: #{inspect(users_with_a)}")

{:ok, result2} = DuckdbEx.PreparedStatement.execute(select_stmt, [30, "%b%"])
users_with_b = DuckdbEx.rows(result2)
IO.puts("Users with 'b' in name, age >= 30: #{inspect(users_with_b)}")

:ok = DuckdbEx.PreparedStatement.destroy(select_stmt)
```

## Parameter Types and Binding

### Supported Parameter Types

```elixir
# Prepare statements for different data types
{:ok, type_test_stmt} = DuckdbEx.PreparedStatement.prepare(conn, """
  CREATE TABLE type_test (
    int_col INTEGER,
    float_col DOUBLE,
    text_col VARCHAR,
    bool_col BOOLEAN,
    date_col DATE,
    timestamp_col TIMESTAMP,
    decimal_col DECIMAL(10,2)
  )
""")

{:ok, _} = DuckdbEx.PreparedStatement.execute(type_test_stmt, [])

# Insert with various types
{:ok, insert_stmt} = DuckdbEx.PreparedStatement.prepare(conn, """
  INSERT INTO type_test VALUES ($1, $2, $3, $4, $5, $6, $7)
""")

# Execute with different Elixir types
{:ok, _} = DuckdbEx.PreparedStatement.execute(insert_stmt, [
  42,                              # INTEGER
  3.14159,                         # DOUBLE
  "Hello, World!",                 # VARCHAR
  true,                            # BOOLEAN
  ~D[2024-01-15],                  # DATE
  ~N[2024-01-15 14:30:45],         # TIMESTAMP
  Decimal.new("999.99")            # DECIMAL
])

{:ok, _} = DuckdbEx.PreparedStatement.execute(insert_stmt, [
  -123,
  -2.71828,
  "Another string",
  false,
  ~D[2023-12-25],
  ~N[2023-12-25 23:59:59],
  Decimal.new("0.01")
])

# Query the results
{:ok, select_all} = DuckdbEx.PreparedStatement.prepare(conn, "SELECT * FROM type_test")
{:ok, result} = DuckdbEx.PreparedStatement.execute(select_all, [])
rows = DuckdbEx.rows(result)

Enum.each(rows, fn row ->
  IO.puts("Row: #{inspect(row)}")
end)
```

### NULL Parameter Handling

```elixir
# Prepare statement that handles NULL values
{:ok, null_stmt} = DuckdbEx.PreparedStatement.prepare(conn, """
  INSERT INTO users (id, name, age, email)
  VALUES ($1, $2, $3, $4)
""")

# Execute with NULL values (use nil in Elixir)
{:ok, _} = DuckdbEx.PreparedStatement.execute(null_stmt, [
  4,
  "Dave",
  nil,           # NULL age
  nil            # NULL email
])

{:ok, _} = DuckdbEx.PreparedStatement.execute(null_stmt, [
  5,
  nil,           # NULL name - this will fail due to NOT NULL constraint
  40,
  "eve@example.com"
])
```

## Advanced Prepared Statement Patterns

### Batch Processing with Prepared Statements

```elixir
defmodule BatchProcessor do
  def batch_insert(conn, table_name, columns, data_rows) do
    # Build parameterized INSERT statement
    column_list = Enum.join(columns, ", ")
    param_list = Enum.map_join(1..length(columns), ", ", &"$#{&1}")

    sql = "INSERT INTO #{table_name} (#{column_list}) VALUES (#{param_list})"

    case DuckdbEx.PreparedStatement.prepare(conn, sql) do
      {:ok, stmt} ->
        try do
          results = Enum.map(data_rows, fn row ->
            DuckdbEx.PreparedStatement.execute(stmt, row)
          end)

          # Check for any failures
          errors = Enum.filter(results, &match?({:error, _}, &1))

          if length(errors) > 0 do
            {:error, "#{length(errors)} insertions failed: #{inspect(errors)}"}
          else
            {:ok, "Successfully inserted #{length(data_rows)} rows"}
          end
        after
          :ok = DuckdbEx.PreparedStatement.destroy(stmt)
        end

      {:error, reason} ->
        {:error, "Failed to prepare statement: #{reason}"}
    end
  end

  def batch_update(conn, updates) do
    DuckdbEx.Transaction.with_transaction(conn, fn ->
      results = Enum.map(updates, fn {sql, params} ->
        case DuckdbEx.PreparedStatement.prepare(conn, sql) do
          {:ok, stmt} ->
            try do
              DuckdbEx.PreparedStatement.execute(stmt, params)
            after
              :ok = DuckdbEx.PreparedStatement.destroy(stmt)
            end

          {:error, reason} ->
            {:error, reason}
        end
      end)

      errors = Enum.filter(results, &match?({:error, _}, &1))

      if length(errors) > 0 do
        {:error, "Batch update failed: #{inspect(errors)}"}
      else
        {:ok, "All #{length(updates)} updates completed"}
      end
    end)
  end
end

# Example: Batch insert new users
new_users = [
  [6, "Frank", 28, "frank@example.com"],
  [7, "Grace", 32, "grace@example.com"],
  [8, "Henry", 27, "henry@example.com"]
]

case BatchProcessor.batch_insert(conn, "users", ["id", "name", "age", "email"], new_users) do
  {:ok, message} -> IO.puts("Batch insert success: #{message}")
  {:error, reason} -> IO.puts("Batch insert failed: #{reason}")
end

# Example: Batch updates
updates = [
  {"UPDATE users SET age = $1 WHERE id = $2", [29, 6]},
  {"UPDATE users SET email = $1 WHERE name = $2", ["grace.new@example.com", "Grace"]},
  {"UPDATE users SET age = age + 1 WHERE id = $1", [8]}
]

case BatchProcessor.batch_update(conn, updates) do
  {:ok, message} -> IO.puts("Batch update success: #{message}")
  {:error, reason} -> IO.puts("Batch update failed: #{reason}")
end
```

### Dynamic Query Building

```elixir
defmodule DynamicQueryBuilder do
  def build_search_query(filters) do
    base_sql = "SELECT id, name, age, email FROM users WHERE 1=1"

    {conditions, params} = build_conditions(filters, [], 1)

    final_sql = if length(conditions) > 0 do
      base_sql <> " AND " <> Enum.join(conditions, " AND ")
    else
      base_sql
    end

    {final_sql, params}
  end

  defp build_conditions([], params, _param_num), do: {[], Enum.reverse(params)}

  defp build_conditions([{:name_contains, value} | rest], params, param_num) do
    condition = "name LIKE $#{param_num}"
    build_conditions(rest, ["%#{value}%" | params], param_num + 1)
    |> add_condition(condition)
  end

  defp build_conditions([{:age_min, value} | rest], params, param_num) do
    condition = "age >= $#{param_num}"
    build_conditions(rest, [value | params], param_num + 1)
    |> add_condition(condition)
  end

  defp build_conditions([{:age_max, value} | rest], params, param_num) do
    condition = "age <= $#{param_num}"
    build_conditions(rest, [value | params], param_num + 1)
    |> add_condition(condition)
  end

  defp build_conditions([{:email_domain, domain} | rest], params, param_num) do
    condition = "email LIKE $#{param_num}"
    build_conditions(rest, ["%@#{domain}" | params], param_num + 1)
    |> add_condition(condition)
  end

  defp build_conditions([_unknown | rest], params, param_num) do
    # Skip unknown filters
    build_conditions(rest, params, param_num)
  end

  defp add_condition({conditions, params}, new_condition) do
    {[new_condition | conditions], params}
  end

  def execute_search(conn, filters) do
    {sql, params} = build_search_query(filters)

    case DuckdbEx.PreparedStatement.prepare(conn, sql) do
      {:ok, stmt} ->
        try do
          case DuckdbEx.PreparedStatement.execute(stmt, params) do
            {:ok, result} ->
              {:ok, DuckdbEx.rows(result)}
            {:error, reason} ->
              {:error, reason}
          end
        after
          :ok = DuckdbEx.PreparedStatement.destroy(stmt)
        end

      {:error, reason} ->
        {:error, "Failed to prepare search query: #{reason}"}
    end
  end
end

# Example: Dynamic search
filters = [
  {:name_contains, "a"},
  {:age_min, 25},
  {:age_max, 35},
  {:email_domain, "example.com"}
]

case DynamicQueryBuilder.execute_search(conn, filters) do
  {:ok, results} ->
    IO.puts("Search results:")
    Enum.each(results, fn [id, name, age, email] ->
      IO.puts("  #{id}: #{name} (#{age}) - #{email}")
    end)

  {:error, reason} ->
    IO.puts("Search failed: #{reason}")
end
```

## Error Handling and Validation

### Parameter Validation

```elixir
defmodule PreparedStatementValidator do
  def safe_execute(conn, sql, params) do
    with {:ok, validated_params} <- validate_parameters(params),
         {:ok, stmt} <- DuckdbEx.PreparedStatement.prepare(conn, sql),
         {:ok, result} <- DuckdbEx.PreparedStatement.execute(stmt, validated_params) do
      :ok = DuckdbEx.PreparedStatement.destroy(stmt)
      {:ok, result}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_parameters(params) when is_list(params) do
    validated = Enum.map(params, &validate_parameter/1)

    case Enum.find(validated, &match?({:error, _}, &1)) do
      nil -> {:ok, Enum.map(validated, fn {:ok, value} -> value end)}
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_parameter(nil), do: {:ok, nil}
  defp validate_parameter(value) when is_integer(value), do: {:ok, value}
  defp validate_parameter(value) when is_float(value), do: {:ok, value}
  defp validate_parameter(value) when is_boolean(value), do: {:ok, value}
  defp validate_parameter(value) when is_binary(value) do
    if String.valid?(value) do
      {:ok, value}
    else
      {:error, "Invalid UTF-8 string"}
    end
  end
  defp validate_parameter(%Date{} = date), do: {:ok, date}
  defp validate_parameter(%NaiveDateTime{} = datetime), do: {:ok, datetime}
  defp validate_parameter(%Decimal{} = decimal), do: {:ok, decimal}
  defp validate_parameter(value), do: {:error, "Unsupported parameter type: #{inspect(value)}"}
end

# Example usage with validation
case PreparedStatementValidator.safe_execute(conn,
  "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
  [9, "Invalid\x00String", 30]) do
  {:ok, result} ->
    IO.puts("Insert successful")
  {:error, reason} ->
    IO.puts("Validation failed: #{reason}")
end
```

### Statement Lifecycle Management

```elixir
defmodule StatementManager do
  def with_prepared_statement(conn, sql, fun) do
    case DuckdbEx.PreparedStatement.prepare(conn, sql) do
      {:ok, stmt} ->
        try do
          fun.(stmt)
        after
          :ok = DuckdbEx.PreparedStatement.destroy(stmt)
        end

      {:error, reason} ->
        {:error, "Failed to prepare statement: #{reason}"}
    end
  end

  def execute_multiple(conn, sql, param_sets) do
    with_prepared_statement(conn, sql, fn stmt ->
      results = Enum.map(param_sets, fn params ->
        case DuckdbEx.PreparedStatement.execute(stmt, params) do
          {:ok, result} -> {:ok, DuckdbEx.rows(result)}
          {:error, reason} -> {:error, reason}
        end
      end)

      # Separate successes and failures
      {successes, failures} = Enum.split_with(results, &match?({:ok, _}, &1))

      success_data = Enum.map(successes, fn {:ok, data} -> data end)

      if length(failures) > 0 do
        {:partial_success, success_data, failures}
      else
        {:ok, success_data}
      end
    end)
  end
end

# Example: Execute statement with multiple parameter sets
param_sets = [
  [10, "John", 45],
  [11, "Jane", 38],
  [12, "Invalid", -5],  # This might fail due to age constraint
  [13, "Jim", 42]
]

case StatementManager.execute_multiple(conn,
  "INSERT INTO users (id, name, age) VALUES ($1, $2, $3)",
  param_sets) do
  {:ok, results} ->
    IO.puts("All executions successful: #{length(results)} results")

  {:partial_success, successes, failures} ->
    IO.puts("#{length(successes)} successes, #{length(failures)} failures")
    IO.puts("Failures: #{inspect(failures)}")

  {:error, reason} ->
    IO.puts("Preparation failed: #{reason}")
end
```

## Performance Optimization

### Statement Caching

```elixir
defmodule StatementCache do
  use GenServer

  def start_link(conn) do
    GenServer.start_link(__MODULE__, conn, name: __MODULE__)
  end

  def execute_cached(sql, params) do
    GenServer.call(__MODULE__, {:execute, sql, params})
  end

  def clear_cache() do
    GenServer.call(__MODULE__, :clear_cache)
  end

  def init(conn) do
    {:ok, %{conn: conn, cache: %{}}}
  end

  def handle_call({:execute, sql, params}, _from, %{conn: conn, cache: cache} = state) do
    case Map.get(cache, sql) do
      nil ->
        # Statement not in cache, prepare it
        case DuckdbEx.PreparedStatement.prepare(conn, sql) do
          {:ok, stmt} ->
            case DuckdbEx.PreparedStatement.execute(stmt, params) do
              {:ok, result} ->
                new_cache = Map.put(cache, sql, stmt)
                {:reply, {:ok, result}, %{state | cache: new_cache}}
              {:error, reason} ->
                :ok = DuckdbEx.PreparedStatement.destroy(stmt)
                {:reply, {:error, reason}, state}
            end

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      stmt ->
        # Statement in cache, just execute
        case DuckdbEx.PreparedStatement.execute(stmt, params) do
          {:ok, result} ->
            {:reply, {:ok, result}, state}
          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  def handle_call(:clear_cache, _from, %{cache: cache} = state) do
    # Destroy all cached statements
    Enum.each(cache, fn {_sql, stmt} ->
      :ok = DuckdbEx.PreparedStatement.destroy(stmt)
    end)

    {:reply, :ok, %{state | cache: %{}}}
  end

  def terminate(_reason, %{cache: cache}) do
    # Clean up all statements
    Enum.each(cache, fn {_sql, stmt} ->
      :ok = DuckdbEx.PreparedStatement.destroy(stmt)
    end)
  end
end

# Example usage with caching
{:ok, _pid} = StatementCache.start_link(conn)

# First execution - statement gets cached
{:ok, result1} = StatementCache.execute_cached(
  "SELECT name, age FROM users WHERE age > $1",
  [30]
)

# Second execution - uses cached statement
{:ok, result2} = StatementCache.execute_cached(
  "SELECT name, age FROM users WHERE age > $1",
  [25]
)

# Different SQL - new statement prepared and cached
{:ok, result3} = StatementCache.execute_cached(
  "SELECT COUNT(*) FROM users WHERE name LIKE $1",
  ["%a%"]
)
```

### Performance Comparison

```elixir
defmodule PreparedStatementBenchmark do
  def benchmark_prepared_vs_regular(conn, iterations) do
    # Setup
    {:ok, _} = DuckdbEx.query(conn, "CREATE OR REPLACE TABLE bench_test (id INTEGER, value VARCHAR)")

    # Benchmark regular queries
    {time_regular, _} = :timer.tc(fn ->
      Enum.each(1..iterations, fn i ->
        {:ok, _} = DuckdbEx.query(conn, "INSERT INTO bench_test VALUES (#{i}, 'value_#{i}')")
      end)
    end)

    # Clear table
    {:ok, _} = DuckdbEx.query(conn, "DELETE FROM bench_test")

    # Benchmark prepared statement
    {time_prepared, _} = :timer.tc(fn ->
      {:ok, stmt} = DuckdbEx.PreparedStatement.prepare(conn, "INSERT INTO bench_test VALUES ($1, $2)")

      try do
        Enum.each(1..iterations, fn i ->
          {:ok, _} = DuckdbEx.PreparedStatement.execute(stmt, [i, "value_#{i}"])
        end)
      after
        :ok = DuckdbEx.PreparedStatement.destroy(stmt)
      end
    end)

    IO.puts("#{iterations} INSERT operations:")
    IO.puts("Regular queries: #{time_regular / 1000} ms")
    IO.puts("Prepared statement: #{time_prepared / 1000} ms")
    IO.puts("Speedup: #{Float.round(time_regular / time_prepared, 2)}x")

    # Cleanup
    {:ok, _} = DuckdbEx.query(conn, "DROP TABLE bench_test")
  end
end

# Run benchmark
PreparedStatementBenchmark.benchmark_prepared_vs_regular(conn, 1000)
```

## Best Practices

1. **Reuse Statements**: Prepare once, execute multiple times for best performance
2. **Always Clean Up**: Use try/after or helper functions to ensure statements are destroyed
3. **Validate Parameters**: Check parameter types and values before execution
4. **Use Transactions**: Combine prepared statements with transactions for consistency
5. **Cache Wisely**: Consider caching frequently used prepared statements
6. **Handle Errors**: Always check return values and handle errors appropriately
7. **Parameter Indexing**: Use $1, $2, etc. for parameters (1-based indexing)

## Common Patterns

### Repository Pattern

```elixir
defmodule UserRepository do
  def create_user(conn, user_attrs) do
    StatementManager.with_prepared_statement(conn,
      "INSERT INTO users (name, age, email) VALUES ($1, $2, $3) RETURNING id",
      fn stmt ->
        case DuckdbEx.PreparedStatement.execute(stmt, [
          user_attrs[:name],
          user_attrs[:age],
          user_attrs[:email]
        ]) do
          {:ok, result} ->
            [[id]] = DuckdbEx.rows(result)
            {:ok, id}
          {:error, reason} ->
            {:error, reason}
        end
      end)
  end

  def find_user_by_email(conn, email) do
    StatementManager.with_prepared_statement(conn,
      "SELECT id, name, age, email FROM users WHERE email = $1",
      fn stmt ->
        case DuckdbEx.PreparedStatement.execute(stmt, [email]) do
          {:ok, result} ->
            case DuckdbEx.rows(result) do
              [] -> {:error, :not_found}
              [[id, name, age, email]] -> {:ok, %{id: id, name: name, age: age, email: email}}
            end
          {:error, reason} ->
            {:error, reason}
        end
      end)
  end

  def update_user(conn, id, updates) do
    # Dynamic update builder would go here
    # For simplicity, assume we're updating age and email
    StatementManager.with_prepared_statement(conn,
      "UPDATE users SET age = $1, email = $2 WHERE id = $3",
      fn stmt ->
        DuckdbEx.PreparedStatement.execute(stmt, [
          updates[:age],
          updates[:email],
          id
        ])
      end)
  end
end

# Example usage
case UserRepository.create_user(conn, %{name: "Test User", age: 25, email: "test@example.com"}) do
  {:ok, user_id} -> IO.puts("Created user with ID: #{user_id}")
  {:error, reason} -> IO.puts("Failed to create user: #{reason}")
end
```

## Next Steps

- Learn about [Bulk Loading](bulk_loading.md) for high-performance data insertion
- Explore [Transactions](transactions.md) for combining prepared statements with transaction control
- See [Examples](examples.md) for real-world prepared statement usage patterns
