# Getting Started

This guide will help you get started with DuckdbEx, from installation to running your first queries.

## Installation

Add DuckdbEx to your `mix.exs` dependencies:

```elixir
def deps do
  [
    {:duckdb_ex, "~> 0.4.0"}
  ]
end
```

Then run:

```bash
mix deps.get
```

## Platform Support

DuckdbEx supports the following platforms with precompiled binaries:

- **Linux**: x86_64, aarch64, arm (gnueabihf), riscv64
- **macOS**: x86_64 (Intel), aarch64 (Apple Silicon)
- **Windows**: x86_64 (MSVC and GNU)

### Building from Source

If precompiled binaries are not available for your platform, or if you prefer to build from source, set the `DUCKDB_EX_BUILD` environment variable:

```bash
export DUCKDB_EX_BUILD=true
mix deps.get
```

This will automatically download the latest DuckDB release and compile the NIF locally.

## NIF Management

DuckdbEx automatically manages Native Implemented Functions (NIFs) for optimal performance:

### Automatic Behavior

- **First Install**: NIFs are downloaded/built automatically during `mix deps.get`
- **Subsequent Runs**: Existing valid NIFs are reused (no rebuilding)
- **Smart Detection**: Automatically detects if rebuild is needed

### Manual Commands

```bash
# Download/build NIF if not present
mix nif.download

# Force rebuild NIF (useful after DuckDB updates)
mix nif.rebuild

# Clean all NIF artifacts
mix nif.clean
```

### Environment Variables

```bash
# Always build from source (skip precompiled download)
export DUCKDB_EX_BUILD=true
mix nif.download

# Force rebuild even if NIF exists
export DUCKDB_EX_FORCE_REBUILD=true
mix nif.download
```

### Troubleshooting NIF Issues

If you encounter NIF loading errors:

```bash
# Clean and rebuild everything
mix clean
mix nif.clean
mix nif.rebuild

# Or force rebuild from source
DUCKDB_EX_BUILD=true mix nif.rebuild
```

## Basic Usage

### 1. Open a Database

```elixir
# In-memory database (fastest, data lost on close)
{:ok, db} = DuckdbEx.open()

# Or specify :memory explicitly
{:ok, db} = DuckdbEx.open(:memory)

# Persistent database file
{:ok, db} = DuckdbEx.open("my_database.db")
```

### 2. Create a Connection

```elixir
{:ok, conn} = DuckdbEx.connect(db)
```

### 3. Execute Queries

```elixir
# Create a table
{:ok, _result} = DuckdbEx.query(conn, """
  CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    age INTEGER,
    created_at TIMESTAMP
  )
""")

# Insert data
{:ok, _result} = DuckdbEx.query(conn, """
  INSERT INTO users (id, name, age, created_at) VALUES
  (1, 'Alice', 30, '2024-01-01 12:00:00'),
  (2, 'Bob', 25, '2024-01-02 14:30:00'),
  (3, 'Charlie', 35, '2024-01-03 09:15:00')
""")

# Query data
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM users WHERE age > 25")

# Get results
columns = DuckdbEx.columns(result)
# [%{name: "id", type: :integer}, %{name: "name", type: :varchar}, ...]

rows = DuckdbEx.rows(result)
# [[1, "Alice", 30, ~N[2024-01-01 12:00:00]], [3, "Charlie", 35, ~N[2024-01-03 09:15:00]]]
```

### 4. Clean Up Resources

```elixir
DuckdbEx.close_connection(conn)
DuckdbEx.close_database(db)
```

## Working with Results

### Column Information

```elixir
{:ok, result} = DuckdbEx.query(conn, "SELECT id, name, age FROM users")

columns = DuckdbEx.columns(result)
# [
#   %{name: "id", type: :integer},
#   %{name: "name", type: :varchar},
#   %{name: "age", type: :integer}
# ]

# Get just column names
column_names = Enum.map(columns, & &1.name)
# ["id", "name", "age"]
```

### Row Data

```elixir
rows = DuckdbEx.rows(result)
# [[1, "Alice", 30], [2, "Bob", 25], [3, "Charlie", 35]]

# Working with rows
Enum.each(rows, fn [id, name, age] ->
  IO.puts("User #{id}: #{name} (#{age} years old)")
end)
```

### Row Count

```elixir
count = DuckdbEx.row_count(result)
# 3
```

## Error Handling

DuckdbEx functions return `{:ok, result}` or `{:error, reason}` tuples:

```elixir
case DuckdbEx.query(conn, "SELECT * FROM nonexistent_table") do
  {:ok, result} ->
    IO.puts("Query successful")
    rows = DuckdbEx.rows(result)

  {:error, reason} ->
    IO.puts("Query failed: #{reason}")
    # Query failed: Catalog Error: Table with name nonexistent_table does not exist!
end
```

## Configuration Example

```elixir
# Configure database settings
config = %{
  "memory_limit" => "1GB",
  "threads" => "4",
  "max_memory" => "80%"
}

{:ok, db} = DuckdbEx.open("configured.db", config)
{:ok, conn} = DuckdbEx.connect(db)

# Verify configuration
{:ok, result} = DuckdbEx.query(conn, "SELECT current_setting('memory_limit')")
[[memory_limit]] = DuckdbEx.rows(result)
IO.puts("Memory limit: #{memory_limit}")
```

## Next Steps

Now that you have the basics working, explore these advanced features:

- [Query API](query_api.md) - Learn about different query methods
- [Chunked API](chunked_api.md) - High-performance data processing
- [Bulk Loading](bulk_loading.md) - Fast data insertion with the Appender API
- [Transactions](transactions.md) - Ensure data consistency
- [Prepared Statements](prepared_statements.md) - Efficient repeated queries
- [Data Types](data_types.md) - Understanding type conversions
