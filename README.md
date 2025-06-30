# DuckDB Ex

An Elixir NIF wrapper for DuckDB using dirty NIFs for safe concurrent access.

## Features

- Fast in-memory and persistent SQL database
- Full SQL support with analytical query capabilities
- Concurrent access using dirty NIFs
- Memory-safe resource management
- Support for prepared statements
- Result streaming for large datasets
- Extension management (core and third-party)
- Vector similarity search (VSS) support
- Built-in array/vector operations

## Installation

Add `duckdb_ex` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:duckdb_ex, "~> 0.4.0"}
  ]
end
```

## Usage

### Basic Usage

```elixir
# Open a database (in-memory)
{:ok, db} = DuckdbEx.open()

# Open a database file
{:ok, db} = DuckdbEx.open("my_database.db")

# Connect to the database
{:ok, conn} = DuckdbEx.connect(db)

# Execute a query
{:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer")

# Access results
columns = DuckdbEx.columns(result)
rows = DuckdbEx.rows(result)

# Clean up
DuckdbEx.close_connection(conn)
DuckdbEx.close_database(db)
```

### Prepared Statements

```elixir
# Prepare a statement
{:ok, stmt} = DuckdbEx.prepare(conn, "SELECT * FROM users WHERE age > ?")

# Execute with parameters
{:ok, result} = DuckdbEx.execute(stmt, [25])
```

### Extensions

DuckDB Ex supports both core extensions and third-party extensions:

```elixir
# List available extensions
{:ok, extensions} = DuckdbEx.list_extensions(conn)

# Install and load a core extension
:ok = DuckdbEx.install_extension(conn, "json")
:ok = DuckdbEx.load_extension(conn, "json")

# Or do both in one step
:ok = DuckdbEx.install_and_load(conn, "parquet")

# Load extension from local file
:ok = DuckdbEx.load_extension_from_path(conn, "/path/to/extension.so")

# Check if extension is loaded
true = DuckdbEx.extension_loaded?(conn, "json")

# Get extension information
{:ok, info} = DuckdbEx.Extension.get_extension_info(conn, "parquet")
```

#### Available Core Extensions

- **Data Formats**: `json`, `parquet`, `csv`
- **Cloud Storage**: `aws`, `azure`, `httpfs`
- **Data Lake**: `delta`, `iceberg`
- **Database Scanners**: `postgres_scanner`, `sqlite_scanner`, `mysql_scanner`
- **Utilities**: `autocomplete`, `icu` (internationalization)

#### Vector Similarity Search (VSS)

DuckDB Ex supports vector similarity search through the DuckDB VSS extension using the standard extension API:

```elixir
# Install and load VSS extension automatically
:ok = DuckdbEx.install_and_load(conn, "vss")

# Check if VSS is loaded
true = DuckdbEx.extension_loaded?(conn, "vss")

# Create table with vector columns
{:ok, result} = DuckdbEx.query(conn, """
  CREATE TABLE embeddings (
    id INTEGER PRIMARY KEY,
    text VARCHAR,
    embedding FLOAT[384]
  )
""")
DuckdbEx.destroy_result(result)

# Insert vector data
{:ok, result} = DuckdbEx.query(conn, """
  INSERT INTO embeddings VALUES (1, 'hello world', [0.1, 0.2, 0.3, ...])
""")
DuckdbEx.destroy_result(result)

# Perform similarity search
{:ok, result} = DuckdbEx.query(conn, """
  SELECT text, array_cosine_similarity(embedding, [0.1, 0.2, 0.3, ...]) as similarity
  FROM embeddings
  ORDER BY similarity DESC
  LIMIT 10
""")
rows = DuckdbEx.rows(result)
DuckdbEx.destroy_result(result)
```

**Built-in Vector Operations**: Even without the VSS extension, DuckDB provides excellent vector support through built-in array functions:

```elixir
# Vector similarity using built-in functions
{:ok, result} = DuckdbEx.query(conn, """
  SELECT
    text,
    array_cosine_similarity(embedding, [0.1, 0.2, 0.3]::FLOAT[3]) as similarity
  FROM embeddings
  ORDER BY similarity DESC
  LIMIT 10
""")
```

**Available Vector Functions**:

- `array_cosine_similarity()` - Cosine similarity
- `array_distance()` - L2/Euclidean distance
- `array_inner_product()` - Dot product
- `array_cosine_distance()` - Cosine distance
- Plus 60+ other array manipulation functions

#### Built-in Extensions

Some extensions like `core_functions` and `parquet` are automatically loaded and provide essential functionality.

## License

MIT License

### Complex Types and Arrays

DuckDB Ex provides two APIs for handling query results:

#### Standard API

The standard API returns primitive types and represents complex types (arrays, lists, structs) as strings:

```elixir
{:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3] as numbers")
rows = DuckdbEx.rows(result)  # Returns: [{nil}] - arrays not supported
```

#### Chunked API (Recommended for Complex Types)

The chunked API uses DuckDB's native chunked result processing and properly handles complex types:

```elixir
{:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3] as numbers")
rows = DuckdbEx.rows_chunked(result)  # Returns: [{[1, 2, 3]}] - arrays as Elixir lists

# Works with nested arrays
{:ok, result} = DuckdbEx.query(conn, "SELECT [[1, 2], [3, 4]] as matrix")
rows = DuckdbEx.rows_chunked(result)  # Returns: [{[[1, 2], [3, 4]]}]

# Handles mixed types
{:ok, result} = DuckdbEx.query(conn, "SELECT 1 as id, 'test' as name, [1, 2, 3] as numbers")
rows = DuckdbEx.rows_chunked(result)  # Returns: [{1, "test", [1, 2, 3]}]
```

**Note**: For applications that need to work with arrays, lists, structs, or other complex DuckDB types, use `DuckdbEx.rows_chunked/1` instead of `DuckdbEx.rows/1`.
