# Query API

The Query API provides the standard interface for executing SQL queries and retrieving results. This API is perfect for most use cases and provides automatic type conversion from DuckDB to Elixir types.

## Basic Query Execution

### Simple Queries

```elixir
{:ok, db} = DuckdbEx.open()
{:ok, conn} = DuckdbEx.connect(db)

# Execute a simple query
{:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer, 'hello' as greeting")

# Get the results
columns = DuckdbEx.columns(result)
# [%{name: "answer", type: :integer}, %{name: "greeting", type: :varchar}]

rows = DuckdbEx.rows(result)
# [[42, "hello"]]
```

### Data Definition Language (DDL)

```elixir
# Create tables
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    name VARCHAR NOT NULL,
    price DECIMAL(10,2),
    category VARCHAR,
    in_stock BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
""")

# Create indexes
{:ok, _} = DuckdbEx.query(conn, "CREATE INDEX idx_category ON products(category)")

# Drop tables
{:ok, _} = DuckdbEx.query(conn, "DROP TABLE IF EXISTS temp_table")
```

### Data Manipulation Language (DML)

```elixir
# Insert data
{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO products (id, name, price, category) VALUES
  (1, 'Laptop', 999.99, 'Electronics'),
  (2, 'Chair', 149.50, 'Furniture'),
  (3, 'Book', 29.95, 'Books')
""")

# Update data
{:ok, _} = DuckdbEx.query(conn, """
  UPDATE products
  SET price = price * 0.9
  WHERE category = 'Books'
""")

# Delete data
{:ok, _} = DuckdbEx.query(conn, "DELETE FROM products WHERE price < 30")
```

## Working with Results

### Column Metadata

```elixir
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM products")

columns = DuckdbEx.columns(result)
# [
#   %{name: "id", type: :integer},
#   %{name: "name", type: :varchar},
#   %{name: "price", type: :decimal},
#   %{name: "category", type: :varchar},
#   %{name: "in_stock", type: :boolean},
#   %{name: "created_at", type: :timestamp}
# ]

# Extract column information
column_names = Enum.map(columns, & &1.name)
column_types = Enum.map(columns, & &1.type)
```

### Row Data Access

```elixir
rows = DuckdbEx.rows(result)

# Access individual rows
[first_row | _rest] = rows
[id, name, price, category, in_stock, created_at] = first_row

# Process all rows
Enum.each(rows, fn [id, name, price, category, in_stock, created_at] ->
  status = if in_stock, do: "Available", else: "Out of Stock"
  IO.puts("#{name} (#{category}): $#{price} - #{status}")
end)
```

### Result Metadata

```elixir
# Get number of rows
row_count = DuckdbEx.row_count(result)

# Get number of columns
column_count = DuckdbEx.column_count(result)

IO.puts("Result has #{row_count} rows and #{column_count} columns")
```

## Advanced Queries

### Analytical Functions

```elixir
{:ok, result} = DuckdbEx.query(conn, """
  SELECT
    category,
    name,
    price,
    ROW_NUMBER() OVER (PARTITION BY category ORDER BY price DESC) as rank,
    AVG(price) OVER (PARTITION BY category) as avg_category_price,
    LAG(price) OVER (ORDER BY price) as prev_price
  FROM products
  ORDER BY category, price DESC
""")

# Results include window function columns
rows = DuckdbEx.rows(result)
```

### Common Table Expressions (CTEs)

```elixir
{:ok, result} = DuckdbEx.query(conn, """
  WITH category_stats AS (
    SELECT
      category,
      COUNT(*) as product_count,
      AVG(price) as avg_price,
      MIN(price) as min_price,
      MAX(price) as max_price
    FROM products
    GROUP BY category
  )
  SELECT * FROM category_stats
  WHERE product_count > 1
  ORDER BY avg_price DESC
""")
```

### JSON and Complex Data

```elixir
# Working with JSON data
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE events (
    id INTEGER,
    data JSON,
    metadata STRUCT(source VARCHAR, version INTEGER)
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO events VALUES
  (1, '{"user_id": 123, "action": "login"}', {'source': 'web', 'version': 1}),
  (2, '{"user_id": 456, "action": "purchase", "amount": 99.99}', {'source': 'mobile', 'version': 2})
""")

# Query JSON fields
{:ok, result} = DuckdbEx.query(conn, """
  SELECT
    id,
    data->>'user_id' as user_id,
    data->>'action' as action,
    metadata.source as source
  FROM events
""")
```

## Error Handling

### SQL Syntax Errors

```elixir
case DuckdbEx.query(conn, "SELCT * FROM products") do
  {:ok, result} ->
    # Process result
    rows = DuckdbEx.rows(result)

  {:error, reason} ->
    # Handle error
    IO.puts("SQL Error: #{reason}")
    # SQL Error: Parser Error: syntax error at or near "SELCT"
end
```

### Runtime Errors

```elixir
# Division by zero
case DuckdbEx.query(conn, "SELECT 1/0") do
  {:error, reason} ->
    IO.puts("Runtime error: #{reason}")
    # Runtime error: Constraint Error: Division by zero!
end

# Type conversion errors
case DuckdbEx.query(conn, "SELECT CAST('not-a-number' AS INTEGER)") do
  {:error, reason} ->
    IO.puts("Conversion error: #{reason}")
end
```

## Performance Considerations

### Query Optimization

```elixir
# Use EXPLAIN to understand query plans
{:ok, result} = DuckdbEx.query(conn, "EXPLAIN SELECT * FROM products WHERE category = 'Electronics'")
plan = DuckdbEx.rows(result)
Enum.each(plan, fn [line] -> IO.puts(line) end)

# Use EXPLAIN ANALYZE for execution statistics
{:ok, result} = DuckdbEx.query(conn, "EXPLAIN ANALYZE SELECT COUNT(*) FROM products")
```

### Large Result Sets

For large result sets, consider using the [Chunked API](chunked_api.md) instead:

```elixir
# Regular API - loads all data into memory
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM large_table")
all_rows = DuckdbEx.rows(result)  # Can use significant memory

# Better for large datasets - use chunked API
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM large_table")
process_chunks(result, conn)
```

## Transaction Integration

Queries automatically participate in transactions:

```elixir
alias DuckdbEx.Transaction

{:ok, result} = Transaction.with_transaction(conn, fn ->
  {:ok, _} = DuckdbEx.query(conn, "INSERT INTO products (name, price) VALUES ('New Product', 49.99)")
  {:ok, _} = DuckdbEx.query(conn, "UPDATE products SET category = 'New' WHERE name = 'New Product'")
  {:ok, "Transaction completed"}
end)

case result do
  {:ok, message} -> IO.puts(message)
  {:error, reason} -> IO.puts("Transaction failed: #{reason}")
end
```

## Query Parameterization

While the basic query API doesn't support parameterization directly, use [Prepared Statements](prepared_statements.md) for parameterized queries:

```elixir
# Instead of string interpolation (vulnerable to SQL injection)
# DuckdbEx.query(conn, "SELECT * FROM users WHERE name = '#{user_input}'")

# Use prepared statements
{:ok, stmt} = DuckdbEx.PreparedStatement.prepare(conn, "SELECT * FROM users WHERE name = $1")
{:ok, result} = DuckdbEx.PreparedStatement.execute(stmt, [user_input])
```

## Next Steps

- Learn about [Chunked API](chunked_api.md) for processing large datasets efficiently
- Explore [Prepared Statements](prepared_statements.md) for parameterized queries
- See [Data Types](data_types.md) for detailed type conversion information
