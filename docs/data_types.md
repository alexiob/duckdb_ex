# Data Types

DuckdbEx provides comprehensive support for DuckDB's rich type system, with automatic conversion between Elixir and DuckDB types. This guide covers all supported data types and their behavior in both the regular and chunked APIs.

## Type Support Overview

| DuckDB Type | Elixir Type | Regular API | Chunked API | Notes |
|-------------|-------------|-------------|-------------|-------|
| `BOOLEAN` | `boolean()` | ✅ | ✅ | Direct mapping |
| `TINYINT` | `integer()` | ✅ | ✅ | -128 to 127 |
| `SMALLINT` | `integer()` | ✅ | ✅ | -32,768 to 32,767 |
| `INTEGER` | `integer()` | ✅ | ✅ | -2^31 to 2^31-1 |
| `BIGINT` | `integer()` | ✅ | ✅ | -2^63 to 2^63-1 |
| `HUGEINT` | `integer()` | ✅ | ✅ | 128-bit integer |
| `FLOAT` | `float()` | ✅ | ✅ | 32-bit floating point |
| `DOUBLE` | `float()` | ✅ | ✅ | 64-bit floating point |
| `DECIMAL` | `Decimal.t()` | ✅ | ✅ | Requires decimal library |
| `VARCHAR` | `String.t()` | ✅ | ✅ | UTF-8 strings |
| `BLOB` | `binary()` | ✅ | ✅ | Binary data |
| `DATE` | `Date.t()` | ✅ | ✅ | Calendar dates |
| `TIME` | `Time.t()` | ✅ | ✅ | Time of day |
| `TIMESTAMP` | `NaiveDateTime.t()` | ✅ | ✅ | Date and time |
| `INTERVAL` | `map()` | ✅ | ✅ | Time intervals |
| `ARRAY` | `list()` | ⚠️ | ✅ | Best in chunked API |
| `LIST` | `list()` | ⚠️ | ✅ | Best in chunked API |
| `STRUCT` | `map()` | ⚠️ | ✅ | Best in chunked API |
| `MAP` | `map()` | ⚠️ | ✅ | Best in chunked API |
| `UNION` | `any()` | ⚠️ | ✅ | Best in chunked API |
| `ENUM` | `String.t()` | ⚠️ | ✅ | String representation |
| `UUID` | `String.t()` | ✅ | ✅ | String format |

⚠️ = Not supported in the regular API, best used with the chunked API.

## Primitive Types

### Boolean

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE bool_test (
    id INTEGER,
    flag BOOLEAN,
    status BOOLEAN DEFAULT true
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO bool_test VALUES
  (1, true, false),
  (2, false, true),
  (3, NULL, NULL)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM bool_test")
rows = DuckdbEx.rows(result)
# [[1, true, false], [2, false, true], [3, nil, nil]]
```

### Integer Types

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE int_test (
    tiny_col TINYINT,
    small_col SMALLINT,
    int_col INTEGER,
    big_col BIGINT,
    huge_col HUGEINT
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO int_test VALUES
  (127, 32767, 2147483647, 9223372036854775807, 170141183460469231731687303715884105727)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM int_test")
[[tiny, small, int, big, huge]] = DuckdbEx.rows(result)

IO.puts("TINYINT: #{tiny} (#{byte_size(<<tiny>>)} bytes)")
IO.puts("SMALLINT: #{small}")
IO.puts("INTEGER: #{int}")
IO.puts("BIGINT: #{big}")
IO.puts("HUGEINT: #{huge}")
```

### Floating Point Types

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE float_test (
    float_col FLOAT,
    double_col DOUBLE,
    real_col REAL
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO float_test VALUES
  (3.14159, 2.718281828459045, 1.414213562373095)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM float_test")
[[float_val, double_val, real_val]] = DuckdbEx.rows(result)

# All are returned as Elixir floats
IO.puts("FLOAT: #{float_val}")
IO.puts("DOUBLE: #{double_val}")
IO.puts("REAL: #{real_val}")
```

### Decimal Types

```elixir
# Note: Requires the :decimal dependency in your mix.exs
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE decimal_test (
    price DECIMAL(10,2),
    rate DECIMAL(5,4),
    big_number DECIMAL(38,10)
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO decimal_test VALUES
  (999.99, 0.1250, 12345678901234567890.1234567890)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM decimal_test")
[[price, rate, big_number]] = DuckdbEx.rows(result)

# Returns Decimal structs
IO.puts("Price: #{Decimal.to_string(price)}")
IO.puts("Rate: #{Decimal.to_string(rate)}")
IO.puts("Big number: #{Decimal.to_string(big_number)}")
```

### String Types

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE string_test (
    short_text VARCHAR(50),
    long_text TEXT,
    unlimited_text VARCHAR
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO string_test VALUES
  ('Short string', 'This is a longer text field', 'Unlimited length string with émojis 🚀✨')
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM string_test")
[[short, long, unlimited]] = DuckdbEx.rows(result)

# All return as UTF-8 Elixir strings
IO.puts("Short: #{short}")
IO.puts("Long: #{long}")
IO.puts("Unlimited: #{unlimited}")
```

### Binary Types

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE blob_test (
    id INTEGER,
    data BLOB
  )
""")

# Insert binary data (use encode to convert string to blob in SQL)
{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO blob_test VALUES
  (1, encode('Hello, World!', 'utf8')),
  (2, '\\x48656C6C6F'::BLOB)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM blob_test")
rows = DuckdbEx.rows(result)

Enum.each(rows, fn [id, blob_data] ->
  IO.puts("ID: #{id}, Data: #{inspect(blob_data)}")
  # Try to decode as string
  case String.valid?(blob_data) do
    true -> IO.puts("  As string: #{blob_data}")
    false -> IO.puts("  Binary data: #{byte_size(blob_data)} bytes")
  end
end)
```

## Temporal Types

### Date and Time

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE temporal_test (
    event_date DATE,
    event_time TIME,
    event_timestamp TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO temporal_test (event_date, event_time, event_timestamp) VALUES
  ('2024-01-15', '14:30:45', '2024-01-15 14:30:45.123456'),
  ('2024-12-25', '23:59:59', '2024-12-25 23:59:59.999999')
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM temporal_test")
rows = DuckdbEx.rows(result)

Enum.each(rows, fn [date, time, timestamp, created_at] ->
  IO.puts("Date: #{Date.to_string(date)}")
  IO.puts("Time: #{Time.to_string(time)}")
  IO.puts("Timestamp: #{NaiveDateTime.to_string(timestamp)}")
  IO.puts("Created at: #{NaiveDateTime.to_string(created_at)}")
  IO.puts("---")
end)
```

### Intervals

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE interval_test (
    name VARCHAR,
    duration INTERVAL
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO interval_test VALUES
  ('Short period', INTERVAL '5 days'),
  ('Medium period', INTERVAL '2 months 15 days'),
  ('Long period', INTERVAL '1 year 6 months 10 days 5 hours 30 minutes'),
  ('Negative period', INTERVAL '-3 days 2 hours')
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM interval_test")
rows = DuckdbEx.rows(result)

Enum.each(rows, fn [name, interval] ->
  IO.puts("#{name}: #{inspect(interval)}")
  # Interval is returned as a map with months, days, and microseconds
end)
```

### Timezone Handling

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE tz_test (
    utc_time TIMESTAMP,
    local_time TIMESTAMPTZ,
    time_with_tz TIMETZ
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO tz_test VALUES
  ('2024-01-15 14:30:45', '2024-01-15 14:30:45-08:00', '14:30:45-08:00')
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM tz_test")
[[utc_time, local_time, time_with_tz]] = DuckdbEx.rows(result)

IO.puts("UTC time: #{inspect(utc_time)}")
IO.puts("Local time: #{inspect(local_time)}")
IO.puts("Time with TZ: #{inspect(time_with_tz)}")
```

## Complex Types (Best with Chunked API)

### Arrays and Lists

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE array_test (
    id INTEGER,
    fixed_array INTEGER[3],
    dynamic_list INTEGER[],
    string_list VARCHAR[]
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO array_test VALUES
  (1, [1, 2, 3], [10, 20, 30, 40], ['apple', 'banana', 'cherry']),
  (2, [4, 5, 6], [100], ['orange'])
""")

# Using chunked API for best complex type support
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM array_test")

case DuckdbEx.fetch_chunk(result, conn) do
  {:ok, chunk_data} ->
    Enum.each(chunk_data, fn [id, fixed_array, dynamic_list, string_list] ->
      IO.puts("ID: #{id}")
      IO.puts("Fixed array: #{inspect(fixed_array)}")
      IO.puts("Dynamic list: #{inspect(dynamic_list)}")
      IO.puts("String list: #{inspect(string_list)}")
      IO.puts("---")
    end)
end
```

### Structs (Records)

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE struct_test (
    id INTEGER,
    person STRUCT(name VARCHAR, age INTEGER, address STRUCT(street VARCHAR, city VARCHAR)),
    coordinates STRUCT(x DOUBLE, y DOUBLE, z DOUBLE)
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO struct_test VALUES
  (1,
   {'name': 'Alice', 'age': 30, 'address': {'street': '123 Main St', 'city': 'New York'}},
   {'x': 1.5, 'y': 2.7, 'z': -0.3}
  ),
  (2,
   {'name': 'Bob', 'age': 25, 'address': {'street': '456 Oak Ave', 'city': 'San Francisco'}},
   {'x': -2.1, 'y': 0.0, 'z': 4.4}
  )
""")

# Best with chunked API
{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM struct_test")

case DuckdbEx.fetch_chunk(result, conn) do
  {:ok, chunk_data} ->
    Enum.each(chunk_data, fn [id, person, coordinates] ->
      IO.puts("ID: #{id}")
      IO.puts("Person: #{inspect(person, pretty: true)}")
      IO.puts("Coordinates: #{inspect(coordinates)}")
      IO.puts("---")
    end)
end
```

### Maps

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE map_test (
    id INTEGER,
    properties MAP(VARCHAR, INTEGER),
    metadata MAP(VARCHAR, VARCHAR)
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO map_test VALUES
  (1,
   MAP(['height', 'width', 'depth'], [100, 200, 50]),
   MAP(['color', 'material'], ['red', 'wood'])
  ),
  (2,
   MAP(['length', 'width'], [300, 150]),
   MAP(['color', 'finish', 'style'], ['blue', 'matte', 'modern'])
  )
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM map_test")

case DuckdbEx.fetch_chunk(result, conn) do
  {:ok, chunk_data} ->
    Enum.each(chunk_data, fn [id, properties, metadata] ->
      IO.puts("ID: #{id}")
      IO.puts("Properties: #{inspect(properties)}")
      IO.puts("Metadata: #{inspect(metadata)}")
      IO.puts("---")
    end)
end
```

### Union Types

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE union_test (
    id INTEGER,
    value UNION(num INTEGER, text VARCHAR, flag BOOLEAN)
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO union_test VALUES
  (1, 42),
  (2, 'hello world'),
  (3, true),
  (4, 3.14)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM union_test")

case DuckdbEx.fetch_chunk(result, conn) do
  {:ok, chunk_data} ->
    Enum.each(chunk_data, fn [id, value] ->
      IO.puts("ID: #{id}")
      IO.puts("Value: #{inspect(value)} (type: #{inspect(value.__struct__ || :primitive)})")
      IO.puts("---")
    end)
end
```

## Special Types

### Enums

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')
""")

{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE enum_test (
    id INTEGER,
    user_mood mood,
    status ENUM('active', 'inactive', 'pending') DEFAULT 'pending'
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO enum_test VALUES
  (1, 'happy', 'active'),
  (2, 'sad', 'inactive'),
  (3, 'ok', DEFAULT)
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM enum_test")
rows = DuckdbEx.rows(result)

Enum.each(rows, fn [id, mood, status] ->
  IO.puts("User #{id}: mood=#{mood}, status=#{status}")
end)
```

### UUIDs

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE uuid_test (
    id UUID PRIMARY KEY,
    name VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO uuid_test (id, name) VALUES
  (gen_random_uuid(), 'Alice'),
  (gen_random_uuid(), 'Bob'),
  ('12345678-1234-5678-9abc-123456789abc', 'Charlie')
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM uuid_test")
rows = DuckdbEx.rows(result)

Enum.each(rows, fn [uuid, name, created_at] ->
  IO.puts("#{name}: #{uuid} (created: #{NaiveDateTime.to_string(created_at)})")
end)
```

## Type Conversion Utilities

### Manual Type Conversion

```elixir
defmodule TypeConverter do
  def convert_to_elixir_types(rows, columns) do
    column_types = Enum.map(columns, & &1.type)

    Enum.map(rows, fn row ->
      Enum.zip(row, column_types)
      |> Enum.map(&convert_value/1)
    end)
  end

  defp convert_value({nil, _type}), do: nil

  defp convert_value({value, :timestamp}) when is_binary(value) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, datetime} -> datetime
      {:error, _} -> value
    end
  end

  defp convert_value({value, :date}) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      {:error, _} -> value
    end
  end

  defp convert_value({value, :decimal}) when is_binary(value) do
    case Decimal.new(value) do
      %Decimal{} = decimal -> decimal
      _ -> value
    end
  end

  defp convert_value({value, _type}), do: value
end

# Example usage
{:ok, result} = DuckdbEx.query(conn, "SELECT id, name, created_at FROM some_table")
columns = DuckdbEx.columns(result)
rows = DuckdbEx.rows(result)

converted_rows = TypeConverter.convert_to_elixir_types(rows, columns)
```

### Handling NULL Values

```elixir
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE null_test (
    id INTEGER,
    optional_text VARCHAR,
    optional_number DOUBLE,
    optional_date DATE
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO null_test VALUES
  (1, 'text', 42.5, '2024-01-01'),
  (2, NULL, NULL, NULL),
  (3, '', 0.0, '1900-01-01')
""")

{:ok, result} = DuckdbEx.query(conn, "SELECT * FROM null_test")
rows = DuckdbEx.rows(result)

Enum.each(rows, fn [id, text, number, date] ->
  text_display = if text == nil, do: "<NULL>", else: "'#{text}'"
  number_display = if number == nil, do: "<NULL>", else: "#{number}"
  date_display = if date == nil, do: "<NULL>", else: Date.to_string(date)

  IO.puts("ID: #{id}, Text: #{text_display}, Number: #{number_display}, Date: #{date_display}")
end)
```

## Best Practices

1. **Use Chunked API for Complex Types**: Always use the chunked API when working with arrays, structs, maps, or unions
2. **Handle NULLs Gracefully**: Always check for `nil` values in your processing logic
3. **Understand Precision**: Be aware of floating-point precision limitations
4. **Use Appropriate Types**: Choose the most specific type that fits your data
5. **Consider Performance**: Simpler types generally perform better
6. **Validate Input**: Always validate data before insertion, especially for complex types

## Troubleshooting Type Issues

### Common Type Conversion Problems

```elixir
# Problem: String that looks like a number
{:ok, result} = DuckdbEx.query(conn, "SELECT '123' as text_number")
[[text_number]] = DuckdbEx.rows(result)
# text_number is "123" (string), not 123 (integer)

# Solution: Explicit casting in SQL
{:ok, result} = DuckdbEx.query(conn, "SELECT CAST('123' AS INTEGER) as real_number")
[[real_number]] = DuckdbEx.rows(result)
# real_number is 123 (integer)

# Problem: Complex type in regular API returns string
{:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3] as array_col")
[[array_string]] = DuckdbEx.rows(result)
# array_string might be "[1, 2, 3]" (string representation)

# Solution: Use chunked API for complex types
case DuckdbEx.fetch_chunk(result, conn) do
  {:ok, [[array_data]]} ->
    # array_data is [1, 2, 3] (actual list)
    IO.inspect(array_data)
end
```

## Next Steps

- Learn about [Chunked API](chunked_api.md) for optimal complex type handling
- Explore [Query API](query_api.md) for type-specific query patterns
- See [Examples](examples.md) for real-world type usage scenarios
