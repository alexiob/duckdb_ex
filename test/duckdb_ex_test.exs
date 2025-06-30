defmodule DuckdbExTest do
  use ExUnit.Case

  setup do
    # Use in-memory database for tests
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    on_exit(fn ->
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end)

    %{db: db, conn: conn}
  end

  test "opens and closes database", %{db: db} do
    assert is_reference(db)
  end

  test "opens and closes connection", %{conn: conn} do
    assert is_reference(conn)
  end

  test "executes simple query", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer, 'hello' as greeting")

    columns = DuckdbEx.columns(result)
    assert length(columns) == 2
    assert Enum.any?(columns, &(&1.name == "answer"))
    assert Enum.any?(columns, &(&1.name == "greeting"))

    rows = DuckdbEx.rows(result)
    assert length(rows) == 1
    assert {42, "hello"} = hd(rows)

    assert DuckdbEx.row_count(result) == 1
    assert DuckdbEx.column_count(result) == 2

    DuckdbEx.destroy_result(result)
  end

  test "handles query errors", %{conn: conn} do
    {:error, reason} = DuckdbEx.query(conn, "SELECT FROM invalid_table")
    assert is_binary(reason)
  end

  test "creates and queries table", %{conn: conn} do
    # Create table
    {:ok, _result} =
      DuckdbEx.query(conn, """
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name VARCHAR,
          age INTEGER
        )
      """)

    # Insert data
    {:ok, _result} =
      DuckdbEx.query(conn, """
        INSERT INTO users VALUES
          (1, 'Alice', 30),
          (2, 'Bob', 25),
          (3, 'Charlie', 35)
      """)

    # Query data
    {:ok, result} =
      DuckdbEx.query(conn, "SELECT name, age FROM users WHERE age > 28 ORDER BY age")

    rows = DuckdbEx.rows(result)
    assert length(rows) == 2
    assert {"Alice", 30} in rows
    assert {"Charlie", 35} in rows

    DuckdbEx.destroy_result(result)
  end

  test "prepared statements", %{conn: conn} do
    # Create test table
    {:ok, _result} =
      DuckdbEx.query(conn, """
        CREATE TABLE test_users (
          id INTEGER,
          name VARCHAR,
          score DOUBLE
        )
      """)

    # Insert test data
    {:ok, _result} =
      DuckdbEx.query(conn, """
        INSERT INTO test_users VALUES
          (1, 'Alice', 95.5),
          (2, 'Bob', 87.2),
          (3, 'Charlie', 92.1)
      """)

    # Prepare statement
    {:ok, stmt} = DuckdbEx.prepare(conn, "SELECT name FROM test_users WHERE score > ?")

    # Execute with parameters
    {:ok, result} = DuckdbEx.execute(stmt, [90.0])

    # Should return Alice and Charlie (both have scores > 90)
    rows = DuckdbEx.rows(result)
    assert length(rows) == 2
    names = Enum.map(rows, fn {name} -> name end)
    assert "Alice" in names
    assert "Charlie" in names

    DuckdbEx.destroy_result(result)
    DuckdbEx.destroy_prepared_statement(stmt)
  end

  test "file database" do
    # Test file-based database
    db_path = "/tmp/test_duckdb_#{:rand.uniform(1_000_000)}.db"

    # Ensure cleanup
    on_exit(fn -> File.rm(db_path) end)

    {:ok, db} = DuckdbEx.open(db_path)
    {:ok, conn} = DuckdbEx.connect(db)

    # Create and query table
    {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE test (id INTEGER, value VARCHAR)")
    {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO test VALUES (1, 'persistent')")

    {:ok, result} = DuckdbEx.query(conn, "SELECT value FROM test WHERE id = 1")
    rows = DuckdbEx.rows(result)
    assert {_} = hd(rows)

    DuckdbEx.destroy_result(result)
    DuckdbEx.close_connection(conn)
    DuckdbEx.close_database(db)

    # Verify file was created
    assert File.exists?(db_path)
  end

  test "data types", %{conn: conn} do
    {:ok, result} =
      DuckdbEx.query(conn, """
        SELECT
          true as bool_val,
          42::TINYINT as tiny_val,
          12345::SMALLINT as small_val,
          987654321::INTEGER as int_val,
          9876543210::BIGINT as big_val,
          3.14::FLOAT as float_val,
          2.71828::DOUBLE as double_val,
          'hello world' as string_val
      """)

    columns = DuckdbEx.columns(result)
    assert length(columns) == 8

    # Verify column types are properly detected
    type_map = Enum.into(columns, %{}, &{&1.name, &1.type})
    assert type_map["bool_val"] == :boolean
    assert type_map["tiny_val"] == :tinyint
    assert type_map["small_val"] == :smallint
    assert type_map["int_val"] == :integer
    assert type_map["big_val"] == :bigint
    assert type_map["float_val"] == :float
    assert type_map["double_val"] == :double
    assert type_map["string_val"] == :varchar

    DuckdbEx.destroy_result(result)
  end

  test "chunked API works", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer, 'hello' as greeting")

    # Test the chunked API
    rows = DuckdbEx.rows_chunked(result)
    assert [{42, "hello"}] = rows
  end

  test "chunked API handles arrays", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as id, [1, 2, 3] as arr")

    # Test both regular and chunked APIs
    regular_rows = DuckdbEx.rows(result)

    {:ok, result2} = DuckdbEx.query(conn, "SELECT 1 as id, [1, 2, 3] as arr")
    chunked_rows = DuckdbEx.rows_chunked(result2)

    # Check what we actually get from regular API
    [{id, _arr_value}] = regular_rows
    assert id == 1
    # Regular API might return nil for arrays (unsupported)

    # Chunked rows should return tuples with arrays as Elixir lists
    [{chunked_id, chunked_arr}] = chunked_rows
    assert chunked_id == 1
    assert is_list(chunked_arr)
    assert chunked_arr == [1, 2, 3]
  end

  test "chunked API handles complex nested arrays", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT [[1, 2], [3, 4, 5]] as nested_arr")

    chunked_rows = DuckdbEx.rows_chunked(result)

    [{nested_arr}] = chunked_rows
    assert is_list(nested_arr)
    assert length(nested_arr) == 2
    assert nested_arr == [[1, 2], [3, 4, 5]]
  end
end
