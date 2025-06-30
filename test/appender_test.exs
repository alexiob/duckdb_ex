defmodule DuckdbEx.AppenderTest do
  use ExUnit.Case
  alias DuckdbEx.Appender

  setup do
    {:ok, db} = DuckdbEx.open(":memory:")
    {:ok, conn} = DuckdbEx.connect(db)
    %{conn: conn, db: db}
  end

  describe "basic appender operations" do
    test "create and destroy appender", %{conn: conn} do
      # Create test table
      {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE test_basic (id INTEGER, name VARCHAR)")

      # Create appender
      {:ok, appender} = Appender.create(conn, nil, "test_basic")
      assert is_reference(appender)

      # Destroy appender
      :ok = Appender.destroy(appender)
    end

    test "get column count", %{conn: conn} do
      # Create test table with 3 columns
      {:ok, _result} =
        DuckdbEx.query(conn, "CREATE TABLE test_columns (id INTEGER, name VARCHAR, age INTEGER)")

      {:ok, appender} = Appender.create(conn, nil, "test_columns")
      assert Appender.column_count(appender) == 3

      :ok = Appender.destroy(appender)
    end

    test "create with schema", %{conn: conn} do
      # Create schema and table
      {:ok, _result} = DuckdbEx.query(conn, "CREATE SCHEMA test_schema")

      {:ok, _result} =
        DuckdbEx.query(conn, "CREATE TABLE test_schema.test_table (id INTEGER, name VARCHAR)")

      # Create appender with schema
      {:ok, appender} = Appender.create(conn, "test_schema", "test_table")
      assert is_reference(appender)

      :ok = Appender.destroy(appender)
    end

    test "create_ext with catalog and schema", %{conn: conn} do
      # Create test table
      {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE test_ext (id INTEGER, name VARCHAR)")

      # Create appender with extended parameters
      {:ok, appender} = Appender.create_ext(conn, nil, nil, "test_ext")
      assert is_reference(appender)

      :ok = Appender.destroy(appender)
    end
  end

  describe "appending data" do
    test "append simple integer and string data", %{conn: conn} do
      # Create test table
      {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE users (id INTEGER, name VARCHAR)")

      # Create appender
      {:ok, appender} = Appender.create(conn, nil, "users")

      # Append first row
      :ok = Appender.append_int32(appender, 1)
      :ok = Appender.append_varchar(appender, "Alice")
      :ok = Appender.end_row(appender)

      # Append second row
      :ok = Appender.append_int32(appender, 2)
      :ok = Appender.append_varchar(appender, "Bob")
      :ok = Appender.end_row(appender)

      # Close and destroy
      :ok = Appender.close(appender)
      :ok = Appender.destroy(appender)

      # Verify data was inserted
      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM users ORDER BY id")
      rows = DuckdbEx.rows(result)
      assert [{1, "Alice"}, {2, "Bob"}] = rows
    end

    test "append different data types", %{conn: conn} do
      {:ok, _result} =
        DuckdbEx.query(
          conn,
          "CREATE TABLE mixed_types (bool_col BOOLEAN, int8_col TINYINT, int16_col SMALLINT, int32_col INTEGER, int64_col BIGINT, float_col FLOAT, double_col DOUBLE, varchar_col VARCHAR, blob_col BLOB)"
        )

      {:ok, appender} = Appender.create(conn, nil, "mixed_types")

      # Append a row with different types
      :ok = Appender.append_bool(appender, true)
      :ok = Appender.append_int8(appender, 127)
      :ok = Appender.append_int16(appender, 32767)
      :ok = Appender.append_int32(appender, 2_147_483_647)
      :ok = Appender.append_int64(appender, 9_223_372_036_854_775_807)
      :ok = Appender.append_float(appender, 3.14)
      :ok = Appender.append_double(appender, 2.71828)
      :ok = Appender.append_varchar(appender, "test string")
      :ok = Appender.append_blob(appender, <<1, 2, 3, 4>>)
      :ok = Appender.end_row(appender)

      :ok = Appender.close(appender)
      :ok = Appender.destroy(appender)

      # Verify data
      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM mixed_types")
      rows = DuckdbEx.rows(result)
      assert length(rows) == 1

      [
        {bool_val, int8_val, int16_val, int32_val, int64_val, float_val, double_val, varchar_val,
         blob_val}
      ] =
        rows

      assert bool_val == true
      assert int8_val == 127
      assert int16_val == 32767
      assert int32_val == 2_147_483_647
      assert int64_val == 9_223_372_036_854_775_807
      assert is_float(float_val) and abs(float_val - 3.14) < 0.01
      assert is_float(double_val) and abs(double_val - 2.71828) < 0.00001
      assert varchar_val == "test string"
      assert blob_val == <<1, 2, 3, 4>>
    end

    test "append unsigned integers", %{conn: conn} do
      {:ok, _result} =
        DuckdbEx.query(
          conn,
          "CREATE TABLE unsigned_types (uint8_col UTINYINT, uint16_col USMALLINT, uint32_col UINTEGER, uint64_col UBIGINT)"
        )

      {:ok, appender} = Appender.create(conn, nil, "unsigned_types")

      :ok = Appender.append_uint8(appender, 255)
      :ok = Appender.append_uint16(appender, 65535)
      :ok = Appender.append_uint32(appender, 4_294_967_295)
      :ok = Appender.append_uint64(appender, 18_446_744_073_709_551_615)
      :ok = Appender.end_row(appender)

      :ok = Appender.close(appender)
      :ok = Appender.destroy(appender)

      # Verify data
      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM unsigned_types")
      rows = DuckdbEx.rows(result)
      assert [{255, 65535, 4_294_967_295, 18_446_744_073_709_551_615}] = rows
    end

    test "append null values", %{conn: conn} do
      {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE nulls (id INTEGER, name VARCHAR)")

      {:ok, appender} = Appender.create(conn, nil, "nulls")

      # Append row with nulls
      :ok = Appender.append_int32(appender, 1)
      :ok = Appender.append_null(appender)
      :ok = Appender.end_row(appender)

      :ok = Appender.append_null(appender)
      :ok = Appender.append_varchar(appender, "not null")
      :ok = Appender.end_row(appender)

      :ok = Appender.close(appender)
      :ok = Appender.destroy(appender)

      # Verify data
      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM nulls ORDER BY id NULLS LAST")
      rows = DuckdbEx.rows(result)
      assert [{1, nil}, {nil, "not null"}] = rows
    end
  end

  describe "high-level helpers" do
    test "append_row helper", %{conn: conn} do
      {:ok, _result} =
        DuckdbEx.query(conn, "CREATE TABLE helper_test (id INTEGER, name VARCHAR, age INTEGER)")

      {:ok, appender} = Appender.create(conn, nil, "helper_test")

      # Use append_row helper
      :ok = Appender.append_row(appender, [1, "Alice", 30])
      :ok = Appender.append_row(appender, [2, "Bob", 25])

      :ok = Appender.close(appender)
      :ok = Appender.destroy(appender)

      # Verify data
      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM helper_test ORDER BY id")
      rows = DuckdbEx.rows(result)
      assert [{1, "Alice", 30}, {2, "Bob", 25}] = rows
    end

    test "append_rows helper", %{conn: conn} do
      {:ok, _result} =
        DuckdbEx.query(conn, "CREATE TABLE batch_test (id INTEGER, name VARCHAR, score DOUBLE)")

      {:ok, appender} = Appender.create(conn, nil, "batch_test")

      rows_to_insert = [
        [1, "Alice", 95.5],
        [2, "Bob", 87.2],
        [3, "Charlie", 92.8]
      ]

      # Use append_rows helper
      :ok = Appender.append_rows(appender, rows_to_insert)

      :ok = Appender.close(appender)
      :ok = Appender.destroy(appender)

      # Verify data
      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM batch_test ORDER BY id")
      rows = DuckdbEx.rows(result)
      assert length(rows) == 3

      [{1, "Alice", score1}, {2, "Bob", score2}, {3, "Charlie", score3}] = rows
      assert abs(score1 - 95.5) < 0.01
      assert abs(score2 - 87.2) < 0.01
      assert abs(score3 - 92.8) < 0.01
    end

    test "insert_rows convenience function", %{conn: conn} do
      {:ok, _result} =
        DuckdbEx.query(conn, "CREATE TABLE convenience_test (id INTEGER, name VARCHAR)")

      rows_to_insert = [
        [1, "Alice"],
        [2, "Bob"],
        [3, "Charlie"]
      ]

      # Use convenience function that handles appender lifecycle
      :ok = Appender.insert_rows(conn, nil, "convenience_test", rows_to_insert)

      # Verify data
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM convenience_test")
      rows = DuckdbEx.rows(result)
      assert [{3}] = rows
    end
  end

  describe "error handling" do
    test "create appender for non-existent table", %{conn: conn} do
      {:error, reason} = Appender.create(conn, nil, "non_existent_table")
      assert is_binary(reason)

      assert String.contains?(reason, "could not be found") or
               String.contains?(reason, "does not exist") or String.contains?(reason, "not found")
    end

    test "append wrong data type", %{conn: conn} do
      {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE type_error_test (id INTEGER)")

      {:ok, appender} = Appender.create(conn, nil, "type_error_test")

      # Try to append string to integer column
      {:error, reason} = Appender.append_varchar(appender, "not an integer")
      assert is_binary(reason)

      :ok = Appender.destroy(appender)
    end

    test "value out of range errors", %{conn: conn} do
      {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE range_test (tiny TINYINT)")

      {:ok, appender} = Appender.create(conn, nil, "range_test")

      # Try to append value out of range for int8
      {:error, reason} = Appender.append_int8(appender, 300)
      assert String.contains?(reason, "out of range")

      :ok = Appender.destroy(appender)
    end
  end

  describe "flush operations" do
    test "flush appender manually", %{conn: conn} do
      {:ok, _result} = DuckdbEx.query(conn, "CREATE TABLE flush_test (id INTEGER)")

      {:ok, appender} = Appender.create(conn, nil, "flush_test")

      :ok = Appender.append_int32(appender, 1)
      :ok = Appender.end_row(appender)

      # Flush manually
      :ok = Appender.flush(appender)

      :ok = Appender.append_int32(appender, 2)
      :ok = Appender.end_row(appender)

      :ok = Appender.close(appender)
      :ok = Appender.destroy(appender)

      # Verify data
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM flush_test")
      rows = DuckdbEx.rows(result)
      assert [{2}] = rows
    end
  end
end
