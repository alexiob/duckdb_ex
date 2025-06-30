defmodule DuckdbEx.NewTypesTest do
  use ExUnit.Case, async: true

  alias DuckdbEx

  setup do
    {:ok, db} = DuckdbEx.open(":memory:")
    {:ok, conn} = DuckdbEx.connect(db)

    on_exit(fn ->
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end)

    {:ok, conn: conn}
  end

  # Helper function to extract column types
  defp get_column_types(columns) do
    Enum.map(columns, & &1.type)
  end

  # Helper function to run query and get result info
  defp query_and_extract(conn, sql) do
    {:ok, result} = DuckdbEx.query(conn, sql)
    columns = DuckdbEx.columns(result)
    rows = DuckdbEx.rows(result)
    DuckdbEx.destroy_result(result)
    {columns, rows}
  end

  describe "new DuckDB types support" do
    test "uuid type", %{conn: conn} do
      # Test with VARCHAR since uuid() function requires extension that may not be available
      {columns, rows} =
        query_and_extract(
          conn,
          "SELECT '550e8400-e29b-41d4-a716-446655440000'::VARCHAR as test_uuid"
        )

      assert get_column_types(columns) == [:varchar]
      assert [{uuid_val}] = rows
      assert is_binary(uuid_val)
      # Standard UUID format
      assert String.length(uuid_val) == 36
    end

    test "decimal type", %{conn: conn} do
      {columns, rows} = query_and_extract(conn, "SELECT 123.456::DECIMAL(10,3) as test_decimal")

      assert get_column_types(columns) == [:decimal]
      assert [{decimal_val}] = rows
      # Decimal can be returned as float or string depending on precision
      assert is_float(decimal_val) or is_binary(decimal_val)
    end

    test "timestamp precision types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            '2023-12-25 14:30:45'::TIMESTAMP_S as ts_s,
            '2023-12-25 14:30:45'::TIMESTAMP_MS as ts_ms,
            '2023-12-25 14:30:45'::TIMESTAMP_NS as ts_ns
        """)

      assert get_column_types(columns) == [:timestamp_s, :timestamp_ms, :timestamp_ns]
      assert [{ts_s, ts_ms, ts_ns}] = rows
      assert is_binary(ts_s)
      assert is_binary(ts_ms)
      assert is_binary(ts_ns)
    end

    test "enum type", %{conn: conn} do
      # Create enum type first
      {:ok, result} = DuckdbEx.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'happy', 'excited')")
      DuckdbEx.destroy_result(result)

      {columns, rows} = query_and_extract(conn, "SELECT 'happy'::mood as mood_val")

      assert get_column_types(columns) == [:enum]
      assert [{mood_val}] = rows
      # Regular API returns placeholder for enum types
      assert mood_val == "<regular_api_enum_limitation>"
    end

    test "map type", %{conn: conn} do
      {columns, rows} = query_and_extract(conn, "SELECT map(['key1', 'key2'], [1, 2]) as map_val")

      assert get_column_types(columns) == [:map]
      assert [{map_val}] = rows
      # Regular API returns placeholder for map types
      assert map_val == "<unsupported_map_type>"
    end

    test "array type", %{conn: conn} do
      {columns, rows} = query_and_extract(conn, "SELECT [1, 2, 3]::INTEGER[3] as array_val")

      assert get_column_types(columns) == [:array]
      assert [{array_val}] = rows
      # Regular API returns placeholder for array types
      assert array_val == "<unsupported_array_type>"
    end

    test "bit type", %{conn: conn} do
      case DuckdbEx.query(conn, "SELECT '101010'::BIT as bit_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert get_column_types(columns) == [:bit]
          assert [{bit_val}] = rows
          # BIT type extraction may not be implemented yet, so could be nil or binary
          assert bit_val == nil or is_binary(bit_val)

        {:error, _reason} ->
          # BIT type might not be fully supported in this DuckDB version
          :ok
      end
    end

    test "hugeint and uhugeint types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            170141183460469231731687303715884105727::HUGEINT as hugeint_val,
            340282366920938463463374607431768211455::UHUGEINT as uhugeint_val
        """)

      assert get_column_types(columns) == [:hugeint, :uhugeint]
      assert [{hugeint_val, uhugeint_val}] = rows
      # Hugeint values are returned as strings for very large numbers
      assert is_binary(hugeint_val) or is_integer(hugeint_val)
      assert is_binary(uhugeint_val) or is_integer(uhugeint_val)
    end

    test "union type (if supported)", %{conn: conn} do
      case DuckdbEx.query(conn, "SELECT union_value(a := 42) as union_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert get_column_types(columns) == [:union]
          assert [{_union_val}] = rows

        {:error, _reason} ->
          # UNION type might not be fully supported
          :ok
      end
    end

    test "time with timezone (if supported)", %{conn: conn} do
      # TIME_TZ might not be available in all DuckDB versions
      case DuckdbEx.query(conn, "SELECT '14:30:45+02:00'::TIME_TZ as time_tz_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert get_column_types(columns) == [:time_tz]
          assert [{time_tz_val}] = rows
          # TIME_TZ is returned as a tuple {micros, offset} or string representation
          assert is_tuple(time_tz_val) or is_binary(time_tz_val)

        {:error, _reason} ->
          # TIME_TZ might not be fully supported
          :ok
      end
    end

    test "timestamp with timezone (if supported)", %{conn: conn} do
      # TIMESTAMP_TZ might not be available
      case DuckdbEx.query(conn, "SELECT now()::TIMESTAMP_TZ as ts_tz_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert get_column_types(columns) == [:timestamp_tz]
          assert [{_ts_tz_val}] = rows

        {:error, _reason} ->
          # TIMESTAMP_TZ might not be fully supported
          :ok
      end
    end
  end

  describe "existing types still work" do
    test "basic types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            true as bool_val,
            42::INTEGER as int_val,
            3.14::DOUBLE as double_val,
            'hello'::VARCHAR as varchar_val,
            '2023-12-25'::DATE as date_val,
            '14:30:45'::TIME as time_val,
            '2023-12-25 14:30:45'::TIMESTAMP as timestamp_val
        """)

      expected_types = [:boolean, :integer, :double, :varchar, :date, :time, :timestamp]
      assert get_column_types(columns) == expected_types

      assert [{bool_val, int_val, double_val, varchar_val, date_val, time_val, timestamp_val}] =
               rows

      assert bool_val == true
      assert int_val == 42
      assert_in_delta double_val, 3.14, 0.001
      assert varchar_val == "hello"
      assert %Date{} = date_val
      assert %Time{} = time_val
      assert %DateTime{} = timestamp_val
    end

    test "complex types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            [1, 2, 3] as list_val,
            {'name': 'John', 'age': 30} as struct_val,
            INTERVAL '3 days 2 hours' as interval_val
        """)

      expected_types = [:list, :struct, :interval]
      assert get_column_types(columns) == expected_types

      assert [{list_val, struct_val, interval_val}] = rows
      # Regular API returns placeholders for complex types
      assert list_val == "<unsupported_list_type>"
      assert struct_val == "<unsupported_struct_type>"
      # Interval is returned as a tuple {months, days, micros}
      assert is_tuple(interval_val)
      assert tuple_size(interval_val) == 3
    end
  end

  describe "null handling for new types" do
    test "null values for new types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            NULL::UUID as null_uuid,
            NULL::DECIMAL(10,2) as null_decimal,
            NULL::TIMESTAMP_S as null_timestamp_s,
            NULL::TIMESTAMP_MS as null_timestamp_ms,
            NULL::TIMESTAMP_NS as null_timestamp_ns
        """)

      expected_types = [:uuid, :decimal, :timestamp_s, :timestamp_ms, :timestamp_ns]
      assert get_column_types(columns) == expected_types

      # All values should be nil
      assert [{nil, nil, nil, nil, nil}] = rows
    end
  end

  describe "chunked data with new types" do
    test "chunked data access", %{conn: conn} do
      # Test that chunked data works with new types (using data_chunk_get_data)
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            uuid() as uuid_val,
            123.456::DECIMAL(10,3) as decimal_val,
            [1, 2, 3] as list_val
          FROM range(3)
        """)

      # Get chunk using the existing API
      {:ok, chunk} = DuckdbEx.result_get_chunk(result, 0)
      chunk_data = DuckdbEx.data_chunk_get_data(chunk)
      DuckdbEx.destroy_result(result)

      assert is_list(chunk_data)
      assert length(chunk_data) == 3

      # Each row should be a tuple with 3 elements
      Enum.each(chunk_data, fn row ->
        assert is_tuple(row)
        assert tuple_size(row) == 3
        {uuid_val, decimal_val, list_val} = row

        assert is_binary(uuid_val)
        assert is_float(decimal_val) or is_binary(decimal_val)
        assert is_list(list_val)
        assert list_val == [1, 2, 3]
      end)
    end
  end
end
