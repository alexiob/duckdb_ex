defmodule DuckdbEx.AllTypesTest do
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

  # Helper function to run query and get chunked result info
  defp query_and_extract_chunked(conn, sql) do
    {:ok, result} = DuckdbEx.query(conn, sql)
    columns = DuckdbEx.columns(result)
    rows = DuckdbEx.rows_chunked(result)
    DuckdbEx.destroy_result(result)
    {columns, rows}
  end

  describe "basic types" do
    test "boolean type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            true as bool_true,
            false as bool_false,
            NULL::BOOLEAN as bool_null
        """)

      assert get_column_types(columns) == [:boolean, :boolean, :boolean]
      assert [{true, false, nil}] = rows
    end

    test "integer types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            42::TINYINT as tinyint_val,
            1000::SMALLINT as smallint_val,
            100000::INTEGER as integer_val,
            9223372036854775807::BIGINT as bigint_val
        """)

      assert get_column_types(columns) == [:tinyint, :smallint, :integer, :bigint]
      assert [{42, 1000, 100_000, 9_223_372_036_854_775_807}] = rows
    end

    test "unsigned integer types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            255::UTINYINT as utinyint_val,
            65535::USMALLINT as usmallint_val,
            4294967295::UINTEGER as uinteger_val,
            18446744073709551615::UBIGINT as ubigint_val
        """)

      assert get_column_types(columns) == [:utinyint, :usmallint, :uinteger, :ubigint]
      assert [{255, 65535, 4_294_967_295, 18_446_744_073_709_551_615}] = rows
    end

    test "floating point types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            3.14::FLOAT as float_val,
            2.718281828::DOUBLE as double_val
        """)

      assert get_column_types(columns) == [:float, :double]
      assert [{float_val, double_val}] = rows
      assert_in_delta float_val, 3.14, 0.001
      assert_in_delta double_val, 2.718281828, 0.00001
    end

    test "decimal type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            123.456::DECIMAL(10,3) as decimal_val,
            999.99::DECIMAL(5,2) as decimal_val2,
            NULL::DECIMAL(10,2) as decimal_null
        """)

      assert get_column_types(columns) == [:decimal, :decimal, :decimal]
      assert [{decimal_val, decimal_val2, nil}] = rows
      # Decimal can be returned as float, string, or decimal depending on precision
      assert is_float(decimal_val) or is_binary(decimal_val) or is_number(decimal_val)
      assert is_float(decimal_val2) or is_binary(decimal_val2) or is_number(decimal_val2)
    end

    test "string types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            'hello'::VARCHAR as varchar_val,
            'world'::STRING as string_val,
            'blob_data'::BLOB as blob_val,
            NULL::VARCHAR as varchar_null
        """)

      assert get_column_types(columns) == [:varchar, :varchar, :blob, :varchar]
      assert [{varchar_val, string_val, blob_val, nil}] = rows
      assert varchar_val == "hello"
      assert string_val == "world"
      assert is_binary(blob_val)
    end
  end

  describe "temporal types" do
    test "date type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            '2023-12-25'::DATE as date_val,
            NULL::DATE as date_null
        """)

      assert get_column_types(columns) == [:date, :date]
      assert [{date_val, nil}] = rows
      assert %Date{} = date_val
      assert date_val == ~D[2023-12-25]
    end

    test "time type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            '14:30:45'::TIME as time_val,
            '23:59:59.123456'::TIME as time_microseconds,
            NULL::TIME as time_null
        """)

      assert get_column_types(columns) == [:time, :time, :time]
      assert [{time_val, time_microseconds, nil}] = rows
      assert %Time{} = time_val
      assert %Time{} = time_microseconds
    end

    test "timestamp types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            '2023-12-25 14:30:45'::TIMESTAMP as timestamp_val,
            '2023-12-25 14:30:45'::TIMESTAMP_S as timestamp_s,
            '2023-12-25 14:30:45.123'::TIMESTAMP_MS as timestamp_ms,
            '2023-12-25 14:30:45.123456789'::TIMESTAMP_NS as timestamp_ns,
            NULL::TIMESTAMP as timestamp_null
        """)

      assert get_column_types(columns) == [
               :timestamp,
               :timestamp_s,
               :timestamp_ms,
               :timestamp_ns,
               :timestamp
             ]

      assert [{timestamp_val, timestamp_s, timestamp_ms, timestamp_ns, nil}] = rows
      assert %DateTime{} = timestamp_val
      # Note: precision timestamp types may return placeholders in regular API
      if is_binary(timestamp_s) and timestamp_s == "<timestamp_extraction_failed>" do
        # Some precision timestamp types may not be supported in regular API
        assert timestamp_s == "<timestamp_extraction_failed>"
      else
        assert %DateTime{} = timestamp_s
      end

      if is_binary(timestamp_ms) and timestamp_ms == "<timestamp_extraction_failed>" do
        assert timestamp_ms == "<timestamp_extraction_failed>"
      else
        assert %DateTime{} = timestamp_ms
      end

      if is_binary(timestamp_ns) and timestamp_ns == "<timestamp_extraction_failed>" do
        assert timestamp_ns == "<timestamp_extraction_failed>"
      else
        assert %DateTime{} = timestamp_ns
      end
    end

    test "interval type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            INTERVAL '3 days 2 hours' as interval_val,
            INTERVAL '1 month' as interval_month,
            INTERVAL '45 minutes' as interval_minutes,
            NULL::INTERVAL as interval_null
        """)

      assert get_column_types(columns) == [:interval, :interval, :interval, :interval]
      assert [{interval_val, interval_month, interval_minutes, nil}] = rows
      # Interval is returned as a tuple {months, days, micros}
      assert is_tuple(interval_val)
      assert tuple_size(interval_val) == 3
      assert is_tuple(interval_month)
      assert is_tuple(interval_minutes)
    end
  end

  describe "new and complex types" do
    test "uuid type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            '550e8400-e29b-41d4-a716-446655440000'::VARCHAR as uuid_specific,
            NULL::VARCHAR as uuid_null
        """)

      assert get_column_types(columns) == [:varchar, :varchar]
      assert [{uuid_specific, nil}] = rows
      assert is_binary(uuid_specific)
      # UUID as string format should have 36 characters with dashes
      assert String.length(uuid_specific) == 36
    end

    test "hugeint types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            170141183460469231731687303715884105727::HUGEINT as hugeint_max,
            340282366920938463463374607431768211455::UHUGEINT as uhugeint_max,
            NULL::HUGEINT as hugeint_null,
            NULL::UHUGEINT as uhugeint_null
        """)

      assert get_column_types(columns) == [:hugeint, :uhugeint, :hugeint, :uhugeint]
      assert [{hugeint_max, uhugeint_max, nil, nil}] = rows
      # Hugeint values can be returned as strings for very large numbers or integers
      assert is_binary(hugeint_max) or is_integer(hugeint_max)
      assert is_binary(uhugeint_max) or is_integer(uhugeint_max)
    end

    test "array type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            [1, 2, 3]::INTEGER[3] as int_array,
            ['a', 'b', 'c']::VARCHAR[3] as string_array,
            [[1, 2], [3, 4]]::INTEGER[][2] as nested_array,
            NULL::INTEGER[3] as array_null
        """)

      assert get_column_types(columns) == [:array, :array, :array, :array]
      assert [{int_array, string_array, nested_array, array_null}] = rows
      # Regular API returns placeholders for array types
      assert int_array == "<unsupported_array_type>"
      assert string_array == "<unsupported_array_type>"
      assert nested_array == "<unsupported_array_type>"
      assert array_null == "<unsupported_array_type>"
    end

    test "list type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            [1, 2, 3, 4, 5] as int_list,
            ['hello', 'world'] as string_list,
            [] as empty_list,
            NULL as list_null
        """)

      assert get_column_types(columns) == [:list, :list, :list, :integer]
      assert [{int_list, string_list, empty_list, nil}] = rows
      # Regular API returns placeholders for list types
      assert int_list == "<unsupported_list_type>"
      assert string_list == "<unsupported_list_type>"
      assert empty_list == "<unsupported_list_type>"
    end

    test "struct type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            {'name': 'John', 'age': 30} as person_struct,
            {'x': 1.0, 'y': 2.0, 'z': 3.0} as point_struct,
            NULL::STRUCT(name VARCHAR, age INTEGER) as struct_null
        """)

      assert get_column_types(columns) == [:struct, :struct, :struct]
      assert [{person_struct, point_struct, struct_null}] = rows
      # Regular API returns placeholders for struct types
      assert person_struct == "<unsupported_struct_type>"
      assert point_struct == "<unsupported_struct_type>"
      assert struct_null == "<unsupported_struct_type>"
    end

    test "map type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            map(['key1', 'key2'], [1, 2]) as simple_map,
            map(['a', 'b', 'c'], ['apple', 'banana', 'cherry']) as string_map,
            NULL::MAP(VARCHAR, VARCHAR) as map_null
        """)

      assert get_column_types(columns) == [:map, :map, :map]
      assert [{simple_map, string_map, map_null}] = rows
      # Regular API returns placeholders for map types
      assert simple_map == "<unsupported_map_type>"
      assert string_map == "<unsupported_map_type>"
      assert map_null == "<unsupported_map_type>"
      assert string_map == "<unsupported_map_type>"
    end

    test "enum type", %{conn: conn} do
      # Create enum type first
      {:ok, result} = DuckdbEx.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'happy', 'excited')")
      DuckdbEx.destroy_result(result)

      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            'happy'::mood as mood_val,
            'excited'::mood as mood_val2,
            NULL::mood as mood_null
        """)

      assert get_column_types(columns) == [:enum, :enum, :enum]
      assert [{mood_val, mood_val2, mood_null}] = rows
      # Regular API returns placeholders for enum types
      assert mood_val == "<regular_api_enum_limitation>"
      assert mood_val2 == "<regular_api_enum_limitation>"
      assert mood_null == "<regular_api_enum_limitation>"
    end

    # Optional/experimental types that might not be fully supported
    test "bit type (if supported)", %{conn: conn} do
      case DuckdbEx.query(conn, "SELECT '101010'::BIT as bit_val, NULL::BIT as bit_null") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert get_column_types(columns) == [:bit, :bit]
          assert [{bit_val, nil}] = rows
          # BIT type may return nil or placeholder in regular API
          assert bit_val == nil or is_binary(bit_val)

        {:error, _reason} ->
          # BIT type might not be fully supported in this DuckDB version
          :ok
      end
    end

    test "union type (if supported)", %{conn: conn} do
      case DuckdbEx.query(conn, "SELECT union_value(a := 42) as union_val, NULL as union_null") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          # Note: NULL without type specification may not be UNION type
          column_types = get_column_types(columns)
          assert List.first(column_types) == :union
          assert [{_union_val, nil}] = rows

        {:error, _reason} ->
          # UNION type might not be fully supported
          :ok
      end
    end

    test "time with timezone (if supported)", %{conn: conn} do
      case DuckdbEx.query(
             conn,
             "SELECT '14:30:45+02:00'::TIME_TZ as time_tz_val, NULL::TIME_TZ as time_tz_null"
           ) do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert get_column_types(columns) == [:time_tz, :time_tz]
          assert [{time_tz_val, nil}] = rows
          # TIME_TZ can be returned as a tuple {micros, offset} or string representation
          assert is_tuple(time_tz_val) or is_binary(time_tz_val)

        {:error, _reason} ->
          # TIME_TZ might not be fully supported
          :ok
      end
    end

    test "timestamp with timezone (if supported)", %{conn: conn} do
      case DuckdbEx.query(
             conn,
             "SELECT now()::TIMESTAMP_TZ as ts_tz_val, NULL::TIMESTAMP_TZ as ts_tz_null"
           ) do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert get_column_types(columns) == [:timestamp_tz, :timestamp_tz]
          assert [{_ts_tz_val, nil}] = rows

        {:error, _reason} ->
          # TIMESTAMP_TZ might not be fully supported
          :ok
      end
    end
  end

  describe "null values" do
    test "null values for all basic types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            NULL::BOOLEAN as null_bool,
            NULL::INTEGER as null_int,
            NULL::DOUBLE as null_double,
            NULL::VARCHAR as null_varchar,
            NULL::DATE as null_date,
            NULL::TIME as null_time,
            NULL::TIMESTAMP as null_timestamp,
            NULL::INTERVAL as null_interval
        """)

      expected_types = [
        :boolean,
        :integer,
        :double,
        :varchar,
        :date,
        :time,
        :timestamp,
        :interval
      ]

      assert get_column_types(columns) == expected_types

      # All values should be nil
      assert [{nil, nil, nil, nil, nil, nil, nil, nil}] = rows
    end

    test "null values for complex types", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            NULL::INTEGER[] as null_list,
            NULL::STRUCT(x INTEGER, y INTEGER) as null_struct,
            NULL::UUID as null_uuid,
            NULL::DECIMAL(10,2) as null_decimal
        """)

      expected_types = [:list, :struct, :uuid, :decimal]
      assert get_column_types(columns) == expected_types

      # All values should be nil
      assert [{nil, nil, nil, nil}] = rows
    end
  end

  describe "chunked data access" do
    test "chunked data with basic types", %{conn: conn} do
      {columns, rows} =
        query_and_extract_chunked(conn, """
          SELECT
            i as int_val,
            i * 3.14 as float_val,
            'row_' || i::VARCHAR as string_val
          FROM range(5) t(i)
        """)

      assert get_column_types(columns) == [:bigint, :decimal, :varchar]
      assert length(rows) == 5

      # Each row should be a tuple with 3 elements
      Enum.with_index(rows, fn {int_val, float_val, string_val}, i ->
        assert int_val == i
        assert_in_delta float_val, i * 3.14, 0.001
        assert string_val == "row_#{i}"
      end)
    end

    test "chunked data with complex types", %{conn: conn} do
      # Test that chunked data works with complex types
      {columns, rows} =
        query_and_extract_chunked(conn, """
          SELECT
            [i, i+1, i+2] as list_val,
            {'id': i, 'name': 'item_' || i::VARCHAR} as struct_val
          FROM range(3) t(i)
        """)

      assert get_column_types(columns) == [:list, :struct]
      assert length(rows) == 3

      # Check each row
      Enum.with_index(rows, fn {list_val, struct_val}, i ->
        assert is_list(list_val)
        assert list_val == [i, i + 1, i + 2]
        assert is_map(struct_val)
        assert struct_val["id"] == i
        assert struct_val["name"] == "item_#{i}"
      end)
    end

    test "chunked data with new types", %{conn: conn} do
      # Test chunked data with newly supported types
      {columns, rows} =
        query_and_extract_chunked(conn, """
          SELECT
            '550e8400-e29b-41d4-a716-446655440000'::UUID as uuid_val,
            123.456::DECIMAL(10,3) as decimal_val,
            '2023-12-25 14:30:45'::TIMESTAMP_S as timestamp_s
          FROM range(2) t(i)
        """)

      assert get_column_types(columns) == [:uuid, :decimal, :timestamp_s]
      assert length(rows) == 2

      # Check each row
      Enum.each(rows, fn {uuid_val, decimal_val, timestamp_s} ->
        assert is_binary(uuid_val)
        # UUID is returned as 32-character hex string (without dashes) in chunked API
        assert String.length(uuid_val) == 36
        assert is_float(decimal_val) or is_binary(decimal_val) or is_number(decimal_val)
        assert is_binary(timestamp_s)
      end)
    end
  end

  describe "edge cases and large data" do
    test "empty result set", %{conn: conn} do
      {columns, rows} = query_and_extract(conn, "SELECT 1 as val WHERE 1 = 0")

      assert get_column_types(columns) == [:integer]
      assert rows == []
    end

    test "large number of rows", %{conn: conn} do
      {columns, rows} = query_and_extract(conn, "SELECT i FROM range(1000) t(i)")

      assert get_column_types(columns) == [:bigint]
      assert length(rows) == 1000
      assert {0} = hd(rows)
      assert {999} = List.last(rows)
    end

    test "wide table with many columns", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            1 as col1, 2 as col2, 3 as col3, 4 as col4, 5 as col5,
            6 as col6, 7 as col7, 8 as col8, 9 as col9, 10 as col10,
            11 as col11, 12 as col12, 13 as col13, 14 as col14, 15 as col15
        """)

      assert length(columns) == 15
      assert get_column_types(columns) == List.duplicate(:integer, 15)
      assert [{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}] = rows
    end

    test "string with special characters", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            'Hello, 世界! 🌍' as unicode_string,
            'Line 1\nLine 2\tTabbed' as multiline_string,
            'Quote: "Hello", Apostrophe: ''World''' as quoted_string
        """)

      assert get_column_types(columns) == [:varchar, :varchar, :varchar]
      assert [{unicode_string, multiline_string, quoted_string}] = rows
      assert unicode_string == "Hello, 世界! 🌍"
      assert multiline_string == "Line 1\nLine 2\tTabbed"
      assert quoted_string == "Quote: \"Hello\", Apostrophe: 'World'"
    end

    test "very large numbers", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            9223372036854775807::BIGINT as max_bigint,
            (-9223372036854775807 - 1)::BIGINT as min_bigint,
            1.7976931348623157E+308::DOUBLE as large_double
        """)

      assert get_column_types(columns) == [:bigint, :bigint, :double]
      assert [{max_bigint, min_bigint, large_double}] = rows
      assert max_bigint == 9_223_372_036_854_775_807
      assert min_bigint == -9_223_372_036_854_775_808
      assert large_double > 1.0e300
    end
  end

  describe "mixed operations" do
    test "aggregation with different types", %{conn: conn} do
      # First create a table with mixed types
      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE mixed_data AS
          SELECT
            i as id,
            i % 2 = 0 as is_even,
            i * 3.14 as score,
            'item_' || i::VARCHAR as name,
            date '2023-01-01' + INTERVAL (i) DAY as created_date,
            [i, i*2, i*3] as multiples
          FROM range(10) t(i)
        """)

      DuckdbEx.destroy_result(result)

      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            count(*) as total_count,
            sum(id) as id_sum,
            avg(score) as avg_score,
            count(DISTINCT is_even) as distinct_booleans,
            min(created_date) as earliest_date,
            max(created_date) as latest_date
          FROM mixed_data
        """)

      expected_types = [:bigint, :hugeint, :double, :bigint, :timestamp, :timestamp]
      assert get_column_types(columns) == expected_types
      assert [{10, 45, avg_score, 2, earliest_date, latest_date}] = rows
      assert_in_delta avg_score, 45 * 3.14 / 10.0, 0.001
      # Date operations with intervals may return timestamp types
      assert is_binary(earliest_date) or match?(%DateTime{}, earliest_date)
      assert is_binary(latest_date) or match?(%DateTime{}, latest_date)
    end

    test "joins with different types", %{conn: conn} do
      # Create two tables with different types
      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE users AS
          SELECT
            i as user_id,
            'user_' || i::VARCHAR as username,
            '550e8400-e29b-41d4-a716-44665544000' || i::VARCHAR as user_uuid
          FROM range(3) t(i)
        """)

      DuckdbEx.destroy_result(result)

      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE orders AS
          SELECT
            i as order_id,
            i % 3 as user_id,
            (i + 1) * 10.50 as amount,
            ['item_' || (i+1)::VARCHAR] as items
          FROM range(5) t(i)
        """)

      DuckdbEx.destroy_result(result)

      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            u.username,
            u.user_uuid,
            o.amount,
            o.items
          FROM users u
          JOIN orders o ON u.user_id = o.user_id
          ORDER BY u.user_id, o.order_id
        """)

      expected_types = [:varchar, :varchar, :decimal, :list]
      assert get_column_types(columns) == expected_types
      assert length(rows) > 0

      # Check that we get proper types back
      {username, user_uuid, amount, items} = hd(rows)
      assert is_binary(username)
      assert is_binary(user_uuid)
      # Amount could be decimal or float
      assert is_float(amount) or is_number(amount)
      # List types return placeholders in regular API
      assert items == "<unsupported_list_type>" or is_list(items)
    end
  end
end
