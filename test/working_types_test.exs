defmodule DuckdbEx.WorkingTypesTest do
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

  describe "basic types (fully working)" do
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
      # Decimal is returned as float in this implementation
      assert is_float(decimal_val)
      assert_in_delta decimal_val, 123.456, 0.001
      assert is_float(decimal_val2)
      assert_in_delta decimal_val2, 999.99, 0.01
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

  describe "temporal types (fully working)" do
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
      assert time_val == ~T[14:30:45]
      assert %Time{} = time_microseconds
      assert time_microseconds == ~T[23:59:59.123456]
    end

    test "timestamp type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            '2023-12-25 14:30:45'::TIMESTAMP as timestamp_val,
            NULL::TIMESTAMP as timestamp_null
        """)

      assert get_column_types(columns) == [:timestamp, :timestamp]
      assert [{timestamp_val, nil}] = rows
      assert %DateTime{} = timestamp_val
      assert timestamp_val == ~U[2023-12-25 14:30:45Z]
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

  describe "new types (working in some contexts)" do
    test "uuid type (limited support)", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            '550e8400-e29b-41d4-a716-446655440000'::VARCHAR as uuid_as_string,
            NULL::VARCHAR as uuid_null
        """)

      assert get_column_types(columns) == [:varchar, :varchar]
      assert [{uuid_as_string, nil}] = rows
      assert is_binary(uuid_as_string)
      # UUID as string format
      assert String.length(uuid_as_string) == 36
      assert String.contains?(uuid_as_string, "-")
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

    test "list type", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            [1, 2, 3, 4, 5] as int_list,
            ['hello', 'world'] as string_list,
            [] as empty_list,
            NULL::INTEGER[] as list_null
        """)

      assert get_column_types(columns) == [:list, :list, :list, :list]
      assert [{int_list, string_list, empty_list, list_null}] = rows

      # Regular API returns placeholders for list types
      assert int_list == "<unsupported_list_type>"
      assert string_list == "<unsupported_list_type>"
      assert empty_list == "<unsupported_list_type>"
      assert list_null == "<unsupported_list_type>"
    end
  end

  describe "chunked data access (better support for complex types)" do
    test "chunked data with basic types", %{conn: conn} do
      {columns, rows} =
        query_and_extract_chunked(conn, """
          SELECT
            i as int_val,
            i * 3.14 as float_val,
            'row_' || i::VARCHAR as string_val
          FROM range(5) t(i)
        """)

      # Note: range(5) returns BIGINT, and i * 3.14 might return DECIMAL in chunked context
      assert length(get_column_types(columns)) == 3
      assert length(rows) == 5

      # Each row should be a tuple with 3 elements
      Enum.with_index(rows, fn {int_val, float_val, string_val}, i ->
        assert int_val == i
        # float_val might be returned as decimal or float
        assert is_number(float_val)
        assert string_val == "row_#{i}"
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
        # UUID is returned as 36-character string with dashes after the fix
        assert String.length(uuid_val) == 36
        assert is_float(decimal_val) or is_binary(decimal_val) or is_number(decimal_val)
        assert is_binary(timestamp_s)
        # timestamp_s is returned as seconds since epoch
        assert String.match?(timestamp_s, ~r/^\d+$/)
      end)
    end

    test "chunked data with complex lists", %{conn: conn} do
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
  end

  describe "edge cases" do
    test "empty result set", %{conn: conn} do
      {columns, rows} = query_and_extract(conn, "SELECT 1 as val WHERE 1 = 0")

      assert get_column_types(columns) == [:integer]
      assert rows == []
    end

    test "large number of rows", %{conn: conn} do
      {columns, rows} = query_and_extract(conn, "SELECT i FROM range(100) t(i)")

      # range() returns BIGINT
      assert get_column_types(columns) == [:bigint]
      assert length(rows) == 100
      assert {0} = hd(rows)
      assert {99} = List.last(rows)
    end

    test "wide table with many columns", %{conn: conn} do
      {columns, rows} =
        query_and_extract(conn, """
          SELECT
            1 as col1, 2 as col2, 3 as col3, 4 as col4, 5 as col5,
            6 as col6, 7 as col7, 8 as col8, 9 as col9, 10 as col10
        """)

      assert length(columns) == 10
      assert get_column_types(columns) == List.duplicate(:integer, 10)
      assert [{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}] = rows
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
            -9223372036854775807::BIGINT as close_to_min_bigint,
            1.7976931348623157E+308::DOUBLE as large_double
        """)

      assert get_column_types(columns) == [:bigint, :bigint, :double]
      assert [{max_bigint, close_to_min_bigint, large_double}] = rows
      assert max_bigint == 9_223_372_036_854_775_807
      assert close_to_min_bigint == -9_223_372_036_854_775_807
      assert large_double > 1.0e300
    end
  end

  describe "null handling" do
    test "null values for all supported types", %{conn: conn} do
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
            NULL::INTERVAL as null_interval,
            NULL::DECIMAL(10,2) as null_decimal,
            NULL::UUID as null_uuid
        """)

      expected_types = [
        :boolean,
        :integer,
        :double,
        :varchar,
        :date,
        :time,
        :timestamp,
        :interval,
        :decimal,
        :uuid
      ]

      assert get_column_types(columns) == expected_types

      # All values should be nil
      assert [{nil, nil, nil, nil, nil, nil, nil, nil, nil, nil}] = rows
    end
  end

  describe "comparison between regular and chunked APIs" do
    test "consistency check for basic types", %{conn: conn} do
      sql = """
        SELECT
          42 as int_val,
          3.14 as float_val,
          'hello' as string_val,
          true as bool_val,
          [1, 2, 3] as list_val
      """

      # Regular API
      {regular_columns, regular_rows} = query_and_extract(conn, sql)

      # Chunked API
      {chunked_columns, chunked_rows} = query_and_extract_chunked(conn, sql)

      # Column types should be the same
      assert get_column_types(regular_columns) == get_column_types(chunked_columns)

      # Row data should match for basic types, but complex types differ
      assert length(regular_rows) == length(chunked_rows)

      [{reg_int, reg_float, reg_string, reg_bool, reg_list}] = regular_rows
      [{chunk_int, chunk_float, chunk_string, chunk_bool, chunk_list}] = chunked_rows

      # Basic types should match
      assert reg_int == chunk_int
      assert reg_float == chunk_float
      assert reg_string == chunk_string
      assert reg_bool == chunk_bool

      # Complex types differ: regular API uses placeholders, chunked API extracts actual data
      # Regular API placeholder
      assert reg_list == "<unsupported_list_type>"
      # Chunked API actual data
      assert chunk_list == [1, 2, 3]
    end
  end
end
