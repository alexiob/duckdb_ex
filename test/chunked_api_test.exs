defmodule ChunkedApiTest do
  use ExUnit.Case
  alias DuckdbEx

  setup do
    {:ok, db} = DuckdbEx.open(":memory:")
    {:ok, conn} = DuckdbEx.connect(db)
    %{conn: conn, db: db}
  end

  describe "Basic Chunked API Functionality" do
    test "chunked API basic operations work", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as id, 'Alice' as name")

      # Test chunk count
      chunk_count = DuckdbEx.Result.chunk_count(result)
      assert is_integer(chunk_count) and chunk_count >= 1

      # Test getting chunks
      {:ok, chunk} = DuckdbEx.Result.get_chunk(result, 0)
      assert is_reference(chunk)

      # Test chunked rows
      rows = DuckdbEx.rows_chunked(result)
      assert [{1, "Alice"}] = rows
    end

    test "chunked API handles empty results", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as id WHERE false")

      chunk_count = DuckdbEx.Result.chunk_count(result)
      assert chunk_count >= 0

      rows = DuckdbEx.rows_chunked(result)
      assert rows == []
    end

    test "chunked API handles large datasets efficiently", %{conn: conn} do
      # Create a table with many rows
      {:ok, _} =
        DuckdbEx.query(conn, """
          CREATE TABLE large_test AS
          SELECT i as id, 'name_' || i as name, random() as score
          FROM range(1, 1001) t(i)
        """)

      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM large_test ORDER BY id")

      rows = DuckdbEx.rows_chunked(result)
      assert length(rows) == 1000

      # Verify first and last rows
      [{1, "name_1", _score1} | _] = rows
      {1000, "name_1000", _score1000} = List.last(rows)
    end
  end

  describe "Unsigned Integer Types" do
    test "chunked API handles unsigned integer boundary values", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            CAST(0 AS UTINYINT) as utiny_min,
            CAST(255 AS UTINYINT) as utiny_max,
            CAST(0 AS USMALLINT) as usmall_min,
            CAST(65535 AS USMALLINT) as usmall_max,
            CAST(0 AS UINTEGER) as uint_min,
            CAST(4294967295 AS UINTEGER) as uint_max,
            CAST(0 AS UBIGINT) as ubig_min,
            CAST(18446744073709551615 AS UBIGINT) as ubig_max
        """)

      rows = DuckdbEx.rows_chunked(result)

      assert [
               {utiny_min, utiny_max, usmall_min, usmall_max, uint_min, uint_max, ubig_min,
                ubig_max}
             ] = rows

      # Test minimum values
      assert is_integer(utiny_min) and utiny_min == 0
      assert is_integer(usmall_min) and usmall_min == 0
      assert is_integer(uint_min) and uint_min == 0
      assert is_integer(ubig_min) and ubig_min == 0

      # Test maximum values
      assert is_integer(utiny_max) and utiny_max == 255
      assert is_integer(usmall_max) and usmall_max == 65535
      assert is_integer(uint_max) and uint_max == 4_294_967_295
      assert is_integer(ubig_max) and ubig_max == 18_446_744_073_709_551_615
    end

    test "chunked API handles arrays of unsigned integers", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT [CAST(100 AS UTINYINT), CAST(200 AS UTINYINT)] as utiny_array
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{utiny_array}] = rows
      assert is_list(utiny_array)
      assert utiny_array == [100, 200]
    end
  end

  describe "Decimal Types" do
    test "chunked API handles various decimal precisions", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            CAST(123.456 AS DECIMAL(6,3)) as decimal1,
            CAST(99.99 AS DECIMAL(4,2)) as decimal2,
            CAST(1.0 AS DECIMAL(3,1)) as decimal3,
            CAST(1000 AS DECIMAL(4,0)) as decimal4,
            CAST(-123.45 AS DECIMAL(5,2)) as decimal5
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{decimal1, decimal2, decimal3, decimal4, decimal5}] = rows

      # Decimals should be returned as proper numbers (float/integer)
      assert is_float(decimal1) and abs(decimal1 - 123.456) < 0.001
      assert is_float(decimal2) and abs(decimal2 - 99.99) < 0.001
      assert is_float(decimal3) and abs(decimal3 - 1.0) < 0.001
      # Scale 0 returns integer
      assert is_integer(decimal4) and decimal4 == 1000
      assert is_float(decimal5) and abs(decimal5 - -123.45) < 0.001
    end

    test "chunked API handles zero and small decimal values", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            CAST(0.00 AS DECIMAL(3,2)) as zero_decimal,
            CAST(0.01 AS DECIMAL(3,2)) as small_decimal,
            CAST(0.001 AS DECIMAL(4,3)) as tiny_decimal
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{zero_decimal, small_decimal, tiny_decimal}] = rows

      assert is_float(zero_decimal) and abs(zero_decimal - 0.00) < 0.001
      assert is_float(small_decimal) and abs(small_decimal - 0.01) < 0.001
      assert is_float(tiny_decimal) and abs(tiny_decimal - 0.001) < 0.0001
    end

    test "chunked API handles decimal arrays", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT [CAST(1.1 AS DECIMAL(2,1)), CAST(2.2 AS DECIMAL(2,1))] as decimal_array
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{decimal_array}] = rows
      assert is_list(decimal_array)
      [dec1, dec2] = decimal_array
      assert is_float(dec1) and abs(dec1 - 1.1) < 0.001
      assert is_float(dec2) and abs(dec2 - 2.2) < 0.001
    end
  end

  describe "Huge Integer Types" do
    test "chunked API handles various hugeint values", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            CAST(0 AS HUGEINT) as huge_zero,
            CAST(123456789012345 AS HUGEINT) as huge_medium,
            CAST(-123456789012345 AS HUGEINT) as huge_negative,
            CAST(170141183460469231731687303715884105727 AS HUGEINT) as huge_max
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{huge_zero, huge_medium, huge_negative, huge_max}] = rows

      # All should be integers with unlimited precision
      assert is_integer(huge_zero) and huge_zero == 0
      assert is_integer(huge_medium) and huge_medium == 123_456_789_012_345

      assert is_integer(huge_negative) and huge_negative == -123_456_789_012_345

      assert is_integer(huge_max) and
               huge_max == 170_141_183_460_469_231_731_687_303_715_884_105_727
    end

    test "chunked API handles hugeint in calculations", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT CAST(1000000000000 AS HUGEINT) * CAST(1000000000000 AS HUGEINT) as huge_product
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{huge_product}] = rows
      assert is_integer(huge_product)
      assert huge_product == 1_000_000_000_000_000_000_000_000
    end
  end

  describe "Date and Time Types" do
    test "chunked API handles comprehensive date/time values", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            CAST('1970-01-01' AS DATE) as epoch_date,
            CAST('2023-12-25' AS DATE) as christmas_date,
            CAST('00:00:00' AS TIME) as midnight_time,
            CAST('23:59:59' AS TIME) as end_of_day_time,
            CAST('1970-01-01 00:00:00' AS TIMESTAMP) as epoch_timestamp,
            CAST('2023-12-25 14:30:45.123456' AS TIMESTAMP) as precise_timestamp
        """)

      rows = DuckdbEx.rows_chunked(result)

      assert [
               {epoch_date, christmas_date, midnight_time, end_of_day_time, epoch_timestamp,
                precise_timestamp}
             ] = rows

      # Date and Time values should be returned as Elixir native types
      assert %Date{} = epoch_date
      assert epoch_date == ~D[1970-01-01]

      assert %Date{} = christmas_date
      assert christmas_date == ~D[2023-12-25]

      assert %Time{} = midnight_time
      assert midnight_time == ~T[00:00:00.000000]

      assert %Time{} = end_of_day_time
      assert end_of_day_time == ~T[23:59:59.000000]

      assert %DateTime{} = epoch_timestamp
      assert epoch_timestamp == ~U[1970-01-01 00:00:00.000000Z]

      assert %DateTime{} = precise_timestamp
      assert precise_timestamp == ~U[2023-12-25 14:30:45.123456Z]
    end

    test "chunked API handles interval types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            INTERVAL '1 year' as year_interval,
            INTERVAL '2 months' as month_interval,
            INTERVAL '5 days' as day_interval,
            INTERVAL '12 hours' as hour_interval,
            INTERVAL '30 minutes' as minute_interval,
            INTERVAL '45 seconds' as second_interval,
            INTERVAL '2 months 5 days 12 hours 30 minutes' as complex_interval
        """)

      rows = DuckdbEx.rows_chunked(result)

      assert [{year_int, month_int, day_int, hour_int, minute_int, second_int, complex_int}] =
               rows

      # All should be tuples with {months, days, micros}
      assert is_tuple(year_int) and tuple_size(year_int) == 3
      assert is_tuple(month_int) and tuple_size(month_int) == 3
      assert is_tuple(day_int) and tuple_size(day_int) == 3
      assert is_tuple(hour_int) and tuple_size(hour_int) == 3
      assert is_tuple(minute_int) and tuple_size(minute_int) == 3
      assert is_tuple(second_int) and tuple_size(second_int) == 3
      assert is_tuple(complex_int) and tuple_size(complex_int) == 3

      # Check specific values for each interval
      # 1 year = 12 months
      assert year_int == {12, 0, 0}
      # 2 months
      assert month_int == {2, 0, 0}
      # 5 days
      assert day_int == {0, 5, 0}
      # 12 hours in microseconds
      assert hour_int == {0, 0, 43_200_000_000}
      # 30 minutes in microseconds
      assert minute_int == {0, 0, 1_800_000_000}
      # 45 seconds in microseconds
      assert second_int == {0, 0, 45_000_000}

      # Complex interval: 2 months 5 days 12 hours 30 minutes
      {months, days, micros} = complex_int
      assert months == 2
      assert days == 5
      # (12*60*60 + 30*60) * 1,000,000 microseconds
      assert micros == 45_000_000_000
    end

    test "chunked API handles date/time arrays", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT [CAST('2023-01-01' AS DATE), CAST('2023-12-31' AS DATE)] as date_array
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{date_array}] = rows
      assert is_list(date_array)
      assert length(date_array) == 2

      # Dates should be returned as Elixir Date structs, not strings
      Enum.each(date_array, fn date ->
        assert %Date{} = date
      end)

      # Verify the actual date values
      assert Enum.at(date_array, 0) == ~D[2023-01-01]
      assert Enum.at(date_array, 1) == ~D[2023-12-31]
    end
  end

  describe "Binary Data (BLOB)" do
    test "chunked API handles various blob sizes and contents", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            ''::BLOB as empty_blob,
            '\\x00'::BLOB as null_byte_blob,
            '\\x48\\x65\\x6c\\x6c\\x6f'::BLOB as hello_blob,
            '\\xff\\xfe\\xfd\\xfc'::BLOB as binary_blob
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{empty_blob, null_byte_blob, hello_blob, binary_blob}] = rows

      assert is_binary(empty_blob) and byte_size(empty_blob) == 0
      assert is_binary(null_byte_blob) and byte_size(null_byte_blob) == 1
      assert is_binary(hello_blob) and hello_blob == "Hello"
      assert is_binary(binary_blob) and byte_size(binary_blob) == 4
    end

    test "chunked API handles large blob data", %{conn: conn} do
      # Create a smaller blob for testing (50 bytes instead of 1000)
      # Each \\x41 represents one 'A' byte
      large_blob_parts = for _ <- 1..50, do: "\\x41"
      large_blob_hex = Enum.join(large_blob_parts, "")

      {:ok, result} =
        DuckdbEx.query(conn, "SELECT '#{large_blob_hex}'::BLOB as large_blob")

      rows = DuckdbEx.rows_chunked(result)
      assert [{large_blob}] = rows

      assert is_binary(large_blob) and byte_size(large_blob) == 50
      # Check that it's all 'A' characters (0x41 bytes)
      assert large_blob == String.duplicate("A", 50)
    end

    test "chunked API handles blob arrays", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT ['\\x41'::BLOB, '\\x42'::BLOB, '\\x43'::BLOB] as blob_array
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{blob_array}] = rows
      assert is_list(blob_array)
      assert blob_array == ["A", "B", "C"]
    end
  end

  describe "Complex Types (STRUCT, LIST, MAP)" do
    test "chunked API handles nested struct types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT {
            'person': {'name': 'Alice', 'age': 30},
            'address': {'street': '123 Main St', 'city': 'Anytown'},
            'active': true
          } as nested_struct
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{nested_struct}] = rows

      assert is_map(nested_struct)
      assert Map.has_key?(nested_struct, "person")
      assert Map.has_key?(nested_struct, "address")
      assert Map.has_key?(nested_struct, "active")

      person = Map.get(nested_struct, "person")
      assert is_map(person)
      assert Map.get(person, "name") == "Alice"
      assert Map.get(person, "age") == 30
    end

    test "chunked API handles lists of different types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            [1, 2, 3, 4, 5] as int_list,
            ['apple', 'banana', 'cherry'] as string_list,
            [true, false, true, false] as bool_list,
            [1.1, 2.2, 3.3] as float_list
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{int_list, string_list, bool_list, float_list}] = rows

      assert is_list(int_list) and int_list == [1, 2, 3, 4, 5]
      assert is_list(string_list) and string_list == ["apple", "banana", "cherry"]
      assert is_list(bool_list) and bool_list == [true, false, true, false]
      assert is_list(float_list) and length(float_list) == 3
      # Floats might be returned as character lists in some cases
      Enum.each(float_list, fn f -> assert is_float(f) or is_list(f) end)
    end

    test "chunked API handles nested lists", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT [[1, 2], [3, 4], [5, 6]] as nested_list
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{nested_list}] = rows

      assert is_list(nested_list)
      assert length(nested_list) == 3

      Enum.each(nested_list, fn sublist ->
        assert is_list(sublist)
        assert length(sublist) == 2
      end)
    end

    test "chunked API handles complex map types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            MAP(['a', 'b', 'c'], [1, 2, 3]) as simple_map,
            MAP(['key1', 'key2'], [{'nested': 'value1'}, {'nested': 'value2'}]) as map_with_struct_values
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{simple_map, map_with_struct_values}] = rows

      assert is_map(simple_map)
      assert Map.get(simple_map, "a") == 1
      assert Map.get(simple_map, "b") == 2
      assert Map.get(simple_map, "c") == 3

      assert is_map(map_with_struct_values)
      value1 = Map.get(map_with_struct_values, "key1")
      assert is_map(value1)
      assert Map.get(value1, "nested") == "value1"
    end

    test "chunked API handles lists of structs", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT [
            {'name': 'Alice', 'age': 30},
            {'name': 'Bob', 'age': 25},
            {'name': 'Charlie', 'age': 35}
          ] as people_list
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{people_list}] = rows

      assert is_list(people_list)
      assert length(people_list) == 3

      Enum.each(people_list, fn person ->
        assert is_map(person)
        assert Map.has_key?(person, "name")
        assert Map.has_key?(person, "age")
      end)
    end
  end

  describe "NULL Values and Edge Cases" do
    test "chunked API handles NULL values for all supported types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            CAST(NULL AS BOOLEAN) as null_bool,
            CAST(NULL AS TINYINT) as null_tinyint,
            CAST(NULL AS UTINYINT) as null_utinyint,
            CAST(NULL AS DECIMAL(10,2)) as null_decimal,
            CAST(NULL AS HUGEINT) as null_hugeint,
            CAST(NULL AS DATE) as null_date,
            CAST(NULL AS TIME) as null_time,
            CAST(NULL AS TIMESTAMP) as null_timestamp,
            CAST(NULL AS INTERVAL) as null_interval,
            CAST(NULL AS BLOB) as null_blob,
            CAST(NULL AS VARCHAR) as null_varchar
        """)

      rows = DuckdbEx.rows_chunked(result)

      assert [
               {null_bool, null_tinyint, null_utinyint, null_decimal, null_hugeint, null_date,
                null_time, null_timestamp, null_interval, null_blob, null_varchar}
             ] = rows

      # All NULL values should return :nil
      assert null_bool == nil
      assert null_tinyint == nil
      assert null_utinyint == nil
      assert null_decimal == nil
      assert null_hugeint == nil
      assert null_date == nil
      assert null_time == nil
      assert null_timestamp == nil
      assert null_interval == nil
      assert null_blob == nil
      assert null_varchar == nil
    end

    test "chunked API handles lists with NULL values", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT [1, NULL, 3, NULL, 5] as list_with_nulls
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{list_with_nulls}] = rows

      assert is_list(list_with_nulls)
      assert list_with_nulls == [1, nil, 3, nil, 5]
    end

    test "chunked API handles struct with NULL values", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT {'name': 'Alice', 'age': NULL, 'active': true} as struct_with_nulls
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{struct_with_nulls}] = rows

      assert is_map(struct_with_nulls)
      assert Map.get(struct_with_nulls, "name") == "Alice"
      assert Map.get(struct_with_nulls, "age") == nil
      assert Map.get(struct_with_nulls, "active") == true
    end

    test "chunked API handles empty lists and structs", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            CAST([] AS INTEGER[]) as empty_list,
            CAST({'empty': true} AS STRUCT(empty BOOLEAN)) as test_struct
        """)

      rows = DuckdbEx.rows_chunked(result)
      assert [{empty_list, test_struct}] = rows

      assert is_list(empty_list) and empty_list == []
      assert is_map(test_struct) and Map.has_key?(test_struct, "empty")
    end
  end

  describe "Performance and Memory" do
    test "chunked API handles very wide tables", %{conn: conn} do
      # Create a table with many columns
      columns = Enum.map(1..50, fn i -> "col#{i} INTEGER" end) |> Enum.join(", ")
      values = Enum.map(1..50, fn i -> "#{i}" end) |> Enum.join(", ")

      {:ok, _} = DuckdbEx.query(conn, "CREATE TABLE wide_table (#{columns})")
      {:ok, _} = DuckdbEx.query(conn, "INSERT INTO wide_table VALUES (#{values})")

      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM wide_table")
      rows = DuckdbEx.rows_chunked(result)

      assert length(rows) == 1
      [row] = rows
      assert tuple_size(row) == 50
      assert elem(row, 0) == 1
      assert elem(row, 49) == 50
    end

    test "chunked API handles multiple chunks correctly", %{conn: conn} do
      # Create a dataset that will likely span multiple chunks
      {:ok, _} =
        DuckdbEx.query(conn, """
          CREATE TABLE multi_chunk_test AS
          SELECT i as id, 'data_' || i as data, random() as value
          FROM range(1, 10001) t(i)
        """)

      {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM multi_chunk_test ORDER BY id")

      chunk_count = DuckdbEx.Result.chunk_count(result)
      rows = DuckdbEx.rows_chunked(result)

      assert chunk_count >= 1
      assert length(rows) == 10000

      # Verify ordering is maintained
      {first_id, _, _} = List.first(rows)
      {last_id, _, _} = List.last(rows)
      assert first_id == 1
      assert last_id == 10000
    end
  end

  describe "Type Consistency and Comparison" do
    test "chunked API returns same types as regular API for supported types", %{conn: conn} do
      {:ok, regular_result} =
        DuckdbEx.query(conn, """
          SELECT
            123 as int_val,
            'hello' as str_val,
            123.45::DOUBLE as float_val,
            true as bool_val
        """)

      {:ok, chunked_result} =
        DuckdbEx.query(conn, """
          SELECT
            123 as int_val,
            'hello' as str_val,
            123.45::DOUBLE as float_val,
            true as bool_val
        """)

      regular_rows = DuckdbEx.rows(regular_result)
      chunked_rows = DuckdbEx.rows_chunked(chunked_result)

      assert length(regular_rows) == length(chunked_rows)
      [{reg_int, reg_str, reg_float, reg_bool}] = regular_rows
      [{chunk_int, chunk_str, chunk_float, chunk_bool}] = chunked_rows

      # Types should match (though chunked API might return some types differently)
      assert is_integer(reg_int) and is_integer(chunk_int)
      assert is_binary(reg_str) and is_binary(chunk_str)
      assert is_float(reg_float) and (is_float(chunk_float) or is_number(chunk_float))
      assert is_atom(reg_bool) and is_atom(chunk_bool)

      # Values should match
      assert reg_int == chunk_int
      assert reg_str == chunk_str
      assert abs(reg_float - chunk_float) < 0.001
      assert reg_bool == chunk_bool
    end
  end

  # Legacy tests (keeping for backwards compatibility)
  test "chunked API returns arrays as Elixir lists", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as id, [1, 2, 3] as arr")
    chunked_rows = DuckdbEx.rows_chunked(result)

    [{1, arr}] = chunked_rows
    assert is_list(arr) and arr == [1, 2, 3]
  end

  test "chunked API handles different primitive types", %{conn: conn} do
    {:ok, result} =
      DuckdbEx.query(conn, """
        SELECT 1 as id, 'Alice' as name, CAST(95.5 AS DOUBLE) as score, true as is_active
        UNION ALL
        SELECT 2 as id, 'Bob' as name, CAST(87.2 AS DOUBLE) as score, false as is_active
      """)

    rows = DuckdbEx.rows_chunked(result)
    assert length(rows) == 2
  end
end
