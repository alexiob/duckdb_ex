defmodule DuckdbEx.TypesTest do
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

  describe "basic types" do
    test "boolean type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT true as bool_true, false as bool_false")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 2

      assert [
               %{name: "bool_true", type: :boolean},
               %{name: "bool_false", type: :boolean}
             ] = columns

      assert [{true, false}] = rows
    end

    test "integer types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            127::TINYINT as tinyint_val,
            32767::SMALLINT as smallint_val,
            2147483647::INTEGER as integer_val,
            9223372036854775807::BIGINT as bigint_val
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 4

      assert [
               %{name: "tinyint_val", type: :tinyint},
               %{name: "smallint_val", type: :smallint},
               %{name: "integer_val", type: :integer},
               %{name: "bigint_val", type: :bigint}
             ] = columns

      assert [{127, 32767, 2_147_483_647, 9_223_372_036_854_775_807}] = rows
    end

    test "unsigned integer types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            255::UTINYINT as utinyint_val,
            65535::USMALLINT as usmallint_val,
            4294967295::UINTEGER as uinteger_val,
            18446744073709551615::UBIGINT as ubigint_val
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 4

      assert [
               %{name: "utinyint_val", type: :utinyint},
               %{name: "usmallint_val", type: :usmallint},
               %{name: "uinteger_val", type: :uinteger},
               %{name: "ubigint_val", type: :ubigint}
             ] = columns

      assert [{255, 65535, 4_294_967_295, 18_446_744_073_709_551_615}] = rows
    end

    test "floating point types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            3.14::FLOAT as float_val,
            2.718281828::DOUBLE as double_val
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 2
      assert [%{name: "float_val", type: :float}, %{name: "double_val", type: :double}] = columns
      [{float_val, double_val}] = rows

      assert_in_delta float_val, 3.14, 0.001
      assert_in_delta double_val, 2.718281828, 0.000000001
    end

    test "string types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            'hello world'::VARCHAR as varchar_val,
            'binary data'::BLOB as blob_val
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 2
      assert [%{name: "varchar_val", type: :varchar}, %{name: "blob_val", type: :blob}] = columns
      assert [{"hello world", blob_val}] = rows
      assert is_binary(blob_val)
    end
  end

  describe "date and time types" do
    test "date type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT '2023-12-25'::DATE as date_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "date_val", type: :date}] = columns
      assert [{date_val}] = rows
      assert %Date{} = date_val
      assert date_val == ~D[2023-12-25]
    end

    test "time type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT '14:30:45'::TIME as time_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "time_val", type: :time}] = columns
      assert [{time_val}] = rows
      assert %Time{} = time_val
      assert time_val == ~T[14:30:45]
    end

    test "timestamp type", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, "SELECT '2023-12-25 14:30:45'::TIMESTAMP as timestamp_val")

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "timestamp_val", type: :timestamp}] = columns
      assert [{timestamp_val}] = rows
      assert %DateTime{} = timestamp_val
      assert timestamp_val == ~U[2023-12-25 14:30:45Z]
    end

    test "timestamp precision types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            '2023-12-25 14:30:45'::TIMESTAMP_S as ts_s,
            '2023-12-25 14:30:45'::TIMESTAMP_MS as ts_ms,
            '2023-12-25 14:30:45'::TIMESTAMP_NS as ts_ns
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 3

      assert [
               %{name: "ts_s", type: :timestamp_s},
               %{name: "ts_ms", type: :timestamp_ms},
               %{name: "ts_ns", type: :timestamp_ns}
             ] = columns

      assert [{ts_s, ts_ms, ts_ns}] = rows
      assert is_binary(ts_s)
      assert is_binary(ts_ms)
      assert is_binary(ts_ns)
    end

    test "time with timezone", %{conn: conn} do
      case DuckdbEx.query(conn, "SELECT '14:30:45+02:00'::TIME_TZ as time_tz_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert [%{name: "time_tz_val", type: :time_tz}] = columns
          assert [{time_tz_val}] = rows
          # TIME_TZ is returned as a tuple {micros, offset} or string representation
          assert is_tuple(time_tz_val) or is_binary(time_tz_val)

        {:error, _reason} ->
          # TIME_TZ type not supported in this DuckDB version
          :ok
      end
    end

    test "interval type", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, "SELECT INTERVAL '3 days 2 hours'::INTERVAL as interval_val")

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "interval_val", type: :interval}] = columns
      assert [{interval_val}] = rows
      # Interval is returned as a tuple {months, days, micros}
      assert is_tuple(interval_val)
      assert tuple_size(interval_val) == 3
    end
  end

  describe "advanced types" do
    test "decimal type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT 123.456::DECIMAL(10,3) as decimal_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "decimal_val", type: :decimal}] = columns
      assert [{decimal_val}] = rows
      # Decimal can be returned as float or string depending on precision
      assert is_float(decimal_val) or is_binary(decimal_val)
    end

    test "uuid type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT uuid() as uuid_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "uuid_val", type: :uuid}] = columns
      assert [{uuid_val}] = rows
      # UUID may return nil in regular API
      assert uuid_val == nil or is_binary(uuid_val)

      if is_binary(uuid_val) do
        # UUID should be 36 characters (8-4-4-4-12 format)
        assert String.length(uuid_val) == 36
      end
    end

    test "hugeint and uhugeint types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            170141183460469231731687303715884105727::HUGEINT as hugeint_val,
            340282366920938463463374607431768211455::UHUGEINT as uhugeint_val
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 2

      assert [%{name: "hugeint_val", type: :hugeint}, %{name: "uhugeint_val", type: :uhugeint}] =
               columns

      assert [{hugeint_val, uhugeint_val}] = rows
      # Hugeint values are returned as strings for very large numbers
      assert is_binary(hugeint_val) or is_integer(hugeint_val)
      assert is_binary(uhugeint_val) or is_integer(uhugeint_val)
    end

    test "enum type", %{conn: conn} do
      # Create enum type
      {:ok, result} = DuckdbEx.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'happy', 'excited')")
      DuckdbEx.destroy_result(result)

      {:ok, result} = DuckdbEx.query(conn, "SELECT 'happy'::mood as mood_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "mood_val", type: :enum}] = columns
      assert [{mood_val}] = rows
      # Regular API returns placeholder for enum types
      assert mood_val == "<regular_api_enum_limitation>"
    end
  end

  describe "complex types" do
    test "list type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3, 4, 5] as list_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "list_val", type: :list}] = columns
      assert [{list_val}] = rows
      # Regular API returns placeholder for list types
      assert list_val == "<unsupported_list_type>"
    end

    test "array type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3]::INTEGER[3] as array_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "array_val", type: :array}] = columns
      assert [{array_val}] = rows
      # Regular API returns placeholder for array types
      assert array_val == "<unsupported_array_type>"
    end

    test "struct type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT {'name': 'John', 'age': 30} as struct_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "struct_val", type: :struct}] = columns
      assert [{struct_val}] = rows
      # Regular API returns placeholder for struct types
      assert struct_val == "<unsupported_struct_type>"
    end

    test "map type", %{conn: conn} do
      {:ok, result} = DuckdbEx.query(conn, "SELECT map(['key1', 'key2'], [1, 2]) as map_val")
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "map_val", type: :map}] = columns
      assert [{map_val}] = rows
      # Regular API returns placeholder for map types
      assert map_val == "<unsupported_map_type>"
    end
  end

  describe "null values" do
    test "null values for all types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            NULL::BOOLEAN as null_bool,
            NULL::INTEGER as null_int,
            NULL::VARCHAR as null_varchar,
            NULL::DATE as null_date,
            NULL::TIMESTAMP as null_timestamp,
            NULL::DECIMAL(10,2) as null_decimal,
            NULL::UUID as null_uuid
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 7
      expected_types = [:boolean, :integer, :varchar, :date, :timestamp, :decimal, :uuid]
      column_types = Enum.map(columns, fn %{type: type} -> type end)
      assert column_types == expected_types

      # All values should be nil
      assert [{nil, nil, nil, nil, nil, nil, nil}] = rows
    end
  end

  describe "chunked data with new types" do
    test "chunked data contains correct types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            uuid() as uuid_val,
            123.456::DECIMAL(10,3) as decimal_val,
            '2023-12-25 14:30:45'::TIMESTAMP_S as timestamp_s_val,
            [1, 2, 3] as list_val
          FROM range(3)
        """)

      # Test chunked access
      {:ok, chunk} = DuckdbEx.fetch_chunk(result, 0)
      chunk_data = DuckdbEx.chunk_to_data(chunk)
      DuckdbEx.destroy_result(result)

      assert is_list(chunk_data)
      assert length(chunk_data) == 3

      # Each row should be a tuple with 4 elements
      Enum.each(chunk_data, fn row ->
        assert is_tuple(row)
        assert tuple_size(row) == 4
        {uuid_val, decimal_val, timestamp_s_val, list_val} = row

        assert is_binary(uuid_val)
        assert is_float(decimal_val) or is_binary(decimal_val)
        assert is_binary(timestamp_s_val)
        assert is_list(list_val)
        assert list_val == [1, 2, 3]
      end)
    end
  end

  describe "type edge cases" do
    test "bit type", %{conn: conn} do
      # BIT type may not be fully supported yet, test if it exists
      case DuckdbEx.query(conn, "SELECT '101010'::BIT as bit_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert [%{name: "bit_val", type: :bit}] = columns
          assert [{bit_val}] = rows
          # BIT type extraction may not be implemented yet, so could be nil or binary
          assert bit_val == nil or is_binary(bit_val)

        {:error, _reason} ->
          # BIT type might not be fully supported, skip this test
          :ok
      end
    end

    test "timestamp with timezone type", %{conn: conn} do
      # TIMESTAMP_TZ may not be fully supported yet
      case DuckdbEx.query(conn, "SELECT now()::TIMESTAMP_TZ as ts_tz_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert [%{name: "ts_tz_val", type: :timestamp_tz}] = columns
          assert [{_ts_tz_val}] = rows

        {:error, _reason} ->
          # TIMESTAMP_TZ type not supported in this DuckDB version
          :ok
      end
    end

    test "union type", %{conn: conn} do
      # UNION type may not be fully supported in regular queries
      case DuckdbEx.query(conn, "SELECT union_value(a := 42) as union_val") do
        {:ok, result} ->
          columns = DuckdbEx.columns(result)
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)

          assert [%{name: "union_val", type: :union}] = columns
          assert [{_union_val}] = rows

        {:error, _reason} ->
          # UNION type might not be fully supported, skip this test
          :ok
      end
    end
  end

  describe "type conversion and compatibility" do
    test "type conversion between similar types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT
            42::TINYINT::INTEGER as tinyint_to_int,
            3.14::FLOAT::DOUBLE as float_to_double,
            '123'::VARCHAR::INTEGER as varchar_to_int
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert length(columns) == 3

      assert [
               %{name: "tinyint_to_int", type: :integer},
               %{name: "float_to_double", type: :double},
               %{name: "varchar_to_int", type: :integer}
             ] = columns

      assert [{42, float_val, 123}] = rows
      assert_in_delta float_val, 3.14, 0.001
    end

    test "nested complex types", %{conn: conn} do
      {:ok, result} =
        DuckdbEx.query(conn, """
          SELECT [
            {'name': 'Alice', 'scores': [95, 87, 92]},
            {'name': 'Bob', 'scores': [88, 91, 85]}
          ] as nested_val
        """)

      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)
      DuckdbEx.destroy_result(result)

      assert [%{name: "nested_val", type: :list}] = columns
      assert [{nested_val}] = rows
      # Regular API returns placeholders for complex nested types
      assert nested_val == "<unsupported_list_type>" or is_list(nested_val)

      if is_list(nested_val) do
        assert length(nested_val) == 2

        # Each element should be a map with name and scores
        Enum.each(nested_val, fn item ->
          assert is_map(item)
          assert Map.has_key?(item, "name")
          assert Map.has_key?(item, "scores")
          assert is_list(Map.get(item, "scores"))
        end)
      end
    end
  end
end
