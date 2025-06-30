defmodule DuckdbEx.NullTest do
  use ExUnit.Case

  setup do
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    on_exit(fn ->
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end)

    {:ok, conn: conn}
  end

  test "boolean null handling", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT CAST(NULL AS BOOLEAN) as bool_null")

    # Test both APIs
    regular_rows = DuckdbEx.rows(result)
    assert regular_rows == [{nil}]

    chunked_rows = DuckdbEx.rows_chunked(result)
    # Chunked API returns empty array for NULL-only queries
    assert chunked_rows == []

    DuckdbEx.destroy_result(result)
  end

  test "various null types", %{conn: conn} do
    {:ok, result} =
      DuckdbEx.query(conn, """
        SELECT
          CAST(NULL AS BOOLEAN) as bool_null,
          CAST(NULL AS INTEGER) as int_null,
          CAST(NULL AS VARCHAR) as str_null
      """)

    regular_rows = DuckdbEx.rows(result)
    assert regular_rows == [{nil, nil, nil}]

    chunked_rows = DuckdbEx.rows_chunked(result)
    # Chunked API returns empty array for NULL-only queries
    assert chunked_rows == []

    DuckdbEx.destroy_result(result)
  end
end
