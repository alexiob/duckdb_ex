defmodule DuckdbEx.TimestampTest do
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

  test "timestamp precision types", %{conn: conn} do
    {:ok, result} =
      DuckdbEx.query(conn, """
        SELECT
          TIMESTAMP_S '2023-01-01 12:00:00' as ts_s,
          TIMESTAMP_MS '2023-01-01 12:00:00.123' as ts_ms,
          TIMESTAMP_NS '2023-01-01 12:00:00.123456789' as ts_ns
      """)

    columns = DuckdbEx.columns(result)

    assert columns == [
             %{name: "ts_s", type: :timestamp_s},
             %{name: "ts_ms", type: :timestamp_ms},
             %{name: "ts_ns", type: :timestamp_ns}
           ]

    rows = DuckdbEx.rows(result)

    assert rows == [
             {"<timestamp_extraction_failed>", "<timestamp_extraction_failed>",
              "<timestamp_extraction_failed>"}
           ]

    DuckdbEx.destroy_result(result)
  end

  test "bit type regular API", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT '101010'::BIT as bit_val")

    columns = DuckdbEx.columns(result)
    assert columns == [%{name: "bit_val", type: :bit}]

    rows = DuckdbEx.rows(result)
    assert rows == [{nil}]

    DuckdbEx.destroy_result(result)
  end

  test "bit type chunked API", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT '101010'::BIT as bit_val")
    columns = DuckdbEx.columns(result)
    assert columns == [%{name: "bit_val", type: :bit}]
    chunked_rows = DuckdbEx.rows_chunked(result)
    assert chunked_rows == [{"\x02\xEA"}]
    DuckdbEx.destroy_result(result)
  end
end
