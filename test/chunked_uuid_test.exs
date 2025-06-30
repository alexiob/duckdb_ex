defmodule DuckdbEx.ChunkedUuidTest do
  use ExUnit.Case, async: false

  test "test UUID in chunked API" do
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    {:ok, result} = DuckdbEx.query(conn, "SELECT gen_random_uuid() as uuid_val")
    columns = DuckdbEx.columns(result)
    [%{name: "uuid_val", type: :uuid}] = columns

    chunked_rows = DuckdbEx.rows_chunked(result)
    [{uuid_str}] = chunked_rows
    assert String.length(uuid_str) == 36
    assert uuid_str =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i

    regular_rows = DuckdbEx.rows(result)
    assert is_list(regular_rows)
    assert length(regular_rows) == 1
    assert [{nil}] = regular_rows

    DuckdbEx.close_connection(conn)
    DuckdbEx.close_database(db)
  end
end
