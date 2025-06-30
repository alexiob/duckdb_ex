defmodule DuckdbEx.EnumTest do
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

  test "enum works with chunked API", %{conn: conn} do
    # Create enum type first
    {:ok, result} = DuckdbEx.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'happy', 'excited')")
    DuckdbEx.destroy_result(result)

    {:ok, result} = DuckdbEx.query(conn, "SELECT 'happy'::mood as mood_val")

    # Try chunked API for ENUMs
    rows_chunked = DuckdbEx.rows_chunked(result)
    assert rows_chunked == [{"happy"}]

    # Also try regular API for comparison
    rows_regular = DuckdbEx.rows(result)
    assert rows_regular == [{"<regular_api_enum_limitation>"}]

    DuckdbEx.destroy_result(result)
  end
end
