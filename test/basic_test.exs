defmodule BasicTest do
  use ExUnit.Case
  alias DuckdbEx

  setup do
    {:ok, db} = DuckdbEx.open(":memory:")
    {:ok, conn} = DuckdbEx.connect(db)
    %{conn: conn}
  end

  test "chunked API works with working query from main test suite", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer, 'hello' as greeting")
    # Test the chunked API
    rows = DuckdbEx.rows_chunked(result)
    assert [{42, "hello"}] = rows
  end
end
