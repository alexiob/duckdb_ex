defmodule DuckdbEx.ArrayTest do
  use ExUnit.Case

  setup do
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    on_exit(fn ->
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end)

    %{conn: conn}
  end

  test "simple enum test", %{conn: conn} do
    # Create enum type first
    {:ok, result} = DuckdbEx.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'happy', 'excited')")
    DuckdbEx.destroy_result(result)

    {:ok, result} = DuckdbEx.query(conn, "SELECT 'happy'::mood as mood_val")
    columns = DuckdbEx.columns(result)
    rows = DuckdbEx.rows(result)
    DuckdbEx.destroy_result(result)

    # Test the enum column metadata
    assert [%{name: "mood_val", type: :enum}] = columns

    # Test the enum value - regular API has limitations with ENUM extraction
    assert [{mood_val}] = rows
    assert mood_val == "<regular_api_enum_limitation>"
  end

  test "simple integer array", %{conn: conn} do
    # For now, let's test that the functionality doesn't crash
    # and that we can identify complex types correctly
    {:ok, result} = DuckdbEx.query(conn, "SELECT ARRAY[1, 2, 3] as arr")
    columns = DuckdbEx.columns(result)
    rows = DuckdbEx.rows(result)
    DuckdbEx.destroy_result(result)

    assert length(columns) == 1
    assert length(rows) == 1
    assert [%{type: :list}] = columns

    # For complex types that can't be extracted with regular API,
    # we expect a placeholder indicating chunked API is needed
    assert [{array_val}] = rows
    assert array_val == "<unsupported_list_type>"
  end
end
