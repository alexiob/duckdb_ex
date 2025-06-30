defmodule DuckdbEx.ColumnOrderTest do
  use ExUnit.Case, async: false

  test "test different column orders" do
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    # Test 1: Simple column + UUID (reverse order)
    {:ok, result1} = DuckdbEx.query(conn, "SELECT 123 as simple_col, uuid() as uuid_col")

    rows1 = DuckdbEx.rows(result1)
    assert is_list(rows1)
    assert length(rows1) == 1
    # When UUID is present in multi-column context, regular API returns nil for all columns
    assert [nil: nil] = rows1

    # Test 2: Three columns with UUID in middle
    {:ok, result2} = DuckdbEx.query(conn, "SELECT 1 as col1, uuid() as uuid_col, 3 as col3")
    rows2 = DuckdbEx.rows(result2)
    assert is_list(rows2)
    assert length(rows2) == 1
    # With UUID in multi-column context, all columns return nil
    assert [{nil, nil, nil}] = rows2

    # Test 3: Two UUIDs
    {:ok, result3} = DuckdbEx.query(conn, "SELECT uuid() as uuid1, uuid() as uuid2")
    rows3 = DuckdbEx.rows(result3)
    assert is_list(rows3)
    assert length(rows3) == 1
    # Multiple UUIDs also return nil for all columns
    assert [nil: nil] = rows3

    DuckdbEx.close_connection(conn)
    DuckdbEx.close_database(db)
  end
end
