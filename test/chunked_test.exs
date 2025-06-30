defmodule DuckdbEx.ChunkedTest do
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

  test "chunked API basic functionality", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as a, 'hello' as b")

    # Test chunked API functions
    count = DuckdbEx.chunk_count(result)
    assert count > 0

    {:ok, chunk} = DuckdbEx.result_get_chunk(result, 0)
    assert chunk != nil

    chunk_data = DuckdbEx.data_chunk_get_data(chunk)
    assert is_list(chunk_data)
    assert length(chunk_data) == 1
    assert [{1, "hello"}] = chunk_data

    DuckdbEx.destroy_result(result)
  end

  test "chunked API with simple array", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3] as arr")

    # Use chunked API
    count = DuckdbEx.chunk_count(result)
    assert count > 0

    {:ok, chunk} = DuckdbEx.result_get_chunk(result, 0)
    chunk_data = DuckdbEx.data_chunk_get_data(chunk)

    assert is_list(chunk_data)
    assert length(chunk_data) == 1

    [{array_val}] = chunk_data
    assert is_list(array_val)
    assert array_val == [1, 2, 3]

    DuckdbEx.destroy_result(result)
  end

  test "rows_chunked function works", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer, 'test' as text")

    rows = DuckdbEx.rows_chunked(result)
    assert [{42, "test"}] = rows

    DuckdbEx.destroy_result(result)
  end
end
