defmodule DuckdbEx.DirectNifTest do
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

  test "direct NIF calls for null values", %{conn: conn} do
    {:ok, result} = DuckdbEx.query(conn, "SELECT CAST(NULL AS BOOLEAN) as bool_null")

    # Test direct NIF calls step by step
    chunk_count = DuckdbEx.Nif.result_chunk_count(result)
    assert chunk_count == 1

    {:ok, chunk} = DuckdbEx.Nif.result_get_chunk(result, 0)
    assert is_reference(chunk)

    chunk_data = DuckdbEx.Nif.data_chunk_get_data(chunk)
    assert chunk_data == [{nil}]

    # Test Result module function
    result_chunked = DuckdbEx.Result.rows_chunked(result)
    assert result_chunked == [{nil}]

    # Test main module function
    main_chunked = DuckdbEx.rows_chunked(result)
    assert main_chunked == [{nil}]

    DuckdbEx.destroy_result(result)
  end
end
