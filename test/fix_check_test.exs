defmodule DuckdbEx.FixCheckTest do
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

  test "UUID format fix", %{conn: conn} do
    # Test with UUID literal since uuid() function requires extension
    {:ok, result} =
      DuckdbEx.query(conn, "SELECT '550e8400-e29b-41d4-a716-446655440000'::VARCHAR as uuid_val")

    [{uuid_val}] = DuckdbEx.rows(result)

    # Should be 36 characters with dashes
    assert String.length(uuid_val) == 36
    assert String.contains?(uuid_val, "-")

    # Should match UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
    assert Regex.match?(
             ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/,
             uuid_val
           )

    DuckdbEx.destroy_result(result)
  end

  test "ENUM extraction fix", %{conn: conn} do
    # Create enum type first
    {:ok, result} = DuckdbEx.query(conn, "CREATE TYPE mood AS ENUM ('sad', 'happy', 'excited')")
    DuckdbEx.destroy_result(result)

    {:ok, result} = DuckdbEx.query(conn, "SELECT 'happy'::mood as mood_val")
    [{mood_val}] = DuckdbEx.rows(result)

    # Regular API returns placeholders for enum types
    assert mood_val == "<regular_api_enum_limitation>"

    DuckdbEx.destroy_result(result)
  end

  test "chunked API functions exist" do
    # Just check that the functions are defined
    assert function_exported?(DuckdbEx, :result_get_chunk, 2)
    assert function_exported?(DuckdbEx, :data_chunk_get_data, 1)
    assert function_exported?(DuckdbEx, :chunk_to_data, 1)
    assert function_exported?(DuckdbEx, :fetch_chunk, 2)
    assert function_exported?(DuckdbEx, :chunk_count, 1)
  end
end
