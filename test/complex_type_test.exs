defmodule DuckdbEx.ComplexTypeTest do
  use ExUnit.Case
  alias DuckdbEx

  setup do
    {:ok, db} = DuckdbEx.open(":memory:")
    {:ok, conn} = DuckdbEx.connect(db)
    {:ok, conn: conn}
  end

  test "test other complex types with multiple columns", %{conn: conn} do
    # Test HUGEINT + HUGEINT (another type that might use similar processing)
    {:ok, result} =
      DuckdbEx.query(
        conn,
        "SELECT 123456789012345678901234567890::HUGEINT as h1, 987654321098765432109876543210::HUGEINT as h2"
      )

    columns = DuckdbEx.columns(result)
    assert columns == [%{name: "h1", type: :hugeint}, %{name: "h2", type: :hugeint}]
    rows = DuckdbEx.rows(result)

    assert rows == [
             {123_456_789_012_345_678_901_234_567_890, 987_654_321_098_765_432_109_876_543_210}
           ]

    # Test VARCHAR + VARCHAR (basic type, should work)

    {:ok, result} =
      DuckdbEx.query(conn, "SELECT 'hello'::VARCHAR as v1, 'world'::VARCHAR as v2")

    columns = DuckdbEx.columns(result)
    assert columns == [%{name: "v1", type: :varchar}, %{name: "v2", type: :varchar}]

    rows = DuckdbEx.rows(result)
    assert rows == [{"hello", "world"}]
  end
end
