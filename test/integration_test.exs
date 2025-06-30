defmodule DuckdbEx.IntegrationTest do
  use ExUnit.Case, async: true

  @moduledoc """
  Integration tests for DuckDB configuration with other features.

  These tests verify that configuration works properly with:
  - Database operations
  - Connections
  - Queries
  - Appender bulk loading
  - Transaction operations
  """

  describe "Configuration with database operations" do
    test "configured database supports all basic operations" do
      # Set up configuration
      config_map = %{
        "memory_limit" => "1GB",
        "threads" => "2"
      }

      # Open database with config
      assert {:ok, db} = DuckdbEx.open(:memory, config_map)
      assert {:ok, conn} = DuckdbEx.connect(db)

      # Test table creation
      assert {:ok, _} =
               DuckdbEx.query(
                 conn,
                 "CREATE TABLE integration_test (id INTEGER, name VARCHAR, value DOUBLE)"
               )

      # Test regular inserts
      assert {:ok, _} =
               DuckdbEx.query(conn, "INSERT INTO integration_test VALUES (1, 'test1', 1.23)")

      # Test prepared statements
      assert {:ok, stmt} = DuckdbEx.prepare(conn, "INSERT INTO integration_test VALUES (?, ?, ?)")
      assert {:ok, _} = DuckdbEx.execute(stmt, [2, "test2", 4.56])

      # Test query results
      assert {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM integration_test ORDER BY id")
      rows = DuckdbEx.rows(result)
      assert [{1, "test1", 1.23}, {2, "test2", 4.56}] = rows

      # Test bulk loading with Appender
      assert {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "integration_test")

      bulk_rows = [
        [3, "bulk1", 7.89],
        [4, "bulk2", 10.11],
        [5, "bulk3", 12.13]
      ]

      assert :ok = DuckdbEx.Appender.append_rows(appender, bulk_rows)
      assert :ok = DuckdbEx.Appender.close(appender)
      assert :ok = DuckdbEx.Appender.destroy(appender)

      # Verify bulk insert worked
      assert {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM integration_test")
      assert [{5}] = DuckdbEx.rows(result)

      # Test transactions
      assert :ok = DuckdbEx.begin_transaction(conn)

      assert {:ok, _} =
               DuckdbEx.query(conn, "INSERT INTO integration_test VALUES (6, 'tx_test', 14.15)")

      assert :ok = DuckdbEx.commit(conn)

      # Verify transaction worked
      assert {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM integration_test")
      assert [{6}] = DuckdbEx.rows(result)

      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end

    test "configuration affects performance settings" do
      # Test with limited threads
      config_low = %{"threads" => "1"}
      assert {:ok, db_low} = DuckdbEx.open(:memory, config_low)
      assert {:ok, conn_low} = DuckdbEx.connect(db_low)

      # Test with more threads
      config_high = %{"threads" => "4"}
      assert {:ok, db_high} = DuckdbEx.open(:memory, config_high)
      assert {:ok, conn_high} = DuckdbEx.connect(db_high)

      # Verify the settings are applied
      assert {:ok, result} = DuckdbEx.query(conn_low, "SELECT current_setting('threads')")
      assert [{1}] = DuckdbEx.rows(result)

      assert {:ok, result} = DuckdbEx.query(conn_high, "SELECT current_setting('threads')")
      assert [{4}] = DuckdbEx.rows(result)

      DuckdbEx.close_connection(conn_low)
      DuckdbEx.close_database(db_low)
      DuckdbEx.close_connection(conn_high)
      DuckdbEx.close_database(db_high)
    end

    test "config struct and map produce same results" do
      # Create config via struct
      {:ok, config_struct} = DuckdbEx.Config.new()
      {:ok, config_struct} = DuckdbEx.Config.set(config_struct, "memory_limit", "1GB")
      {:ok, config_struct} = DuckdbEx.Config.set(config_struct, "threads", "2")

      # Create config via map
      config_map = %{
        "memory_limit" => "1GB",
        "threads" => "2"
      }

      # Test both databases
      assert {:ok, db1} = DuckdbEx.open(:memory, config_struct)
      assert {:ok, conn1} = DuckdbEx.connect(db1)

      assert {:ok, db2} = DuckdbEx.open(:memory, config_map)
      assert {:ok, conn2} = DuckdbEx.connect(db2)

      # Verify they have the same settings
      assert {:ok, result1} =
               DuckdbEx.query(
                 conn1,
                 "SELECT current_setting('memory_limit'), current_setting('threads')"
               )

      assert {:ok, result2} =
               DuckdbEx.query(
                 conn2,
                 "SELECT current_setting('memory_limit'), current_setting('threads')"
               )

      assert DuckdbEx.rows(result1) == DuckdbEx.rows(result2)

      DuckdbEx.close_connection(conn1)
      DuckdbEx.close_database(db1)
      DuckdbEx.close_connection(conn2)
      DuckdbEx.close_database(db2)
    end
  end
end
