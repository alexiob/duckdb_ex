defmodule DuckdbEx.TransactionTest do
  use ExUnit.Case, async: false

  alias DuckdbEx.Transaction

  setup do
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    # Create a test table
    {:ok, _result} =
      DuckdbEx.query(conn, """
        CREATE TABLE users (
          id INTEGER PRIMARY KEY,
          name VARCHAR NOT NULL,
          email VARCHAR
        )
      """)

    on_exit(fn ->
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end)

    %{db: db, conn: conn}
  end

  describe "basic transaction control" do
    test "begin, commit transaction", %{conn: conn} do
      # Begin transaction
      assert :ok = Transaction.begin(conn)

      # Insert data
      {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
      {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (2, 'Bob')")

      # Commit transaction
      assert :ok = Transaction.commit(conn)

      # Verify data is persisted
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM users")
      rows = DuckdbEx.rows(result)
      assert [{2}] = rows
    end

    test "begin, rollback transaction", %{conn: conn} do
      # Insert some initial data
      {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (1, 'Alice')")

      # Begin transaction
      assert :ok = Transaction.begin(conn)

      # Insert more data
      {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (2, 'Bob')")
      {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (3, 'Charlie')")

      # Rollback transaction
      assert :ok = Transaction.rollback(conn)

      # Verify only initial data remains
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM users")
      rows = DuckdbEx.rows(result)
      assert [{1}] = rows

      {:ok, result} = DuckdbEx.query(conn, "SELECT name FROM users")
      rows = DuckdbEx.rows(result)
      assert [{"Alice"}] = rows
    end
  end

  describe "with_transaction helper" do
    test "successful transaction commits automatically", %{conn: conn} do
      result =
        Transaction.with_transaction(conn, fn ->
          {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
          {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (2, 'Bob')")
          {:ok, "Users inserted"}
        end)

      assert {:ok, "Users inserted"} = result

      # Verify data was committed
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM users")
      rows = DuckdbEx.rows(result)
      assert [{2}] = rows
    end

    test "failed transaction rolls back automatically", %{conn: conn} do
      result =
        Transaction.with_transaction(conn, fn ->
          {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
          {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (2, 'Bob')")
          {:error, "Something went wrong"}
        end)

      assert {:error, "Something went wrong"} = result

      # Verify no data was committed
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM users")
      rows = DuckdbEx.rows(result)
      assert [{0}] = rows
    end

    test "exception in transaction rolls back automatically", %{conn: conn} do
      result =
        Transaction.with_transaction(conn, fn ->
          {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (id, name) VALUES (1, 'Alice')")
          raise "Oops!"
        end)

      assert {:error, error_msg} = result
      assert String.contains?(error_msg, "Exception in transaction")

      # Verify no data was committed
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM users")
      rows = DuckdbEx.rows(result)
      assert [{0}] = rows
    end
  end

  describe "main module delegation" do
    test "transaction functions are available from main module", %{conn: conn} do
      # Test that basic transaction functions are available from DuckdbEx module
      assert :ok = DuckdbEx.begin_transaction(conn)
      assert :ok = DuckdbEx.commit(conn)

      # Test helper functions
      result =
        DuckdbEx.with_transaction(conn, fn ->
          {:ok, "success"}
        end)

      assert {:ok, "success"} = result
    end
  end
end
