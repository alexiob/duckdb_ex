defmodule DuckdbEx.Transaction do
  @moduledoc """
  Transaction management for DuckDB connections.

  This module provides functions for managing transactions in DuckDB.

  **Important Note**: DuckDB has limited transaction support compared to traditional RDBMS:
  - ✅ Basic transaction control (BEGIN, COMMIT, ROLLBACK) - **Supported**
  - ❌ Savepoints and nested transactions - **Not supported by DuckDB**
  - ❌ Transaction isolation level control - **Not supported by DuckDB**

  DuckDB operates with ACID compliance but doesn't support all advanced transaction features.

  ## Basic Usage

      {:ok, db} = DuckdbEx.open()
      {:ok, conn} = DuckdbEx.connect(db)

      # Begin transaction
      :ok = DuckdbEx.Transaction.begin(conn)

      # Execute some queries
      {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO users (name) VALUES ('Alice')")
      {:ok, _result} = DuckdbEx.query(conn, "INSERT INTO users (name) VALUES ('Bob')")

      # Commit transaction
      :ok = DuckdbEx.Transaction.commit(conn)

  ## Helper Functions

  The module provides helper functions for common transaction patterns:
  - `with_transaction/2` - Execute a function within a transaction
  """

  alias DuckdbEx.{Connection, Nif}

  @type connection :: Connection.t()

  ## Basic Transaction Control

  @doc """
  Begins a new transaction.

  ## Parameters
  - `connection` - The database connection

  ## Examples

      :ok = DuckdbEx.Transaction.begin(conn)
  """
  @spec begin(connection) :: :ok | {:error, String.t()}
  def begin(connection) do
    Nif.connection_begin_transaction(connection)
  end

  @doc """
  Commits the current transaction.

  ## Parameters
  - `connection` - The database connection

  ## Examples

      :ok = DuckdbEx.Transaction.commit(conn)
  """
  @spec commit(connection) :: :ok | {:error, String.t()}
  def commit(connection) do
    Nif.connection_commit(connection)
  end

  @doc """
  Rolls back the current transaction.

  ## Parameters
  - `connection` - The database connection

  ## Examples

      :ok = DuckdbEx.Transaction.rollback(conn)
  """
  @spec rollback(connection) :: :ok | {:error, String.t()}
  def rollback(connection) do
    Nif.connection_rollback(connection)
  end

  ## Helper Functions

  @doc """
  Executes a function within a transaction.

  If the function returns `{:ok, result}`, the transaction is committed and `{:ok, result}` is returned.
  If the function returns `{:error, reason}` or raises an exception, the transaction is rolled back.

  ## Parameters
  - `connection` - The database connection
  - `fun` - Function to execute within the transaction

  ## Examples

      result = DuckdbEx.Transaction.with_transaction(conn, fn ->
        {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (name) VALUES ('Alice')")
        {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (name) VALUES ('Bob')")
        {:ok, "Users inserted"}
      end)

      case result do
        {:ok, message} -> IO.puts(message)
        {:error, error_reason} -> IO.puts("Transaction failed: \#{error_reason}")
      end
  """
  @spec with_transaction(connection, (-> {:ok, any()} | {:error, any()})) ::
          {:ok, any()} | {:error, any()}
  def with_transaction(connection, fun) when is_function(fun, 0) do
    case begin(connection) do
      :ok ->
        try do
          case fun.() do
            {:ok, result} ->
              case commit(connection) do
                :ok -> {:ok, result}
                {:error, reason} -> {:error, "Commit failed: #{reason}"}
              end

            {:error, reason} ->
              rollback(connection)
              {:error, reason}

            other ->
              rollback(connection)

              {:error,
               "Function must return {:ok, result} or {:error, reason}, got: #{inspect(other)}"}
          end
        rescue
          exception ->
            rollback(connection)
            {:error, "Exception in transaction: #{Exception.message(exception)}"}
        end

      {:error, reason} ->
        {:error, "Failed to begin transaction: #{reason}"}
    end
  end
end
