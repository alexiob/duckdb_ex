defmodule DuckdbEx.PreparedStatement do
  @moduledoc """
  Prepared statement resource management for DuckDB.
  """

  alias DuckdbEx.Connection

  @type t :: reference()

  @doc """
  Prepares a SQL statement.
  """
  @spec prepare(Connection.t(), String.t()) :: {:ok, t()} | {:error, String.t()}
  def prepare(connection, sql) do
    case DuckdbEx.Nif.prepared_statement_prepare(connection, sql) do
      {:ok, ref} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Executes a prepared statement with parameters.
  """
  @spec execute(t(), list()) :: {:ok, DuckdbEx.Result.t()} | {:error, String.t()}
  def execute(prepared_statement, params \\ []) do
    case DuckdbEx.Nif.prepared_statement_execute(prepared_statement, params) do
      {:ok, result_ref} -> {:ok, result_ref}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Destroys a prepared statement and frees its resources.
  """
  @spec destroy(t()) :: :ok
  def destroy(_prepared_statement) do
    # Prepared statement cleanup is handled by the NIF resource destructor
    :ok
  end
end
