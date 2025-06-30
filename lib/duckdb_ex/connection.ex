defmodule DuckdbEx.Connection do
  @moduledoc """
  Connection resource management for DuckDB.
  """

  alias DuckdbEx.Database

  @type t :: reference()

  @doc """
  Opens a connection to a DuckDB database.
  """
  @spec open(Database.t()) :: {:ok, t()} | {:error, String.t()}
  def open(database) do
    case DuckdbEx.Nif.connection_open(database) do
      {:ok, ref} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Closes a database connection.
  """
  @spec close(t()) :: :ok
  def close(_connection) do
    # Connection cleanup is handled by the NIF resource destructor
    :ok
  end

  @doc """
  Executes a SQL query on the connection.
  """
  @spec query(t(), String.t()) :: {:ok, DuckdbEx.Result.t()} | {:error, String.t()}
  def query(connection, sql) do
    case DuckdbEx.Nif.connection_query(connection, sql) do
      {:ok, result_ref} -> {:ok, result_ref}
      {:error, reason} -> {:error, reason}
    end
  end
end
