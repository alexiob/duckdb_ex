defmodule DuckdbEx.Database do
  @moduledoc """
  Database resource management for DuckDB.
  """

  alias DuckdbEx.Config

  @type t :: reference()

  @doc """
  Opens a DuckDB database.
  """
  @spec open(String.t() | nil | :memory) :: {:ok, t()} | {:error, String.t()}
  def open(path) do
    normalized_path = normalize_path(path)

    case DuckdbEx.Nif.database_open(normalized_path) do
      {:ok, ref} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Opens a DuckDB database with configuration.
  """
  @spec open(String.t() | nil | :memory, Config.t() | map()) :: {:ok, t()} | {:error, String.t()}
  def open(path, config) when is_reference(config) do
    normalized_path = normalize_path(path)

    case DuckdbEx.Nif.database_open_ext(normalized_path, config) do
      {:ok, ref} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  def open(path, config) when is_map(config) do
    with {:ok, config_ref} <- Config.from_map(config) do
      open(path, config_ref)
    end
  end

  @doc """
  Closes a DuckDB database.
  """
  @spec close(t()) :: :ok
  def close(_database) do
    # Database cleanup is handled by the NIF resource destructor
    :ok
  end

  defp normalize_path(nil), do: nil
  defp normalize_path(:memory), do: nil
  defp normalize_path(path) when is_binary(path), do: path
end
