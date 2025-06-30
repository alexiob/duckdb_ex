defmodule DuckdbEx.Result do
  @moduledoc """
  Result resource management for DuckDB query results.
  """

  @type t :: reference()

  @doc """
  Gets column information from a result.
  """
  @spec columns(t()) :: [%{name: String.t(), type: atom()}]
  def columns(result) do
    DuckdbEx.Nif.result_columns(result)
  end

  @doc """
  Gets all rows from a result.
  """
  @spec rows(t()) :: [tuple()]
  def rows(result) do
    DuckdbEx.Nif.result_rows(result)
  end

  @doc """
  Gets the number of rows in a result.
  """
  @spec row_count(t()) :: non_neg_integer()
  def row_count(result) do
    DuckdbEx.Nif.result_row_count(result)
  end

  @doc """
  Gets the number of columns in a result.
  """
  @spec column_count(t()) :: non_neg_integer()
  def column_count(result) do
    DuckdbEx.Nif.result_column_count(result)
  end

  @doc """
  Destroys a result and frees its resources.
  """
  @spec destroy(t()) :: :ok
  def destroy(_result) do
    # Result cleanup is handled by the NIF resource destructor
    :ok
  end

  @doc """
  Gets all rows from a result using the chunked API.
  This provides better support for complex types like arrays and lists.
  """
  @spec rows_chunked(t()) :: [tuple()]
  def rows_chunked(result) do
    chunk_count = DuckdbEx.Nif.result_chunk_count(result)

    # Handle case where there are no chunks
    if chunk_count == 0 do
      []
    else
      # Collect rows from all chunks
      all_rows =
        for chunk_idx <- 0..(chunk_count - 1) do
          case DuckdbEx.Nif.result_get_chunk(result, chunk_idx) do
            {:ok, chunk} -> DuckdbEx.Nif.data_chunk_get_data(chunk)
            {:error, _reason} -> []
          end
        end

      # Flatten the list of chunks into a single list of rows
      List.flatten(all_rows)
    end
  end

  @doc """
  Gets the number of chunks in a result.
  """
  @spec chunk_count(t()) :: non_neg_integer()
  def chunk_count(result) do
    DuckdbEx.Nif.result_chunk_count(result)
  end

  @doc """
  Gets a specific chunk from a result.
  """
  @spec get_chunk(t(), non_neg_integer()) :: {:ok, reference()} | {:error, String.t()}
  def get_chunk(result, chunk_index) do
    DuckdbEx.Nif.result_get_chunk(result, chunk_index)
  end
end
