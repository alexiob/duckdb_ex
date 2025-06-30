defmodule DuckdbEx.Nif do
  @moduledoc """
  Native Implemented Functions (NIFs) for DuckDB.

  This module contains the actual NIF implementations that interface with
  the DuckDB C library using dirty NIFs for safe concurrent access.
  """

  @on_load :load_nifs

  def load_nifs do
    so_file = Application.app_dir(:duckdb_ex, "priv/duckdb_ex")

    case :erlang.load_nif(so_file, 0) do
      :ok ->
        :ok

      {:error, {:load_failed, _reason}} ->
        # Try to download/build NIF if it's not available
        try_download_nif_and_reload(so_file)

      error ->
        error
    end
  end

  defp try_download_nif_and_reload(so_file) do
    case Code.ensure_loaded(DuckdbEx.NifDownloader) do
      {:module, _} ->
        case DuckdbEx.NifDownloader.download_nif() do
          :ok ->
            # Retry loading after download
            :erlang.load_nif(so_file, 0)

          {:error, reason} ->
            {:error, {:nif_download_failed, reason}}
        end

      {:error, _} ->
        {:error, :nif_downloader_not_available}
    end
  end

  ## Database Operations

  @doc """
  Opens a DuckDB database (NIF implementation).
  """
  def database_open(_path) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Opens a DuckDB database with configuration (NIF implementation).
  """
  def database_open_ext(_path, _config) do
    :erlang.nif_error(:nif_not_loaded)
  end

  ## Configuration Operations

  @doc """
  Creates a new DuckDB configuration object (NIF implementation).
  """
  def config_create() do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Sets a configuration option (NIF implementation).
  """
  def config_set(_config, _name, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  ## Connection Operations

  @doc """
  Opens a connection to a DuckDB database (NIF implementation).
  """
  def connection_open(_database) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Executes a SQL query (NIF implementation).
  """
  def connection_query(_connection, _sql) do
    :erlang.nif_error(:nif_not_loaded)
  end

  ## Transaction Operations

  @doc """
  Begins a transaction (NIF implementation).
  """
  def connection_begin_transaction(_connection) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Commits a transaction (NIF implementation).
  """
  def connection_commit(_connection) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Rolls back a transaction (NIF implementation).
  """
  def connection_rollback(_connection) do
    :erlang.nif_error(:nif_not_loaded)
  end

  ## Appender Operations

  @doc """
  Creates an appender (NIF implementation).
  """
  def appender_create(_connection, _schema, _table) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Creates an appender with catalog support (NIF implementation).
  """
  def appender_create_ext(_connection, _catalog, _schema, _table) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Gets the column count of an appender (NIF implementation).
  """
  def appender_column_count(_appender) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Flushes an appender (NIF implementation).
  """
  def appender_flush(_appender) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Closes an appender (NIF implementation).
  """
  def appender_close(_appender) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Destroys an appender (NIF implementation).
  """
  def appender_destroy(_appender) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Ends a row in an appender (NIF implementation).
  """
  def appender_end_row(_appender) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a boolean value (NIF implementation).
  """
  def appender_append_bool(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends an 8-bit integer value (NIF implementation).
  """
  def appender_append_int8(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a 16-bit integer value (NIF implementation).
  """
  def appender_append_int16(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a 32-bit integer value (NIF implementation).
  """
  def appender_append_int32(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a 64-bit integer value (NIF implementation).
  """
  def appender_append_int64(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends an unsigned 8-bit integer value (NIF implementation).
  """
  def appender_append_uint8(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends an unsigned 16-bit integer value (NIF implementation).
  """
  def appender_append_uint16(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends an unsigned 32-bit integer value (NIF implementation).
  """
  def appender_append_uint32(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends an unsigned 64-bit integer value (NIF implementation).
  """
  def appender_append_uint64(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a float value (NIF implementation).
  """
  def appender_append_float(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a double value (NIF implementation).
  """
  def appender_append_double(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a varchar value (NIF implementation).
  """
  def appender_append_varchar(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a blob value (NIF implementation).
  """
  def appender_append_blob(_appender, _value) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Appends a NULL value (NIF implementation).
  """
  def appender_append_null(_appender) do
    :erlang.nif_error(:nif_not_loaded)
  end

  ## Prepared Statement Operations

  @doc """
  Prepares a SQL statement (NIF implementation).
  """
  def prepared_statement_prepare(_connection, _sql) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Executes a prepared statement (NIF implementation).
  """
  def prepared_statement_execute(_prepared_statement, _params) do
    :erlang.nif_error(:nif_not_loaded)
  end

  ## Result Operations

  @doc """
  Gets column information from a result (NIF implementation).
  """
  def result_columns(_result) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Gets all rows from a result (NIF implementation).
  """
  def result_rows(_result) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Gets the number of rows in a result (NIF implementation).
  """
  def result_row_count(_result) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Gets the number of columns in a result (NIF implementation).
  """
  def result_column_count(_result) do
    :erlang.nif_error(:nif_not_loaded)
  end

  ## Chunked API Operations

  @doc """
  Gets the number of chunks in a result (NIF implementation).
  """
  def result_chunk_count(_result) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Gets a specific chunk from a result (NIF implementation).
  """
  def result_get_chunk(_result, _chunk_index) do
    :erlang.nif_error(:nif_not_loaded)
  end

  @doc """
  Gets data from a data chunk (NIF implementation).
  """
  def data_chunk_get_data(_data_chunk) do
    :erlang.nif_error(:nif_not_loaded)
  end
end
