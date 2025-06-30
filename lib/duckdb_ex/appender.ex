defmodule DuckdbEx.Appender do
  @moduledoc """
  High-performance bulk data insertion via DuckDB Appender API.

  The Appender provides the most efficient way of loading data into DuckDB from within the Elixir API.
  It is recommended for fast data loading as it performs better than prepared statements or individual
  `INSERT INTO` statements.

  ## Usage

  The basic workflow for using an appender is:

  1. Create an appender for a specific table using `create/3` or `create/4`
  2. Append data row by row using `append_*` functions
  3. Call `end_row/1` after each complete row
  4. Call `close/1` to finalize the data
  5. Call `destroy/1` to clean up resources

  ## Example

      # Create a table first
      {:ok, result} = DuckdbEx.query(conn, "CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")

      # Create an appender
      {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "users")

      # Append data row by row
      :ok = DuckdbEx.Appender.append_int32(appender, 1)
      :ok = DuckdbEx.Appender.append_varchar(appender, "Alice")
      :ok = DuckdbEx.Appender.append_int32(appender, 30)
      :ok = DuckdbEx.Appender.end_row(appender)

      :ok = DuckdbEx.Appender.append_int32(appender, 2)
      :ok = DuckdbEx.Appender.append_varchar(appender, "Bob")
      :ok = DuckdbEx.Appender.append_int32(appender, 25)
      :ok = DuckdbEx.Appender.end_row(appender)

      # Finalize and cleanup
      :ok = DuckdbEx.Appender.close(appender)
      :ok = DuckdbEx.Appender.destroy(appender)

  ## Batch Append Helper

  For convenience, you can also use `append_rows/2` to append multiple rows at once:

      {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "users")

      rows = [
        [1, "Alice", 30],
        [2, "Bob", 25],
        [3, "Charlie", 35]
      ]

      :ok = DuckdbEx.Appender.append_rows(appender, rows)
      :ok = DuckdbEx.Appender.close(appender)
      :ok = DuckdbEx.Appender.destroy(appender)
  """

  alias DuckdbEx.{Connection, Nif}

  @type t :: reference()
  @type connection :: Connection.t()

  ## Appender Creation and Management

  @doc """
  Creates an appender object for the specified table.

  ## Parameters
  - `connection` - The database connection
  - `schema` - The schema name (use `nil` for default schema)
  - `table` - The table name

  ## Returns
  - `{:ok, appender}` on success
  - `{:error, reason}` on failure

  ## Examples

      {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "my_table")
      {:ok, appender} = DuckdbEx.Appender.create(conn, "public", "my_table")
  """
  @spec create(connection, String.t() | nil, String.t()) :: {:ok, t()} | {:error, String.t()}
  def create(connection, schema, table) do
    Nif.appender_create(connection, schema, table)
  end

  @doc """
  Creates an appender object for the specified table with catalog support.

  ## Parameters
  - `connection` - The database connection
  - `catalog` - The catalog name (use `nil` for default catalog)
  - `schema` - The schema name (use `nil` for default schema)
  - `table` - The table name

  ## Returns
  - `{:ok, appender}` on success
  - `{:error, reason}` on failure

  ## Examples

      {:ok, appender} = DuckdbEx.Appender.create_ext(conn, nil, nil, "my_table")
      {:ok, appender} = DuckdbEx.Appender.create_ext(conn, "main", "public", "my_table")
  """
  @spec create_ext(connection, String.t() | nil, String.t() | nil, String.t()) ::
          {:ok, t()} | {:error, String.t()}
  def create_ext(connection, catalog, schema, table) do
    Nif.appender_create_ext(connection, catalog, schema, table)
  end

  @doc """
  Returns the number of columns in the appender.

  ## Parameters
  - `appender` - The appender object

  ## Returns
  - The number of columns as an integer

  ## Examples

      column_count = DuckdbEx.Appender.column_count(appender)
  """
  @spec column_count(t()) :: non_neg_integer()
  def column_count(appender) do
    Nif.appender_column_count(appender)
  end

  @doc """
  Flushes the appender to the table, forcing the cache to be cleared.

  ## Parameters
  - `appender` - The appender object

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      :ok = DuckdbEx.Appender.flush(appender)
  """
  @spec flush(t()) :: :ok | {:error, String.t()}
  def flush(appender) do
    Nif.appender_flush(appender)
  end

  @doc """
  Closes the appender by flushing all data and finalizing it.

  ## Parameters
  - `appender` - The appender object

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      :ok = DuckdbEx.Appender.close(appender)
  """
  @spec close(t()) :: :ok | {:error, String.t()}
  def close(appender) do
    Nif.appender_close(appender)
  end

  @doc """
  Destroys the appender and frees all associated memory.

  ## Parameters
  - `appender` - The appender object

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      :ok = DuckdbEx.Appender.destroy(appender)
  """
  @spec destroy(t()) :: :ok | {:error, String.t()}
  def destroy(appender) do
    Nif.appender_destroy(appender)
  end

  ## Row Management

  @doc """
  Finishes the current row of appends. Must be called after each complete row.

  ## Parameters
  - `appender` - The appender object

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      :ok = DuckdbEx.Appender.append_int32(appender, 42)
      :ok = DuckdbEx.Appender.append_varchar(appender, "hello")
      :ok = DuckdbEx.Appender.end_row(appender)
  """
  @spec end_row(t()) :: :ok | {:error, String.t()}
  def end_row(appender) do
    Nif.appender_end_row(appender)
  end

  ## Value Appending Functions

  @doc """
  Appends a boolean value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The boolean value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_bool(t(), boolean()) :: :ok | {:error, String.t()}
  def append_bool(appender, value) do
    Nif.appender_append_bool(appender, value)
  end

  @doc """
  Appends an 8-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append (-128 to 127)

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_int8(t(), integer()) :: :ok | {:error, String.t()}
  def append_int8(appender, value) do
    Nif.appender_append_int8(appender, value)
  end

  @doc """
  Appends a 16-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append (-32,768 to 32,767)

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_int16(t(), integer()) :: :ok | {:error, String.t()}
  def append_int16(appender, value) do
    Nif.appender_append_int16(appender, value)
  end

  @doc """
  Appends a 32-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_int32(t(), integer()) :: :ok | {:error, String.t()}
  def append_int32(appender, value) do
    Nif.appender_append_int32(appender, value)
  end

  @doc """
  Appends a 64-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_int64(t(), integer()) :: :ok | {:error, String.t()}
  def append_int64(appender, value) do
    Nif.appender_append_int64(appender, value)
  end

  @doc """
  Appends an unsigned 8-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append (0 to 255)

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_uint8(t(), non_neg_integer()) :: :ok | {:error, String.t()}
  def append_uint8(appender, value) do
    Nif.appender_append_uint8(appender, value)
  end

  @doc """
  Appends an unsigned 16-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append (0 to 65,535)

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_uint16(t(), non_neg_integer()) :: :ok | {:error, String.t()}
  def append_uint16(appender, value) do
    Nif.appender_append_uint16(appender, value)
  end

  @doc """
  Appends an unsigned 32-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_uint32(t(), non_neg_integer()) :: :ok | {:error, String.t()}
  def append_uint32(appender, value) do
    Nif.appender_append_uint32(appender, value)
  end

  @doc """
  Appends an unsigned 64-bit integer value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The integer value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_uint64(t(), non_neg_integer()) :: :ok | {:error, String.t()}
  def append_uint64(appender, value) do
    Nif.appender_append_uint64(appender, value)
  end

  @doc """
  Appends a float value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The float value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_float(t(), float()) :: :ok | {:error, String.t()}
  def append_float(appender, value) do
    Nif.appender_append_float(appender, value)
  end

  @doc """
  Appends a double value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The double value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_double(t(), float()) :: :ok | {:error, String.t()}
  def append_double(appender, value) do
    Nif.appender_append_double(appender, value)
  end

  @doc """
  Appends a varchar (string) value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The string value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_varchar(t(), String.t()) :: :ok | {:error, String.t()}
  def append_varchar(appender, value) do
    Nif.appender_append_varchar(appender, value)
  end

  @doc """
  Appends a blob (binary) value to the appender.

  ## Parameters
  - `appender` - The appender object
  - `value` - The binary value to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_blob(t(), binary()) :: :ok | {:error, String.t()}
  def append_blob(appender, value) do
    Nif.appender_append_blob(appender, value)
  end

  @doc """
  Appends a NULL value to the appender.

  ## Parameters
  - `appender` - The appender object

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure
  """
  @spec append_null(t()) :: :ok | {:error, String.t()}
  def append_null(appender) do
    Nif.appender_append_null(appender)
  end

  ## High-level Helper Functions

  @doc """
  Appends multiple rows of data to the appender.

  This is a convenience function that automatically handles the row management
  for you. Each row should be a list of values in the correct order for the table columns.

  ## Parameters
  - `appender` - The appender object
  - `rows` - A list of rows, where each row is a list of values

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      rows = [
        [1, "Alice", 30],
        [2, "Bob", 25],
        [3, "Charlie", 35]
      ]
      :ok = DuckdbEx.Appender.append_rows(appender, rows)
  """
  @spec append_rows(t(), [[any()]]) :: :ok | {:error, String.t()}
  def append_rows(appender, rows) when is_list(rows) do
    Enum.reduce_while(rows, :ok, fn row, :ok ->
      case append_row(appender, row) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  @doc """
  Appends a single row of data to the appender.

  This is a convenience function that automatically appends all values in the row
  and calls `end_row/1` for you.

  ## Parameters
  - `appender` - The appender object
  - `row` - A list of values in the correct order for the table columns

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      :ok = DuckdbEx.Appender.append_row(appender, [1, "Alice", 30])
  """
  @spec append_row(t(), [any()]) :: :ok | {:error, String.t()}
  def append_row(appender, row) when is_list(row) do
    with :ok <- append_values(appender, row),
         :ok <- end_row(appender) do
      :ok
    end
  end

  # Private helper to append a list of values
  defp append_values(appender, values) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case append_value(appender, value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  # Private helper to append a single value of any type
  defp append_value(appender, nil), do: append_null(appender)
  defp append_value(appender, value) when is_boolean(value), do: append_bool(appender, value)
  defp append_value(appender, value) when is_integer(value), do: append_int64(appender, value)
  defp append_value(appender, value) when is_float(value), do: append_double(appender, value)
  defp append_value(appender, value) when is_binary(value), do: append_varchar(appender, value)

  defp append_value(_appender, value) do
    {:error, "Unsupported value type: #{inspect(value)}"}
  end

  @doc """
  Creates an appender, appends rows, and automatically closes and destroys it.

  This is a convenience function that handles the complete appender lifecycle.

  ## Parameters
  - `connection` - The database connection
  - `schema` - The schema name (use `nil` for default schema)
  - `table` - The table name
  - `rows` - A list of rows to append

  ## Returns
  - `:ok` on success
  - `{:error, reason}` on failure

  ## Examples

      rows = [
        [1, "Alice", 30],
        [2, "Bob", 25]
      ]
      :ok = DuckdbEx.Appender.insert_rows(conn, nil, "users", rows)
  """
  @spec insert_rows(connection, String.t() | nil, String.t(), [[any()]]) ::
          :ok | {:error, String.t()}
  def insert_rows(connection, schema, table, rows) do
    case create(connection, schema, table) do
      {:ok, appender} ->
        case append_rows(appender, rows) do
          :ok ->
            case close(appender) do
              :ok ->
                destroy(appender)

              {:error, _} = error ->
                destroy(appender)
                error
            end

          {:error, _} = error ->
            destroy(appender)
            error
        end

      {:error, _} = error ->
        error
    end
  end
end
