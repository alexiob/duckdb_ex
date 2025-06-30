defmodule DuckdbEx do
  @moduledoc """
  Elixir NIF wrapper for DuckDB database.

  This module provides a safe interface to DuckDB using dirty NIFs for concurrent access.
  DuckDB is an in-process analytical database that supports full SQL.

  ## Basic Usage

      # Open database
      {:ok, db} = DuckdbEx.open()
      {:ok, conn} = DuckdbEx.connect(db)

      # Execute query
      {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer")

      # Get results
      columns = DuckdbEx.columns(result)
      rows = DuckdbEx.rows(result)

      # Cleanup
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)

  ## Configuration

      # Open database with configuration
      config_map = %{
        "memory_limit" => "1GB",
        "threads" => "4",
        "access_mode" => "READ_ONLY"
      }
      {:ok, db} = DuckdbEx.open("my_database.db", config_map)

      # Or use Config object for more control
      {:ok, config} = DuckdbEx.Config.new()
                      |> DuckdbEx.Config.set("memory_limit", "2GB")
                      |> DuckdbEx.Config.set("threads", "8")
      {:ok, db} = DuckdbEx.open("my_database.db", config)  ## Bulk Data Loading

      # Use Appender for high-performance bulk inserts
      {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "my_table")

      # Add rows one by one
      :ok = DuckdbEx.Appender.append_row(appender, [1, "hello", 3.14])
      :ok = DuckdbEx.Appender.append_row(appender, [2, "world", 2.71])

      # Or add multiple rows at once
      rows = [[3, "foo", 1.41], [4, "bar", 1.73]]
      :ok = DuckdbEx.Appender.append_rows(appender, rows)

      # Flush and close
      :ok = DuckdbEx.Appender.flush(appender)
      :ok = DuckdbEx.Appender.close(appender)

  ## Supported Data Types

  DuckDB supports a comprehensive set of data types that are accessible through both
  the regular query interface and the chunked data API:

  ### Primitive Types
  - `:boolean` - Boolean values (true/false)
  - `:tinyint` - 8-bit signed integer (-128 to 127)
  - `:smallint` - 16-bit signed integer (-32,768 to 32,767)
  - `:integer` - 32-bit signed integer (-2,147,483,648 to 2,147,483,647)
  - `:bigint` - 64-bit signed integer
  - `:utinyint` - 8-bit unsigned integer (0 to 255)
  - `:usmallint` - 16-bit unsigned integer (0 to 65,535)
  - `:uinteger` - 32-bit unsigned integer (0 to 4,294,967,295)
  - `:ubigint` - 64-bit unsigned integer
  - `:hugeint` - 128-bit signed integer
  - `:uhugeint` - 128-bit unsigned integer
  - `:float` - 32-bit floating point number
  - `:double` - 64-bit floating point number
  - `:decimal` - Fixed-precision decimal number
  - `:varchar` - Variable-length character string
  - `:blob` - Binary large object

  ### Temporal Types
  - `:date` - Calendar date (year, month, day)
  - `:time` - Time of day (hour, minute, second, microsecond)
  - `:timestamp` - Date and time
  - `:timestamp_s` - Timestamp with second precision
  - `:timestamp_ms` - Timestamp with millisecond precision
  - `:timestamp_ns` - Timestamp with nanosecond precision
  - `:timestamp_tz` - Timestamp with timezone
  - `:time_tz` - Time with timezone
  - `:interval` - Time interval (months, days, microseconds)

  ### Complex Types
  - `:list` - Variable-length list of values
  - `:array` - Fixed-length array of values
  - `:struct` - Structured record with named fields
  - `:map` - Key-value mapping
  - `:union` - Union of multiple types

  ### Special Types
  - `:enum` - Enumerated type with predefined values
  - `:uuid` - Universally unique identifier
  - `:bit` - Bit string

  ## Note on Type Support

  All types are supported in the chunked data API (`DuckdbEx.fetch_chunk/2`), which
  provides the most efficient and comprehensive data access. The regular query API
  (`DuckdbEx.rows/1`) provides good support for primitive types but may return
  placeholder strings for complex types like structs, lists, and maps.

  For working with complex types, prefer the chunked API or consider using
  SQL functions to extract or flatten the data as needed.
  """

  alias DuckdbEx.{Database, Connection, Result, PreparedStatement, Extension, Transaction, Config}

  @type database :: Database.t()
  @type connection :: Connection.t()
  @type result :: Result.t()
  @type prepared_statement :: PreparedStatement.t()
  @type config :: Config.t()

  ## Database Operations

  @doc """
  Opens a DuckDB database.

  ## Parameters
  - `path` - Path to database file. Use `nil` or `:memory` for in-memory database.

  ## Examples

      # In-memory database
      {:ok, db} = DuckdbEx.open()
      {:ok, db} = DuckdbEx.open(:memory)

      # File database
      {:ok, db} = DuckdbEx.open("my_database.db")
  """
  @spec open(String.t() | nil | :memory) :: {:ok, database} | {:error, String.t()}
  def open(path \\ nil) do
    Database.open(path)
  end

  @doc """
  Opens a DuckDB database with configuration.

  ## Parameters
  - `path` - Path to database file. Use `nil` or `:memory` for in-memory database.
  - `config` - Configuration object or map of configuration options

  ## Examples

      # Using a Config object
      {:ok, config} = DuckdbEx.Config.new()
                      |> DuckdbEx.Config.set("memory_limit", "1GB")
                      |> DuckdbEx.Config.set("threads", "4")
      {:ok, db} = DuckdbEx.open("my_database.db", config)

      # Using a configuration map
      config_map = %{
        "memory_limit" => "1GB",
        "threads" => "4",
        "access_mode" => "READ_ONLY"
      }
      {:ok, db} = DuckdbEx.open("my_database.db", config_map)
  """
  @spec open(String.t() | nil | :memory, config | map()) :: {:ok, database} | {:error, String.t()}
  def open(path, config) do
    Database.open(path, config)
  end

  @doc """
  Closes a DuckDB database.

  ## Parameters
  - `database` - The database to close
  """
  @spec close_database(database) :: :ok
  def close_database(database) do
    Database.close(database)
  end

  ## Connection Operations

  @doc """
  Opens a connection to a DuckDB database.

  ## Parameters
  - `database` - The database to connect to

  ## Examples

      {:ok, db} = DuckdbEx.open()
      {:ok, conn} = DuckdbEx.connect(db)
  """
  @spec connect(database) :: {:ok, connection} | {:error, String.t()}
  def connect(database) do
    Connection.open(database)
  end

  @doc """
  Closes a database connection.

  ## Parameters
  - `connection` - The connection to close
  """
  @spec close_connection(connection) :: :ok
  def close_connection(connection) do
    Connection.close(connection)
  end

  @doc """
  Closes a database connection (alias for close_connection/1).

  ## Parameters
  - `connection` - The connection to close
  """
  @spec disconnect(connection) :: :ok
  def disconnect(connection) do
    close_connection(connection)
  end

  ## Query Operations

  @doc """
  Executes a SQL query.

  ## Parameters
  - `connection` - The database connection
  - `sql` - The SQL query string

  ## Examples

      {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as num, 'hello' as text")
  """
  @spec query(connection, String.t()) :: {:ok, result} | {:error, String.t()}
  def query(connection, sql) do
    Connection.query(connection, sql)
  end

  ## Transaction Operations

  @doc """
  Begins a new transaction.

  ## Parameters
  - `connection` - The database connection

  ## Examples

      :ok = DuckdbEx.begin_transaction(conn)
  """
  @spec begin_transaction(connection) :: :ok | {:error, String.t()}
  def begin_transaction(connection) do
    Transaction.begin(connection)
  end

  @doc """
  Commits the current transaction.

  ## Parameters
  - `connection` - The database connection

  ## Examples

      :ok = DuckdbEx.commit(conn)
  """
  @spec commit(connection) :: :ok | {:error, String.t()}
  def commit(connection) do
    Transaction.commit(connection)
  end

  @doc """
  Rolls back the current transaction.

  ## Parameters
  - `connection` - The database connection

  ## Examples

      :ok = DuckdbEx.rollback(conn)
  """
  @spec rollback(connection) :: :ok | {:error, String.t()}
  def rollback(connection) do
    Transaction.rollback(connection)
  end

  @doc """
  Executes a function within a transaction.

  If the function returns `{:ok, result}`, the transaction is committed.
  If the function returns `{:error, reason}` or raises an exception, the transaction is rolled back.

  ## Parameters
  - `connection` - The database connection
  - `fun` - Function to execute within the transaction

  ## Examples

      result = DuckdbEx.with_transaction(conn, fn ->
        {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (name) VALUES ('Alice')")
        {:ok, _} = DuckdbEx.query(conn, "INSERT INTO users (name) VALUES ('Bob')")
        {:ok, "Users inserted"}
      end)
  """
  @spec with_transaction(connection, (-> {:ok, any()} | {:error, any()})) ::
          {:ok, any()} | {:error, any()}
  def with_transaction(connection, fun) do
    Transaction.with_transaction(connection, fun)
  end

  ## Prepared Statement Operations

  @doc """
  Prepares a SQL statement for repeated execution.

  ## Parameters
  - `connection` - The database connection
  - `sql` - The SQL statement with parameter placeholders (?)

  ## Examples

      {:ok, stmt} = DuckdbEx.prepare(conn, "SELECT * FROM users WHERE age > ?")
  """
  @spec prepare(connection, String.t()) :: {:ok, prepared_statement} | {:error, String.t()}
  def prepare(connection, sql) do
    PreparedStatement.prepare(connection, sql)
  end

  @doc """
  Executes a prepared statement with parameters.

  ## Parameters
  - `prepared_statement` - The prepared statement
  - `params` - List of parameter values

  ## Examples

      {:ok, stmt} = DuckdbEx.prepare(conn, "SELECT * FROM users WHERE age > ?")
      {:ok, result} = DuckdbEx.execute(stmt, [25])
  """
  @spec execute(prepared_statement, list()) :: {:ok, result} | {:error, String.t()}
  def execute(prepared_statement, params \\ []) do
    PreparedStatement.execute(prepared_statement, params)
  end

  @doc """
  Destroys a prepared statement and frees its resources.

  ## Parameters
  - `prepared_statement` - The prepared statement to destroy
  """
  @spec destroy_prepared_statement(prepared_statement) :: :ok
  def destroy_prepared_statement(prepared_statement) do
    PreparedStatement.destroy(prepared_statement)
  end

  ## Result Operations

  @doc """
  Gets the column information from a query result.

  ## Parameters
  - `result` - The query result

  ## Returns
  List of column maps with `:name` and `:type` keys

  ## Examples

      {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as num, 'hello' as text")
      columns = DuckdbEx.columns(result)
      # [%{name: "num", type: :integer}, %{name: "text", type: :varchar}]
  """
  @spec columns(result) :: [%{name: String.t(), type: atom()}]
  def columns(result) do
    Result.columns(result)
  end

  @doc """
  Gets all rows from a query result.

  ## Parameters
  - `result` - The query result

  ## Examples

      {:ok, result} = DuckdbEx.query(conn, "SELECT 1 as num, 'hello' as text")
      rows = DuckdbEx.rows(result)
      # [{1, "hello"}]
  """
  @spec rows(result | {:ok, result} | {:error, String.t()}) :: [tuple()] | {[map()], [tuple()]}
  def rows({:ok, result}) do
    # Handle pattern where query result tuple is passed directly
    # Return {columns, rows} for backward compatibility with some tests
    columns = Result.columns(result)
    raw_rows = Result.rows(result)

    # Convert each row by applying type conversion to each column
    processed_rows =
      Enum.map(raw_rows, fn row ->
        row
        |> Tuple.to_list()
        |> Enum.zip(columns)
        |> Enum.map(fn {value, column} ->
          DuckdbEx.TypeConverter.convert_value(value, column.type)
        end)
        |> List.to_tuple()
      end)

    {columns, processed_rows}
  end

  def rows({:error, reason}) do
    raise ArgumentError, "Query failed: #{reason}"
  end

  def rows(result) do
    # Handle direct result reference - return just rows
    columns = Result.columns(result)
    raw_rows = Result.rows(result)

    # Convert each row by applying type conversion to each column
    Enum.map(raw_rows, fn row ->
      row
      |> Tuple.to_list()
      |> Enum.zip(columns)
      |> Enum.map(fn {value, column} ->
        DuckdbEx.TypeConverter.convert_value(value, column.type)
      end)
      |> List.to_tuple()
    end)
  end

  @doc """
  Gets all rows from a query result using the chunked API.

  This function provides better support for complex data types like arrays and lists.
  Use this when working with FLOAT[], LIST, or other complex types.

  ## Parameters
  - `result` - The query result

  ## Examples

      {:ok, result} = DuckdbEx.query(conn, "SELECT [1.0, 2.0, 3.0] as vector")
      rows = DuckdbEx.rows_chunked(result)
      # [{[1.0, 2.0, 3.0]}] - Arrays are properly parsed as Elixir lists
  """
  @spec rows_chunked(result | {:ok, result} | {:error, String.t()}) ::
          [tuple()] | {[map()], [tuple()]}
  def rows_chunked({:ok, result}) do
    # Handle pattern where query result tuple is passed directly
    # Return {columns, rows} for backward compatibility with some tests
    columns = Result.columns(result)
    raw_rows = Result.rows_chunked(result)

    # Convert each row by applying type conversion to each column
    processed_rows =
      Enum.map(raw_rows, fn row ->
        row
        |> Tuple.to_list()
        |> Enum.zip(columns)
        |> Enum.map(fn {value, column} ->
          DuckdbEx.TypeConverter.convert_value(value, column.type)
        end)
        |> List.to_tuple()
      end)

    {columns, processed_rows}
  end

  def rows_chunked({:error, reason}) do
    raise ArgumentError, "Query failed: #{reason}"
  end

  def rows_chunked(result) do
    # Handle direct result reference - return just rows
    columns = Result.columns(result)
    raw_rows = Result.rows_chunked(result)

    # Convert each row by applying type conversion to each column
    Enum.map(raw_rows, fn row ->
      row
      |> Tuple.to_list()
      |> Enum.zip(columns)
      |> Enum.map(fn {value, column} ->
        DuckdbEx.TypeConverter.convert_value(value, column.type)
      end)
      |> List.to_tuple()
    end)
  end

  @doc """
  Gets the number of rows in a query result.

  ## Parameters
  - `result` - The query result
  """
  @spec row_count(result) :: non_neg_integer()
  def row_count(result) do
    Result.row_count(result)
  end

  @doc """
  Gets the number of columns in a query result.

  ## Parameters
  - `result` - The query result
  """
  @spec column_count(result) :: non_neg_integer()
  def column_count(result) do
    Result.column_count(result)
  end

  @doc """
  Destroys a result and frees its resources.

  ## Parameters
  - `result` - The result to destroy
  """
  @spec destroy_result(result) :: :ok
  def destroy_result(result) do
    Result.destroy(result)
  end

  ## Chunked Data Operations

  @doc """
  Gets a specific chunk from a result.

  ## Parameters
  - `result` - The query result
  - `chunk_index` - Index of the chunk to retrieve

  ## Examples

      {:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3] as arr")
      {:ok, chunk} = DuckdbEx.result_get_chunk(result, 0)
  """
  @spec result_get_chunk(result, non_neg_integer()) :: {:ok, reference()} | {:error, String.t()}
  def result_get_chunk(result, chunk_index) do
    Result.get_chunk(result, chunk_index)
  end

  @doc """
  Gets the number of chunks in a result.

  ## Parameters
  - `result` - The query result

  ## Examples

      {:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3] as arr")
      count = DuckdbEx.chunk_count(result)
  """
  @spec chunk_count(result) :: non_neg_integer()
  def chunk_count(result) do
    Result.chunk_count(result)
  end

  @doc """
  Extracts data from a data chunk.

  ## Parameters
  - `chunk` - The data chunk reference

  ## Examples

      {:ok, result} = DuckdbEx.query(conn, "SELECT [1, 2, 3] as arr")
      {:ok, chunk} = DuckdbEx.result_get_chunk(result, 0)
      chunk_data = DuckdbEx.data_chunk_get_data(chunk)
  """
  @spec data_chunk_get_data(reference()) :: [tuple()]
  def data_chunk_get_data(chunk) do
    DuckdbEx.Nif.data_chunk_get_data(chunk)
  end

  @doc """
  Alias for data_chunk_get_data/1 for backwards compatibility.
  """
  @spec chunk_to_data(reference()) :: [tuple()]
  def chunk_to_data(chunk) do
    data_chunk_get_data(chunk)
  end

  @doc """
  Alias for result_get_chunk/2 for backwards compatibility.
  """
  @spec fetch_chunk(result, non_neg_integer()) :: {:ok, reference()} | {:error, String.t()}
  def fetch_chunk(result, chunk_index) do
    result_get_chunk(result, chunk_index)
  end

  ## Extension Operations

  @doc """
  Lists all available extensions.

  Returns information about all extensions including their installation and load status.

  ## Examples

      {:ok, extensions} = DuckdbEx.list_extensions(conn)
  """
  @spec list_extensions(connection) :: {:ok, [Extension.extension_info()]} | {:error, String.t()}
  def list_extensions(connection) do
    Extension.list_extensions(connection)
  end

  @doc """
  Installs a core extension.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension to install

  ## Examples

      :ok = DuckdbEx.install_extension(conn, "json")
  """
  @spec install_extension(connection, String.t()) :: :ok | {:error, String.t()}
  def install_extension(connection, extension_name) do
    Extension.install_extension(connection, extension_name)
  end

  @doc """
  Loads an installed extension.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension to load

  ## Examples

      :ok = DuckdbEx.load_extension(conn, "json")
  """
  @spec load_extension(connection, String.t()) :: :ok | {:error, String.t()}
  def load_extension(connection, extension_name) do
    Extension.load_extension(connection, extension_name)
  end

  @doc """
  Loads an extension from a local file path.

  ## Parameters
  - `connection` - Active database connection
  - `path` - Full path to the extension file

  ## Examples

      :ok = DuckdbEx.load_extension_from_path(conn, "/path/to/extension.so")
  """
  @spec load_extension_from_path(connection, String.t()) :: :ok | {:error, String.t()}
  def load_extension_from_path(connection, path) do
    Extension.load_extension_from_path(connection, path)
  end

  @doc """
  Installs and loads an extension in one step.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension

  ## Examples

      :ok = DuckdbEx.install_and_load(conn, "json")
  """
  @spec install_and_load(connection, String.t()) :: :ok | {:error, String.t()}
  def install_and_load(connection, extension_name) do
    Extension.install_and_load(connection, extension_name)
  end

  @doc """
  Checks if an extension is currently loaded.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension to check

  ## Examples

      true = DuckdbEx.extension_loaded?(conn, "json")
  """
  @spec extension_loaded?(connection, String.t()) :: boolean()
  def extension_loaded?(connection, extension_name) do
    Extension.extension_loaded?(connection, extension_name)
  end
end
