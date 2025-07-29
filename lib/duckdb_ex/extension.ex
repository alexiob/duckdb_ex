defmodule DuckdbEx.Extension do
  @moduledoc """
  Extension management for DuckDB.

  DuckDB supports both core extensions (built-in or downloadable) and third-party extensions.
  This module provides functions to manage and load extensions.

  ## Core Extensions

  Core extensions are maintained by the DuckDB team and can be installed/loaded via:
  - `install_extension/2` - Downloads and installs an extension
  - `load_extension/2` - Loads an installed extension

  ## Available Core Extensions

  - `json` - JSON functions and operators
  - `parquet` - Parquet file format support
  - `iceberg` - Apache Iceberg support
  - `delta` - Delta Lake support
  - `aws` - AWS services integration
  - `azure` - Azure services integration
  - `httpfs` - HTTP/S3 filesystem support
  - `postgres_scanner` - PostgreSQL scanner
  - `sqlite_scanner` - SQLite scanner
  - `mysql_scanner` - MySQL scanner
  - `autocomplete` - SQL autocomplete

  ## Third-Party Extensions

  Third-party extensions can be loaded from local files using `load_extension_from_path/2`.

  ## Vector Similarity Search (VSS) Extension

  The VSS extension provides vector operations and similarity search:
  - Use standard extension API: `install_and_load/2` or `install_extension/2` + `load_extension/2`
  - Supports cosine similarity, L2 distance, inner product
  - Vector indexing for fast similarity search
  - Built-in array functions like `array_cosine_similarity/2`

  ## Examples

      # Load a core extension
      {:ok, conn} = DuckdbEx.connect(db)
      :ok = DuckdbEx.Extension.install_extension(conn, "json")
      :ok = DuckdbEx.Extension.load_extension(conn, "json")

      # Load extension from local file
      :ok = DuckdbEx.Extension.load_extension_from_path(conn, "/path/to/extension.so")

      # Vector Similarity Search extension
      :ok = DuckdbEx.Extension.install_and_load(conn, "vss")

      # List available extensions
      {:ok, extensions} = DuckdbEx.Extension.list_extensions(conn)

      # Check if extension is loaded
      true = DuckdbEx.Extension.extension_loaded?(conn, "json")
      true = DuckdbEx.Extension.extension_loaded?(conn, "vss")
  """

  alias DuckdbEx.Connection

  @type extension_name :: String.t()
  @type extension_info :: %{
          name: String.t(),
          loaded: boolean(),
          installed: boolean(),
          description: String.t(),
          version: String.t() | nil
        }

  @doc """
  Lists all available extensions.

  Returns information about all extensions including their installation and load status.
  """
  @spec list_extensions(Connection.t()) :: {:ok, [extension_info()]} | {:error, String.t()}
  def list_extensions(connection) do
    case DuckdbEx.query(connection, "SELECT * FROM duckdb_extensions()") do
      {:ok, result} ->
        rows = DuckdbEx.rows(result)
        DuckdbEx.destroy_result(result)

        extensions =
          Enum.map(rows, fn {name, loaded, installed, description, aliases, extension_version,
                             install_path, load_path, installed_version} ->
            %{
              name: name,
              loaded: loaded,
              installed: installed,
              description: description,
              aliases: aliases,
              extension_version: extension_version,
              install_path: install_path,
              load_path: load_path,
              installed_version: installed_version
            }
          end)

        {:ok, extensions}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Installs a core extension.

  Downloads and installs the specified extension. This requires internet connectivity
  for downloading from the DuckDB extension repository.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension to install

  ## Examples

      :ok = DuckdbEx.Extension.install_extension(conn, "json")
      :ok = DuckdbEx.Extension.install_extension(conn, "parquet")
  """
  @spec install_extension(Connection.t(), extension_name()) :: :ok | {:error, String.t()}
  def install_extension(connection, extension_name) when is_binary(extension_name) do
    case DuckdbEx.query(connection, "INSTALL #{extension_name}") do
      {:ok, result} ->
        DuckdbEx.destroy_result(result)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Installs an extension.

  Alias for `install_extension/2` for documentation compatibility.
  """
  @spec install(Connection.t(), extension_name()) :: :ok | {:error, String.t()}
  def install(connection, extension_name), do: install_extension(connection, extension_name)

  @doc """
  Loads an installed extension.

  Loads a previously installed extension into the current session.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension to load

  ## Examples

      :ok = DuckdbEx.Extension.load_extension(conn, "json")
      :ok = DuckdbEx.Extension.load_extension(conn, "parquet")
  """
  @spec load_extension(Connection.t(), extension_name()) :: :ok | {:error, String.t()}
  def load_extension(connection, extension_name) when is_binary(extension_name) do
    case DuckdbEx.query(connection, "LOAD #{extension_name}") do
      {:ok, result} ->
        DuckdbEx.destroy_result(result)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Loads an extension.

  Alias for `load_extension/2` for documentation compatibility.
  """
  @spec load(Connection.t(), extension_name()) :: :ok | {:error, String.t()}
  def load(connection, extension_name), do: load_extension(connection, extension_name)

  @doc """
  Loads an extension from a local file path.

  This allows loading third-party extensions or locally built extensions.

  ## Parameters
  - `connection` - Active database connection
  - `path` - Full path to the extension file (.so, .dll, or .dylib)

  ## Examples

      :ok = DuckdbEx.Extension.load_extension_from_path(conn, "/usr/local/lib/my_extension.so")
      :ok = DuckdbEx.Extension.load_extension_from_path(conn, "./custom_extension.dylib")
  """
  @spec load_extension_from_path(Connection.t(), String.t()) :: :ok | {:error, String.t()}
  def load_extension_from_path(connection, path) when is_binary(path) do
    # Use single quotes to handle paths with special characters
    case DuckdbEx.query(connection, "LOAD '#{path}'") do
      {:ok, result} ->
        DuckdbEx.destroy_result(result)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Installs and loads an extension in one step.

  Convenience function that installs and then loads an extension.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension

  ## Examples

      :ok = DuckdbEx.Extension.install_and_load(conn, "json")
  """
  @spec install_and_load(Connection.t(), extension_name()) :: :ok | {:error, String.t()}
  def install_and_load(connection, extension_name) do
    with :ok <- install_extension(connection, extension_name),
         :ok <- load_extension(connection, extension_name) do
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Checks if an extension is currently loaded.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension to check

  ## Examples

      true = DuckdbEx.Extension.extension_loaded?(conn, "json")
      false = DuckdbEx.Extension.extension_loaded?(conn, "nonexistent")
  """
  @spec extension_loaded?(Connection.t(), extension_name()) :: boolean()
  def extension_loaded?(connection, extension_name) do
    case DuckdbEx.query(
           connection,
           "SELECT loaded FROM duckdb_extensions() WHERE extension_name = '#{extension_name}'"
         ) do
      {:ok, result} ->
        rows = DuckdbEx.rows(result)
        DuckdbEx.destroy_result(result)

        case rows do
          [{true}] -> true
          _ -> false
        end

      {:error, _} ->
        false
    end
  end

  @doc """
  Gets information about a specific extension.

  ## Parameters
  - `connection` - Active database connection
  - `extension_name` - Name of the extension

  ## Examples

      {:ok, info} = DuckdbEx.Extension.get_extension_info(conn, "json")
      {:error, "Extension not found"} = DuckdbEx.Extension.get_extension_info(conn, "nonexistent")
  """
  @spec get_extension_info(Connection.t(), extension_name()) ::
          {:ok, extension_info()} | {:error, String.t()}
  def get_extension_info(connection, extension_name) do
    case DuckdbEx.query(
           connection,
           "SELECT * FROM duckdb_extensions() WHERE extension_name = '#{extension_name}'"
         ) do
      {:ok, result} ->
        rows = DuckdbEx.rows(result)
        DuckdbEx.destroy_result(result)

        case rows do
          [
            {name, loaded, installed, description, aliases, extension_version, install_path,
             load_path, installed_version}
          ] ->
            info = %{
              name: name,
              loaded: loaded,
              installed: installed,
              description: description,
              aliases: aliases,
              extension_version: extension_version,
              install_path: install_path,
              load_path: load_path,
              installed_version: installed_version
            }

            {:ok, info}

          [] ->
            {:error, "Extension not found"}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Lists all loaded extensions.

  Returns only the extensions that are currently loaded in the session.

  ## Examples

      {:ok, loaded_extensions} = DuckdbEx.Extension.list_loaded_extensions(conn)
  """
  @spec list_loaded_extensions(Connection.t()) :: {:ok, [String.t()]} | {:error, String.t()}
  def list_loaded_extensions(connection) do
    case DuckdbEx.query(
           connection,
           "SELECT extension_name FROM duckdb_extensions() WHERE loaded = true"
         ) do
      {:ok, result} ->
        rows = DuckdbEx.rows(result)
        DuckdbEx.destroy_result(result)
        loaded_extensions = Enum.map(rows, fn {name} -> name end)
        {:ok, loaded_extensions}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Sets the extension directory for loading third-party extensions.

  This configures where DuckDB should look for extension files.

  ## Parameters
  - `connection` - Active database connection
  - `directory` - Path to the extension directory

  ## Examples

      :ok = DuckdbEx.Extension.set_extension_directory(conn, "/usr/local/lib/duckdb_extensions")
  """
  @spec set_extension_directory(Connection.t(), String.t()) :: :ok | {:error, String.t()}
  def set_extension_directory(connection, directory) do
    case DuckdbEx.query(connection, "SET extension_directory = '#{directory}'") do
      {:ok, result} ->
        DuckdbEx.destroy_result(result)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Lists all available extensions.

  Alias for `list_extensions/1` for documentation compatibility.
  """
  @spec list(Connection.t()) :: {:ok, [extension_info()]} | {:error, String.t()}
  def list(connection), do: list_extensions(connection)
end
