defmodule DuckdbEx.Config do
  @moduledoc """
  Configuration options for DuckDB database.

  This module provides a way to configure DuckDB database settings before opening a database.
  Configuration options are passed to `DuckdbEx.open/2` to create a database with specific settings.

  ## Examples

      # Create a config with custom settings
      config = DuckdbEx.Config.new()
               |> DuckdbEx.Config.set("memory_limit", "1GB")
               |> DuckdbEx.Config.set("threads", "4")

      # Open database with config
      {:ok, db} = DuckdbEx.open("my_db.db", config)

      # Or using a map
      config_map = %{
        "memory_limit" => "1GB",
        "threads" => "4",
        "access_mode" => "READ_ONLY"
      }
      {:ok, db} = DuckdbEx.open("my_db.db", config_map)
  """

  @type t :: reference()

  @doc """
  Creates a new configuration object.

  ## Examples

      config = DuckdbEx.Config.new()
  """
  @spec new() :: {:ok, t()} | {:error, String.t()}
  def new() do
    case DuckdbEx.Nif.config_create() do
      {:ok, ref} -> {:ok, ref}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Sets a configuration option.

  ## Parameters
  - `config` - The configuration object
  - `name` - The name of the configuration option
  - `value` - The value to set

  ## Examples

      config = DuckdbEx.Config.new()
      config = DuckdbEx.Config.set(config, "memory_limit", "1GB")

  ## Common Configuration Options

  - `"access_mode"` - Database access mode ("AUTOMATIC", "READ_ONLY", "READ_WRITE")
  - `"memory_limit"` - Memory limit (e.g., "1GB", "512MB")
  - `"threads"` - Number of threads to use (integer as string)
  - `"max_memory"` - Maximum memory usage
  - `"default_order"` - Default ordering ("ASC" or "DESC")
  - `"enable_profiling"` - Enable query profiling ("true" or "false")
  - `"profiling_output"` - Profiling output file path
  """
  @spec set(t(), String.t(), String.t()) :: {:ok, t()} | {:error, String.t()}
  def set(config, name, value) when is_binary(name) and is_binary(value) do
    case DuckdbEx.Nif.config_set(config, name, value) do
      :ok -> {:ok, config}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Sets a configuration option, raising on error.

  ## Parameters
  - `config` - The configuration object
  - `name` - The name of the configuration option
  - `value` - The value to set

  ## Examples

      config = DuckdbEx.Config.new()
               |> DuckdbEx.Config.set!("memory_limit", "1GB")
               |> DuckdbEx.Config.set!("threads", "4")
  """
  @spec set!(t(), String.t(), String.t()) :: t()
  def set!(config, name, value) do
    case set(config, name, value) do
      {:ok, config} -> config
      {:error, reason} -> raise "Failed to set config option '#{name}': #{reason}"
    end
  end

  @doc """
  Sets multiple configuration options from a map.

  ## Parameters
  - `config` - The configuration object
  - `options` - A map of option names to values

  ## Examples

      config = DuckdbEx.Config.new()
      options = %{
        "memory_limit" => "1GB",
        "threads" => "4",
        "access_mode" => "READ_ONLY"
      }
      {:ok, config} = DuckdbEx.Config.set_all(config, options)
  """
  @spec set_all(t(), map()) :: {:ok, t()} | {:error, String.t()}
  def set_all(config, options) when is_map(options) do
    Enum.reduce_while(options, {:ok, config}, fn {name, value}, {:ok, acc_config} ->
      case set(acc_config, to_string(name), to_string(value)) do
        {:ok, new_config} -> {:cont, {:ok, new_config}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  @doc """
  Sets multiple configuration options from a map, raising on error.

  ## Parameters
  - `config` - The configuration object
  - `options` - A map of option names to values

  ## Examples

      config = DuckdbEx.Config.new()
               |> DuckdbEx.Config.set_all!(%{
                 "memory_limit" => "1GB",
                 "threads" => "4"
               })
  """
  @spec set_all!(t(), map()) :: t()
  def set_all!(config, options) do
    case set_all(config, options) do
      {:ok, config} -> config
      {:error, reason} -> raise "Failed to set config options: #{reason}"
    end
  end

  @doc """
  Creates a new configuration object with the given options.

  ## Parameters
  - `options` - A map of option names to values

  ## Examples

      {:ok, config} = DuckdbEx.Config.from_map(%{
        "memory_limit" => "1GB",
        "threads" => "4"
      })

      # Or with the bang version
      config = DuckdbEx.Config.from_map!(%{
        "memory_limit" => "1GB",
        "threads" => "4"
      })
  """
  @spec from_map(map()) :: {:ok, t()} | {:error, String.t()}
  def from_map(options) when is_map(options) do
    with {:ok, config} <- new(),
         {:ok, config} <- set_all(config, options) do
      {:ok, config}
    end
  end

  @doc """
  Creates a new configuration object with the given options, raising on error.
  """
  @spec from_map!(map()) :: t()
  def from_map!(options) do
    case from_map(options) do
      {:ok, config} -> config
      {:error, reason} -> raise "Failed to create config from map: #{reason}"
    end
  end

  @doc """
  Destroys a configuration object and frees its memory.

  Note: This is automatically called when the configuration object is garbage collected,
  so manual cleanup is not usually necessary.
  """
  @spec destroy(t()) :: :ok
  def destroy(_config) do
    # Cleanup is handled by the NIF resource destructor
    :ok
  end
end
