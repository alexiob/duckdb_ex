defmodule DuckdbEx.Types do
  @moduledoc """
  Type definitions for DuckDB data types.

  This module defines all the DuckDB data types that are supported by the
  Elixir NIF wrapper. These types can be used in table functions, appenders,
  and other type-aware operations.
  """

  @typedoc """
  DuckDB logical data types.

  These correspond to the DUCKDB_TYPE enum values in the C API.
  """
  # Basic types
  @type logical_type ::
          :boolean
          | :tinyint
          | :smallint
          | :integer
          | :bigint
          | :utinyint
          | :usmallint
          | :uinteger
          | :ubigint
          | :float
          | :double
          | :varchar
          | :blob

          # Date/time types
          | :date
          | :time
          | :timestamp
          | :interval
          | :time_tz
          | :timestamp_s
          | :timestamp_ms
          | :timestamp_ns
          | :timestamp_tz

          # Numeric types
          | :hugeint
          | :uhugeint
          | :decimal

          # Complex types
          | :list
          | :array
          | :struct
          | :map
          | :union

          # Special types
          | :enum
          | :uuid
          | :bit

  @doc """
  Returns true if the given type is a primitive type.
  """
  @spec primitive?(logical_type()) :: boolean()
  def primitive?(type) do
    type in [
      :boolean,
      :tinyint,
      :smallint,
      :integer,
      :bigint,
      :utinyint,
      :usmallint,
      :uinteger,
      :ubigint,
      :float,
      :double,
      :varchar,
      :blob,
      :date,
      :time,
      :timestamp,
      :interval,
      :hugeint,
      :uhugeint,
      :decimal,
      :enum,
      :uuid,
      :bit,
      :time_tz,
      :timestamp_s,
      :timestamp_ms,
      :timestamp_ns,
      :timestamp_tz
    ]
  end

  @doc """
  Returns true if the given type is a complex type.
  """
  @spec complex?(logical_type()) :: boolean()
  def complex?(type) do
    type in [:list, :array, :struct, :map, :union]
  end

  @doc """
  Returns true if the given type is a temporal type.
  """
  @spec temporal?(logical_type()) :: boolean()
  def temporal?(type) do
    type in [
      :date,
      :time,
      :timestamp,
      :interval,
      :time_tz,
      :timestamp_s,
      :timestamp_ms,
      :timestamp_ns,
      :timestamp_tz
    ]
  end

  @doc """
  Returns true if the given type is a numeric type.
  """
  @spec numeric?(logical_type()) :: boolean()
  def numeric?(type) do
    type in [
      :tinyint,
      :smallint,
      :integer,
      :bigint,
      :utinyint,
      :usmallint,
      :uinteger,
      :ubigint,
      :float,
      :double,
      :hugeint,
      :uhugeint,
      :decimal
    ]
  end

  @doc """
  Returns true if the given type is an integer type.
  """
  @spec integer?(logical_type()) :: boolean()
  def integer?(type) do
    type in [
      :tinyint,
      :smallint,
      :integer,
      :bigint,
      :utinyint,
      :usmallint,
      :uinteger,
      :ubigint,
      :hugeint,
      :uhugeint
    ]
  end

  @doc """
  Returns true if the given type is a floating point type.
  """
  @spec float?(logical_type()) :: boolean()
  def float?(type) do
    type in [:float, :double]
  end

  @doc """
  Returns true if the given type is a string-like type.
  """
  @spec string_like?(logical_type()) :: boolean()
  def string_like?(type) do
    type in [:varchar, :blob, :bit]
  end

  @doc """
  Returns the Elixir type that best represents the given DuckDB type.
  """
  @spec to_elixir_type(logical_type()) :: atom()
  def to_elixir_type(type) do
    case type do
      :boolean ->
        :boolean

      t when t in [:tinyint, :smallint, :integer] ->
        :integer

      t when t in [:bigint, :hugeint, :uhugeint] ->
        :integer

      t when t in [:utinyint, :usmallint, :uinteger, :ubigint] ->
        :non_neg_integer

      t when t in [:float, :double, :decimal] ->
        :float

      t when t in [:varchar, :blob, :uuid, :bit] ->
        :binary

      t
      when t in [
             :date,
             :time,
             :timestamp,
             :interval,
             :time_tz,
             :timestamp_s,
             :timestamp_ms,
             :timestamp_ns,
             :timestamp_tz
           ] ->
        :binary

      :enum ->
        :binary

      t when t in [:list, :array] ->
        :list

      t when t in [:struct, :map] ->
        :map

      :union ->
        :term

      _ ->
        :term
    end
  end

  @doc """
  Returns a human-readable description of the type.
  """
  @spec describe(logical_type()) :: String.t()
  def describe(type) do
    case type do
      :boolean -> "Boolean (true/false)"
      :tinyint -> "8-bit signed integer (-128 to 127)"
      :smallint -> "16-bit signed integer (-32,768 to 32,767)"
      :integer -> "32-bit signed integer (-2,147,483,648 to 2,147,483,647)"
      :bigint -> "64-bit signed integer"
      :utinyint -> "8-bit unsigned integer (0 to 255)"
      :usmallint -> "16-bit unsigned integer (0 to 65,535)"
      :uinteger -> "32-bit unsigned integer (0 to 4,294,967,295)"
      :ubigint -> "64-bit unsigned integer"
      :hugeint -> "128-bit signed integer"
      :uhugeint -> "128-bit unsigned integer"
      :float -> "32-bit floating point number"
      :double -> "64-bit floating point number"
      :decimal -> "Fixed-precision decimal number"
      :varchar -> "Variable-length character string"
      :blob -> "Binary large object"
      :date -> "Calendar date (year, month, day)"
      :time -> "Time of day (hour, minute, second, microsecond)"
      :timestamp -> "Date and time"
      :timestamp_s -> "Timestamp with second precision"
      :timestamp_ms -> "Timestamp with millisecond precision"
      :timestamp_ns -> "Timestamp with nanosecond precision"
      :timestamp_tz -> "Timestamp with timezone"
      :time_tz -> "Time with timezone"
      :interval -> "Time interval (months, days, microseconds)"
      :list -> "Variable-length list of values"
      :array -> "Fixed-length array of values"
      :struct -> "Structured record with named fields"
      :map -> "Key-value mapping"
      :union -> "Union of multiple types"
      :enum -> "Enumerated type with predefined values"
      :uuid -> "Universally unique identifier"
      :bit -> "Bit string"
      _ -> "Unknown type"
    end
  end
end
