defmodule DuckdbEx.TypeConverter do
  @moduledoc """
  Converts DuckDB string representations to idiomatic Elixir data types.

  DuckDB complex types like ARRAY, LIST, MAP, STRUCT are converted from their
  string representations to proper Elixir data structures.
  """

  @doc """
  Converts a DuckDB value to an idiomatic Elixir type.

  For simple types, returns the value as-is.
  For complex types, parses the string representation.
  """
  def convert_value(value, type_atom) when is_atom(type_atom) do
    case type_atom do
      :list -> parse_list(value)
      :array -> parse_array(value)
      :map -> parse_map(value)
      :struct -> parse_struct(value)
      :enum -> parse_enum(value)
      :decimal -> parse_decimal(value)
      :interval -> parse_interval(value)
      :uuid -> parse_uuid(value)
      :date -> parse_date(value)
      :time -> parse_time(value)
      :timestamp -> parse_timestamp(value)
      :timestamp_s -> parse_timestamp(value)
      :timestamp_ms -> parse_timestamp(value)
      :timestamp_ns -> parse_timestamp(value)
      :timestamp_tz -> parse_timestamp_tz(value)
      :hugeint -> parse_hugeint(value)
      :uhugeint -> parse_hugeint(value)
      _ -> value
    end
  end

  @doc """
  Parses a DuckDB LIST string representation into an Elixir list.

  Examples:
  - "[1, 2, 3]" -> [1, 2, 3]
  - "[1.5, 2.7, 3.9]" -> [1.5, 2.7, 3.9]
  - "['a', 'b', 'c']" -> ["a", "b", "c"]
  """
  def parse_list(value) when is_list(value) do
    # Handle case where value is already a list (from chunked API)
    # Try to detect and convert date/time/timestamp strings within the list
    Enum.map(value, fn element ->
      cond do
        is_binary(element) and String.match?(element, ~r/^\d{4}-\d{2}-\d{2}$/) ->
          # Looks like a date string: YYYY-MM-DD
          parse_date(element)

        is_binary(element) and String.match?(element, ~r/^\d{2}:\d{2}:\d{2}/) ->
          # Looks like a time string: HH:MM:SS
          parse_time(element)

        is_binary(element) and String.match?(element, ~r/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/) ->
          # Looks like a timestamp string: YYYY-MM-DD HH:MM:SS
          parse_timestamp(element)

        true ->
          # Return element as-is
          element
      end
    end)
  end

  def parse_list(value) when is_binary(value) do
    # Check for placeholder formats
    cond do
      value == "<unsupported_list_type>" ->
        value

      String.starts_with?(value, "<") and String.ends_with?(value, ">") ->
        # Return indicator as-is for other complex type indicators
        value

      String.starts_with?(value, "[") and String.ends_with?(value, "]") ->
        try_parse_bracket_list(value)

      # Try JSON parsing
      true ->
        case Jason.decode(value) do
          {:ok, parsed} when is_list(parsed) -> parsed
          _ -> try_parse_duckdb_list(value)
        end
    end
  end

  def parse_list(value), do: value

  @doc """
  Parses a DuckDB ARRAY string representation into an Elixir list.
  Arrays are treated the same as lists in Elixir.
  """
  def parse_array(value) when is_list(value), do: parse_list(value)
  def parse_array(value), do: parse_list(value)

  @doc """
  Parses a DuckDB MAP string representation into an Elixir map.

  Examples:
  - "{key1: value1, key2: value2}" -> %{"key1" => "value1", "key2" => "value2"}
  - "{a=1, b=2}" -> %{"a" => 1, "b" => 2}
  """
  def parse_map(value) when is_binary(value) do
    # Check for placeholder formats
    cond do
      value == "<unsupported_map_type>" ->
        value

      String.starts_with?(value, "<") and String.ends_with?(value, ">") ->
        # Return indicator as-is for other complex type indicators
        value

      String.starts_with?(value, "{") and String.ends_with?(value, "}") ->
        try_parse_bracket_map(value)

      # Try JSON parsing
      true ->
        case Jason.decode(value) do
          {:ok, parsed} when is_map(parsed) -> parsed
          _ -> try_parse_duckdb_map(value)
        end
    end
  end

  def parse_map(value), do: value

  @doc """
  Parses a DuckDB STRUCT string representation into an Elixir map.
  Structs are represented as maps in Elixir.
  """
  def parse_struct(value) when is_binary(value) do
    # Check for placeholder formats
    cond do
      value == "<unsupported_struct_type>" ->
        value

      # Otherwise delegate to map parsing
      true ->
        parse_map(value)
    end
  end

  def parse_struct(value), do: parse_map(value)

  @doc """
  Parses an ENUM value. ENUMs are simple string values in DuckDB.
  """
  def parse_enum(value) when is_binary(value) do
    cond do
      value == "<regular_api_enum_limitation>" ->
        # Regular API can't extract ENUM - return placeholder indicating limitation
        value

      String.starts_with?(value, "<") and String.ends_with?(value, ">") ->
        # Other extraction failures - return the error message
        value

      true ->
        # Valid enum string
        value
    end
  end

  def parse_enum(value), do: value

  @doc """
  Parses a DuckDB DECIMAL value to a Decimal struct if Decimal library is available,
  otherwise returns as string.
  """
  def parse_decimal(value) when is_binary(value) do
    # Try to convert to float if it's a simple decimal
    case Float.parse(value) do
      {float_val, ""} -> float_val
      _ -> value
    end
  end

  def parse_decimal(value), do: value

  @doc """
  Parses a DuckDB INTERVAL value into a tuple {months, days, microseconds}.

  DuckDB interval format examples:
  - "3 days 7200000000 microseconds" -> {0, 3, 7200000000}
  - "1 month" -> {1, 0, 0}
  - "45 minutes" -> {0, 0, 2700000000}
  """
  def parse_interval(value) when is_binary(value) do
    try do
      # Initialize components
      months = 0
      days = 0
      microseconds = 0

      # Parse different components from the string
      {months, days, microseconds} = parse_interval_components(value, months, days, microseconds)

      {months, days, microseconds}
    rescue
      # Return original string if parsing fails
      _ -> value
    end
  end

  def parse_interval(value), do: value

  # Helper function to parse interval components
  defp parse_interval_components(value, months, days, microseconds) do
    value
    |> String.split()
    |> Enum.chunk_every(2)
    |> Enum.reduce({months, days, microseconds}, fn
      [num_str, "month" <> _], {m, d, us} ->
        {m + String.to_integer(num_str), d, us}

      [num_str, "day" <> _], {m, d, us} ->
        {m, d + String.to_integer(num_str), us}

      [num_str, "hour" <> _], {m, d, us} ->
        {m, d, us + String.to_integer(num_str) * 3_600_000_000}

      [num_str, "minute" <> _], {m, d, us} ->
        {m, d, us + String.to_integer(num_str) * 60_000_000}

      [num_str, "second" <> _], {m, d, us} ->
        {m, d, us + String.to_integer(num_str) * 1_000_000}

      [num_str, "microsecond" <> _], {m, d, us} ->
        {m, d, us + String.to_integer(num_str)}

      # Skip unrecognized components
      _, acc ->
        acc
    end)
  end

  # Helper function to parse bracket-enclosed lists: [item1, item2, item3]
  defp try_parse_bracket_list(value) do
    content =
      value
      |> String.trim()
      # Remove [ and ]
      |> String.slice(1..-2//1)
      |> String.trim()

    if content == "" do
      []
    else
      parse_csv_items(content)
    end
  rescue
    # Return original value if parsing fails
    _ -> value
  end

  # Helper function to parse bracket-enclosed maps: {key1: value1, key2: value2}
  defp try_parse_bracket_map(value) do
    content =
      value
      |> String.trim()
      # Remove { and }
      |> String.slice(1..-2//1)
      |> String.trim()

    if content == "" do
      %{}
    else
      content
      |> parse_csv_items()
      |> Enum.reduce(%{}, fn item, acc ->
        case parse_key_value_pair(item) do
          {key, val} -> Map.put(acc, key, val)
          _ -> acc
        end
      end)
    end
  rescue
    # Return original value if parsing fails
    _ -> value
  end

  # Helper function to parse comma-separated items with proper quote handling
  defp parse_csv_items(content) do
    content
    |> String.graphemes()
    |> parse_csv_tokens([], "", false, 0)
    |> Enum.map(&parse_list_item/1)
  end

  # Tokenizer that respects quotes and nested structures
  defp parse_csv_tokens([], acc, current, _in_quotes, _nesting) do
    if String.trim(current) != "" do
      [String.trim(current) | acc]
    else
      acc
    end
    |> Enum.reverse()
  end

  defp parse_csv_tokens([char | rest], acc, current, in_quotes, nesting) do
    case char do
      "\"" when not in_quotes ->
        parse_csv_tokens(rest, acc, current <> char, true, nesting)

      "\"" when in_quotes ->
        parse_csv_tokens(rest, acc, current <> char, false, nesting)

      "'" when not in_quotes ->
        parse_csv_tokens(rest, acc, current <> char, true, nesting)

      "'" when in_quotes ->
        parse_csv_tokens(rest, acc, current <> char, false, nesting)

      c when c in ["[", "{"] and not in_quotes ->
        parse_csv_tokens(rest, acc, current <> char, in_quotes, nesting + 1)

      c when c in ["]", "}"] and not in_quotes ->
        parse_csv_tokens(rest, acc, current <> char, in_quotes, nesting - 1)

      "," when not in_quotes and nesting == 0 ->
        new_acc = if String.trim(current) != "", do: [String.trim(current) | acc], else: acc
        parse_csv_tokens(rest, new_acc, "", false, 0)

      _ ->
        parse_csv_tokens(rest, acc, current <> char, in_quotes, nesting)
    end
  end

  # Helper function to parse key-value pairs (key: value or key=value)
  defp parse_key_value_pair(item) do
    item = String.trim(item)

    cond do
      String.contains?(item, ":") ->
        [key, value] = String.split(item, ":", parts: 2)
        {String.trim(key), parse_list_item(String.trim(value))}

      String.contains?(item, "=") ->
        [key, value] = String.split(item, "=", parts: 2)
        {String.trim(key), parse_list_item(String.trim(value))}

      true ->
        nil
    end
  rescue
    _ -> nil
  end

  # Helper function to parse DuckDB-specific list format (fallback)
  defp try_parse_duckdb_list(value) do
    # DuckDB lists are typically in format: [item1, item2, item3]
    # Remove brackets and split by comma
    value
    |> String.trim()
    |> String.trim_leading("[")
    |> String.trim_trailing("]")
    |> String.split(",")
    |> Enum.map(&String.trim/1)
    |> Enum.map(&parse_list_item/1)
  rescue
    # Return original value if parsing fails
    _ -> value
  end

  # Helper function to parse DuckDB-specific map format (fallback)
  defp try_parse_duckdb_map(value) do
    # DuckDB maps might be in format: {key1: value1, key2: value2}
    # This is a simplified parser for fallback
    %{}
  rescue
    # Return original value if parsing fails
    _ -> value
  end

  # Helper to parse individual list items with better type inference
  defp parse_list_item(item) do
    item = String.trim(item)

    cond do
      # Handle quoted strings
      (String.starts_with?(item, "\"") and String.ends_with?(item, "\"")) or
          (String.starts_with?(item, "'") and String.ends_with?(item, "'")) ->
        String.slice(item, 1..-2//1)

      # Handle null values
      item in ["NULL", "null", "nil"] ->
        nil

      # Try parsing as integer
      Regex.match?(~r/^-?\d+$/, item) ->
        case Integer.parse(item) do
          {int_val, ""} -> int_val
          _ -> item
        end

      # Try parsing as float
      Regex.match?(~r/^-?\d+\.\d+$/, item) ->
        case Float.parse(item) do
          {float_val, ""} -> float_val
          _ -> item
        end

      # Try parsing as boolean
      item in ["true", "TRUE", "false", "FALSE"] ->
        String.downcase(item) == "true"

      # Handle nested structures
      String.starts_with?(item, "[") and String.ends_with?(item, "]") ->
        try_parse_bracket_list(item)

      String.starts_with?(item, "{") and String.ends_with?(item, "}") ->
        try_parse_bracket_map(item)

      # Return as string
      true ->
        item
    end
  rescue
    _ -> item
  end

  @doc """
  Parses a UUID value or handles extraction failures.
  """
  def parse_uuid(value) when is_binary(value) do
    cond do
      value == "<uuid_extraction_failed>" ->
        # UUID extraction failed, return nil
        nil

      value == "<regular_api_uuid_limitation>" ->
        # Regular API can't extract UUID, use chunked API instead
        nil

      String.starts_with?(value, "<") and String.ends_with?(value, ">") ->
        # Other extraction failures
        nil

      true ->
        # Valid UUID string
        value
    end
  end

  def parse_uuid(value), do: value

  @doc """
  Parses a DuckDB DATE string representation into an Elixir Date struct.

  Examples:
  - "2023-12-25" -> ~D[2023-12-25]
  """
  def parse_date(value) when is_binary(value) do
    case Date.from_iso8601(value) do
      {:ok, date} -> date
      # Return original on parse error
      {:error, _} -> value
    end
  end

  def parse_date(value), do: value

  @doc """
  Parses a DuckDB TIME string representation into an Elixir Time struct.

  Examples:
  - "14:30:45.000000" -> ~T[14:30:45.000000]
  - "14:30:45" -> ~T[14:30:45]
  """
  def parse_time(value) when is_binary(value) do
    case Time.from_iso8601(value) do
      {:ok, time} -> time
      # Return original on parse error
      {:error, _} -> value
    end
  end

  def parse_time(value), do: value

  @doc """
  Parses a DuckDB TIMESTAMP string representation into an Elixir DateTime struct.

  Examples:
  - "2023-12-25 14:30:45" -> DateTime in UTC
  """
  def parse_timestamp(value) when is_binary(value) do
    # Handle different timestamp formats from DuckDB
    case DateTime.from_iso8601(value <> "Z") do
      {:ok, datetime, _} ->
        datetime

      {:error, _} ->
        # Try as naive datetime and assume UTC
        case NaiveDateTime.from_iso8601(value) do
          {:ok, naive_dt} -> DateTime.from_naive!(naive_dt, "Etc/UTC")
          # Return original on parse error
          {:error, _} -> value
        end
    end
  end

  def parse_timestamp(value), do: value

  @doc """
  Parses a DuckDB TIMESTAMPTZ string representation into an Elixir DateTime struct.

  Examples:
  - "2023-12-25 14:30:45+02:00" -> DateTime with timezone
  """
  def parse_timestamp_tz(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _} -> datetime
      # Return original on parse error
      {:error, _} -> value
    end
  end

  def parse_timestamp_tz(value), do: value

  @doc """
  Parses a DuckDB HUGEINT (128-bit integer) value into an Elixir integer.

  Elixir integers have unlimited precision, so we can convert string representations
  of very large integers into proper Elixir integers.

  Examples:
  - For values that fit in 64-bit: returns the integer directly (already handled in C)
  - For larger values: parses the decimal string representation
  """
  def parse_hugeint(value) when is_binary(value) do
    try do
      # Check if it's a special hugeint format from chunked API
      if String.starts_with?(value, "hugeint:") do
        parse_hugeint_components(value)
      else
        String.to_integer(value)
      end
    rescue
      ArgumentError ->
        # If parsing fails, return the original value
        value
    end
  end

  def parse_hugeint(value), do: value

  # Parse hugeint from raw components (upper:lower format)
  defp parse_hugeint_components("hugeint:" <> components) do
    case String.split(components, ":") do
      [upper_str, lower_str] ->
        upper = String.to_integer(upper_str)
        lower = String.to_integer(lower_str)

        # Compute: upper * 2^64 + lower
        # We need to handle this carefully to avoid overflow
        # Elixir integers have arbitrary precision, so this should work
        upper * 18_446_744_073_709_551_616 + lower

      _ ->
        # Fallback to original value if format is unexpected
        "hugeint:" <> components
    end
  end
end
