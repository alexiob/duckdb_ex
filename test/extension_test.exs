defmodule ExtensionTest do
  use ExUnit.Case

  setup do
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    on_exit(fn ->
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end)

    %{db: db, conn: conn}
  end

  describe "extension management" do
    test "list available extensions", %{conn: conn} do
      {:ok, extensions} = DuckdbEx.list_extensions(conn)

      assert is_list(extensions)
      assert length(extensions) > 0

      # Check structure of extension info
      first_ext = hd(extensions)
      assert Map.has_key?(first_ext, :name)
      assert Map.has_key?(first_ext, :loaded)
      assert Map.has_key?(first_ext, :installed)
      assert Map.has_key?(first_ext, :description)

      # Should include core_functions (built-in)
      core_functions = Enum.find(extensions, fn ext -> ext.name == "core_functions" end)
      assert core_functions != nil
      assert core_functions.loaded == true
    end

    test "check if extension is loaded", %{conn: conn} do
      # core_functions should always be loaded
      assert DuckdbEx.extension_loaded?(conn, "core_functions") == true

      # Non-existent extension should not be loaded
      assert DuckdbEx.extension_loaded?(conn, "nonexistent_extension") == false
    end

    test "get extension info", %{conn: conn} do
      {:ok, info} = DuckdbEx.Extension.get_extension_info(conn, "core_functions")

      assert info.name == "core_functions"
      assert info.loaded == true
      assert is_binary(info.description)

      # Test non-existent extension
      {:error, reason} = DuckdbEx.Extension.get_extension_info(conn, "nonexistent")
      assert reason == "Extension not found"
    end

    test "list loaded extensions", %{conn: conn} do
      {:ok, loaded} = DuckdbEx.Extension.list_loaded_extensions(conn)

      assert is_list(loaded)
      assert "core_functions" in loaded
    end

    test "set extension directory", %{conn: conn} do
      # This should not fail even if directory doesn't exist
      result = DuckdbEx.Extension.set_extension_directory(conn, "/tmp/test_extensions")
      assert result == :ok
    end

    # Note: We skip actual extension installation tests because they require
    # network connectivity and may fail in CI environments. Instead, we test
    # the interface and error handling.

    test "install extension handles network errors gracefully", %{conn: conn} do
      # This might fail due to network issues, but should return proper error
      case DuckdbEx.install_extension(conn, "json") do
        :ok ->
          # If successful, try to load it
          assert DuckdbEx.load_extension(conn, "json") == :ok
          assert DuckdbEx.extension_loaded?(conn, "json") == true

        {:error, reason} ->
          # Network or other error is expected in some environments
          assert is_binary(reason)
      end
    end

    test "load extension from invalid path returns error", %{conn: conn} do
      {:error, reason} = DuckdbEx.load_extension_from_path(conn, "/nonexistent/path/extension.so")
      assert is_binary(reason)

      assert String.contains?(reason, "Error") or String.contains?(reason, "failed") or
               String.contains?(reason, "not found")
    end

    test "extension workflow with error handling", %{conn: conn} do
      # Test the complete workflow with a non-existent extension
      extension_name = "definitely_nonexistent_extension_12345"

      # Should not be loaded initially
      assert DuckdbEx.extension_loaded?(conn, extension_name) == false

      # Install should fail
      {:error, _reason} = DuckdbEx.install_extension(conn, extension_name)

      # Load should also fail
      {:error, _reason} = DuckdbEx.load_extension(conn, extension_name)

      # Install and load should fail
      {:error, _reason} = DuckdbEx.install_and_load(conn, extension_name)

      # Should still not be loaded
      assert DuckdbEx.extension_loaded?(conn, extension_name) == false
    end
  end

  describe "SQL-based extension functions" do
    test "can query extension metadata", %{conn: conn} do
      # Test that we can query DuckDB's extension system directly
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM duckdb_extensions()")
      rows = DuckdbEx.rows(result)
      {count} = hd(rows)
      assert count > 0
      DuckdbEx.destroy_result(result)
    end

    test "can filter extensions by status", %{conn: conn} do
      # Test querying loaded extensions via SQL
      {:ok, result} =
        DuckdbEx.query(conn, "SELECT extension_name FROM duckdb_extensions() WHERE loaded = true")

      rows = DuckdbEx.rows(result)
      loaded_names = Enum.map(rows, fn {name} -> name end)
      assert "core_functions" in loaded_names
      DuckdbEx.destroy_result(result)
    end
  end
end
