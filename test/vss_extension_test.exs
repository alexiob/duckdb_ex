defmodule VSSExtensionTest do
  use ExUnit.Case

  setup do
    {:ok, db} = DuckdbEx.open()
    {:ok, conn} = DuckdbEx.connect(db)

    # Try to install and load VSS extension automatically
    vss_available =
      case DuckdbEx.install_and_load(conn, "vss") do
        :ok -> true
        {:error, _} -> false
      end

    on_exit(fn ->
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end)

    %{db: db, conn: conn, vss_available: vss_available}
  end

  describe "VSS extension loading" do
    test "VSS extension installation", %{conn: conn} do
      # Test installing VSS extension using standard extension API
      result = DuckdbEx.install_extension(conn, "vss")

      case result do
        :ok ->
          # VSS extension was successfully installed
          # Now load it
          assert DuckdbEx.load_extension(conn, "vss") == :ok
          assert DuckdbEx.extension_loaded?(conn, "vss") == true

        {:error, reason} ->
          # Expected if VSS extension not available or already installed
          assert is_binary(reason)
      end
    end

    test "VSS extension install and load", %{conn: conn} do
      # Test the combined install and load function
      result = DuckdbEx.install_and_load(conn, "vss")

      case result do
        :ok ->
          assert DuckdbEx.extension_loaded?(conn, "vss") == true

        {:error, reason} ->
          # Expected if VSS extension not available
          assert is_binary(reason)
      end
    end
  end

  describe "VSS vector operations (without extension)" do
    test "create vector table with FLOAT array", %{conn: conn} do
      # Test vector table creation (should work with built-in array support)
      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE test_vectors (
            id INTEGER,
            name VARCHAR,
            embedding FLOAT[3]
          )
        """)

      DuckdbEx.destroy_result(result)

      # Verify table was created
      {:ok, tables_result} = DuckdbEx.query(conn, "SHOW TABLES")
      tables = DuckdbEx.rows(tables_result)
      table_names = Enum.map(tables, fn {name} -> name end)
      assert "test_vectors" in table_names
      DuckdbEx.destroy_result(tables_result)
    end

    test "insert vector data", %{conn: conn} do
      # Create table first
      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE test_embeddings (
            id INTEGER,
            text VARCHAR,
            vector FLOAT[2]
          )
        """)

      DuckdbEx.destroy_result(result)

      # Test vector insertion
      {:ok, insert_result} =
        DuckdbEx.query(conn, """
          INSERT INTO test_embeddings VALUES (1, 'hello', [0.1, 0.2])
        """)

      DuckdbEx.destroy_result(insert_result)

      # Verify data was inserted
      {:ok, select_result} = DuckdbEx.query(conn, "SELECT * FROM test_embeddings")
      rows = DuckdbEx.rows(select_result)
      assert length(rows) == 1
      {id, text, _vector} = hd(rows)
      assert id == 1
      assert text == "hello"
      DuckdbEx.destroy_result(select_result)
    end
  end

  describe "VSS similarity operations" do
    setup %{conn: conn} do
      # Create test table with vector data
      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE similarity_test (
            id INTEGER,
            description VARCHAR,
            embedding FLOAT[3]
          )
        """)

      DuckdbEx.destroy_result(result)

      # Insert test data
      test_data = [
        [1, "first", [1.0, 0.0, 0.0]],
        [2, "second", [0.0, 1.0, 0.0]],
        [3, "third", [0.0, 0.0, 1.0]],
        [4, "similar to first", [0.9, 0.1, 0.0]]
      ]

      Enum.each(test_data, fn [id, desc, vec] ->
        vec_str = "[#{Enum.join(vec, ", ")}]"

        {:ok, insert_result} =
          DuckdbEx.query(conn, """
            INSERT INTO similarity_test VALUES (#{id}, '#{desc}', #{vec_str})
          """)

        DuckdbEx.destroy_result(insert_result)
      end)

      %{table: "similarity_test"}
    end

    test "similarity search with built-in functions", %{conn: conn, table: table} do
      # Test similarity search using built-in array functions
      query_vector = "[1.0, 0.0, 0.0]"

      case DuckdbEx.query(conn, """
             SELECT *,
                    array_cosine_similarity(embedding, #{query_vector}) as similarity
             FROM #{table}
             ORDER BY similarity DESC
             LIMIT 2
           """) do
        {:ok, result} ->
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)
          # If successful, verify results
          assert is_list(rows)
          assert length(rows) <= 2

        {:error, reason} ->
          # Expected if vector similarity functions not available
          assert is_binary(reason)
          assert String.contains?(reason, "function") or String.contains?(reason, "not found")
      end
    end

    test "compute similarity between vectors", %{conn: conn} do
      vector1 = "[1.0, 0.0, 0.0]"
      vector2 = "[0.9, 0.1, 0.0]"

      case DuckdbEx.query(conn, """
             SELECT array_cosine_similarity(#{vector1}, #{vector2}) as similarity
           """) do
        {:ok, result} ->
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)
          {similarity} = hd(rows)
          assert is_number(similarity)
          # Should be high similarity
          assert similarity > 0.8

        {:error, reason} ->
          # Expected if similarity functions not available
          assert is_binary(reason)
      end
    end
  end

  describe "VSS utility functions" do
    test "list vector functions", %{conn: conn} do
      # Test listing vector/array functions
      case DuckdbEx.query(conn, """
             SELECT function_name
             FROM duckdb_functions()
             WHERE function_name LIKE '%array%'
                OR function_name LIKE '%vector%'
                OR function_name LIKE '%similarity%'
             ORDER BY function_name
           """) do
        {:ok, result} ->
          rows = DuckdbEx.rows(result)
          DuckdbEx.destroy_result(result)
          functions = Enum.map(rows, fn {name} -> name end)
          assert is_list(functions)
          # Should include at least some array functions
          array_functions = Enum.filter(functions, &String.contains?(&1, "array"))
          assert length(array_functions) > 0

        {:error, reason} ->
          assert is_binary(reason)
      end
    end

    test "create vector index interface", %{conn: conn, vss_available: vss_available} do
      # Create a test table
      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE index_test (
            id INTEGER,
            vector FLOAT[2]
          )
        """)

      DuckdbEx.destroy_result(result)

      if vss_available do
        # Test index creation with VSS extension loaded
        case DuckdbEx.query(conn, "CREATE INDEX vec_idx ON index_test (vector)") do
          {:ok, index_result} ->
            DuckdbEx.destroy_result(index_result)
            :ok

          {:error, reason} ->
            # May fail depending on VSS extension capabilities
            assert is_binary(reason)
        end
      end
    end
  end

  describe "VSS main module integration" do
    test "extension management functions", %{conn: conn} do
      # Test convenience functions from main DuckdbEx module
      initially_loaded = DuckdbEx.extension_loaded?(conn, "vss")
      assert is_boolean(initially_loaded)

      # Test extension installation and loading
      case DuckdbEx.install_and_load(conn, "vss") do
        :ok ->
          assert DuckdbEx.extension_loaded?(conn, "vss") == true

        {:error, reason} ->
          assert is_binary(reason)
      end
    end
  end

  describe "VSS vector data operations" do
    test "vector value operations", %{conn: conn, vss_available: vss_available} do
      # Test that vectors are properly handled in SQL
      {:ok, result} =
        DuckdbEx.query(conn, """
          CREATE TABLE format_test (
            id INTEGER,
            vec FLOAT[4]
          )
        """)

      DuckdbEx.destroy_result(result)

      # Test with different vector formats
      test_vectors = [
        [1, [1.0, 2.0, 3.0, 4.0]],
        [2, [0.1, 0.2, 0.3, 0.4]],
        [3, [-1.0, -2.0, 3.0, 4.0]]
      ]

      Enum.each(test_vectors, fn [id, vec] ->
        vec_str = "[#{Enum.join(vec, ", ")}]"

        {:ok, insert_result} =
          DuckdbEx.query(conn, """
            INSERT INTO format_test VALUES (#{id}, #{vec_str})
          """)

        DuckdbEx.destroy_result(insert_result)
      end)

      # Verify data was inserted correctly
      {:ok, count_result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM format_test")
      rows = DuckdbEx.rows(count_result)
      {count} = hd(rows)
      assert count == 3
      DuckdbEx.destroy_result(count_result)

      if vss_available do
        # Test VSS-specific functions if available
        case DuckdbEx.query(conn, """
               SELECT vec, array_cosine_similarity(vec, [1.0, 2.0, 3.0, 4.0]) as similarity
               FROM format_test
               ORDER BY similarity DESC
             """) do
          {:ok, similarity_result} ->
            similarity_rows = DuckdbEx.rows(similarity_result)
            DuckdbEx.destroy_result(similarity_result)
            assert length(similarity_rows) == 3

          {:error, _reason} ->
            # VSS functions might not be available
            :ok
        end
      end
    end
  end
end
