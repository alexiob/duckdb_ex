defmodule DuckdbEx.ConfigTest do
  use ExUnit.Case, async: true

  alias DuckdbEx.Config

  describe "Config.new/0" do
    test "creates a new configuration object" do
      assert {:ok, config} = Config.new()
      assert is_reference(config)
    end
  end

  describe "Config.set/3" do
    test "sets a configuration option" do
      {:ok, config} = Config.new()
      assert {:ok, _config} = Config.set(config, "threads", "2")
    end
  end

  describe "Config.set!/3" do
    test "sets a configuration option successfully" do
      {:ok, config} = Config.new()
      {:ok, config} = Config.set(config, "threads", "2")
      assert is_reference(config)
    end
  end

  describe "Config.set_all/2" do
    test "sets multiple configuration options" do
      {:ok, config} = Config.new()

      options = %{
        "threads" => "2",
        "memory_limit" => "512MB"
      }

      assert {:ok, _config} = Config.set_all(config, options)
    end

    test "works with string keys" do
      {:ok, config} = Config.new()

      options = %{
        "threads" => "2"
      }

      assert {:ok, _config} = Config.set_all(config, options)
    end

    test "works with atom keys" do
      {:ok, config} = Config.new()

      options = %{
        threads: "2"
      }

      assert {:ok, _config} = Config.set_all(config, options)
    end
  end

  describe "Config.set_all!/2" do
    test "sets multiple configuration options successfully" do
      {:ok, config} = Config.new()

      options = %{
        "threads" => "2"
      }

      {:ok, config} = Config.set_all(config, options)
      assert is_reference(config)
    end
  end

  describe "Config.from_map/1" do
    test "creates configuration from map" do
      options = %{
        "threads" => "2",
        "memory_limit" => "512MB"
      }

      assert {:ok, config} = Config.from_map(options)
      assert is_reference(config)
    end
  end

  describe "Config.from_map!/1" do
    test "creates configuration from map successfully" do
      options = %{
        "threads" => "2"
      }

      config = Config.from_map!(options)
      assert is_reference(config)
    end
  end

  describe "Database operations with configuration" do
    test "opens database with configuration object" do
      {:ok, config} = Config.new()
      {:ok, config} = Config.set(config, "threads", "1")

      assert {:ok, db} = DuckdbEx.open(:memory, config)
      assert {:ok, conn} = DuckdbEx.connect(db)

      # Test that the database works with the configuration
      assert {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer")
      assert DuckdbEx.rows(result) == [{42}]

      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end

    test "opens database with configuration map" do
      config_map = %{
        "threads" => "1"
      }

      assert {:ok, db} = DuckdbEx.open(:memory, config_map)
      assert {:ok, conn} = DuckdbEx.connect(db)

      # Test that the database works with the configuration
      assert {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer")
      assert DuckdbEx.rows(result) == [{42}]

      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end

    test "opens file database with configuration" do
      config_map = %{
        "threads" => "1"
      }

      # Use a temporary file path
      db_path = "/tmp/test_config_#{:rand.uniform(1_000_000)}.db"

      try do
        assert {:ok, db} = DuckdbEx.open(db_path, config_map)
        assert {:ok, conn} = DuckdbEx.connect(db)

        # Test that the database works
        assert {:ok, result} = DuckdbEx.query(conn, "SELECT 42 as answer")
        assert DuckdbEx.rows(result) == [{42}]

        DuckdbEx.close_connection(conn)
        DuckdbEx.close_database(db)
      after
        # Clean up the test file
        File.rm(db_path)
      end
    end
  end

  describe "Configuration options" do
    test "can set memory_limit" do
      {:ok, config} = Config.new()
      assert {:ok, _config} = Config.set(config, "memory_limit", "1GB")
    end

    test "can set access_mode" do
      {:ok, config} = Config.new()
      assert {:ok, _config} = Config.set(config, "access_mode", "READ_ONLY")
    end

    test "can set threads" do
      {:ok, config} = Config.new()
      assert {:ok, _config} = Config.set(config, "threads", "4")
    end

    test "can chain configuration settings" do
      {:ok, config} = Config.new()
      {:ok, config} = Config.set(config, "threads", "2")
      {:ok, config} = Config.set(config, "memory_limit", "512MB")

      assert is_reference(config)

      # Test that the configured database works
      assert {:ok, db} = DuckdbEx.open(:memory, config)
      assert {:ok, conn} = DuckdbEx.connect(db)

      assert {:ok, result} = DuckdbEx.query(conn, "SELECT 1")
      assert DuckdbEx.rows(result) == [{1}]

      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end
  end
end
