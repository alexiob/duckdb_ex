# Extensions

DuckDB supports a rich ecosystem of extensions that add functionality for various data formats, sources, and analytical capabilities. DuckdbEx provides access to DuckDB's extension system through SQL commands and configuration options.

## Overview

DuckDB extensions provide:

- **Data Format Support**: Parquet, JSON, CSV, Excel, and more
- **External Data Sources**: HTTP, S3, Azure, GCS, databases
- **Analytical Functions**: Spatial analysis, full-text search, statistics
- **Specialized Types**: JSON, UUID, spatial types
- **Performance Features**: Compression, indexing, caching

## Extension Management

### Installing Extensions

```elixir
{:ok, db} = DuckdbEx.open()
{:ok, conn} = DuckdbEx.connect(db)

# Install popular extensions
extensions_to_install = [
  "httpfs",        # HTTP and S3 file system
  "parquet",       # Parquet file format
  "json",          # JSON functions
  "fts",           # Full-text search
  "spatial"        # Spatial/GIS functions
]

Enum.each(extensions_to_install, fn extension ->
  case DuckdbEx.query(conn, "INSTALL #{extension}") do
    {:ok, _} -> IO.puts("Successfully installed #{extension}")
    {:error, reason} -> IO.puts("Failed to install #{extension}: #{reason}")
  end
end)
```

### Loading Extensions

```elixir
# Load extensions for use in current session
extensions_to_load = ["httpfs", "parquet", "json"]

Enum.each(extensions_to_load, fn extension ->
  case DuckdbEx.query(conn, "LOAD #{extension}") do
    {:ok, _} -> IO.puts("Successfully loaded #{extension}")
    {:error, reason} -> IO.puts("Failed to load #{extension}: #{reason}")
  end
end)

# Verify loaded extensions
{:ok, result} = DuckdbEx.query(conn, """
  SELECT extension_name, loaded, installed
  FROM duckdb_extensions()
  WHERE loaded = true
""")

loaded_extensions = DuckdbEx.rows(result)
IO.puts("Currently loaded extensions:")
Enum.each(loaded_extensions, fn [name, loaded, installed] ->
  IO.puts("  #{name} (loaded: #{loaded}, installed: #{installed})")
end)
```

### Auto-loading Extensions

```elixir
# Configure automatic extension loading
config = %{
  "autoload_known_extensions" => "true",
  "autoinstall_known_extensions" => "false"  # Set to true for auto-install
}

{:ok, db} = DuckdbEx.open("auto_extensions.db", config)
{:ok, conn} = DuckdbEx.connect(db)

# Extensions will be auto-loaded when needed
# For example, reading a Parquet file will auto-load the parquet extension
{:ok, _} = DuckdbEx.query(conn, "CREATE TABLE test AS SELECT * FROM 'example.parquet'")
```

## Popular Extensions

### HTTP and Cloud Storage (httpfs)

```elixir
# Load httpfs extension for external data access
{:ok, _} = DuckdbEx.query(conn, "INSTALL httpfs")
{:ok, _} = DuckdbEx.query(conn, "LOAD httpfs")

# Configure S3 credentials (if needed)
{:ok, _} = DuckdbEx.query(conn, "SET s3_region='us-west-2'")
{:ok, _} = DuckdbEx.query(conn, "SET s3_access_key_id='your_access_key'")
{:ok, _} = DuckdbEx.query(conn, "SET s3_secret_access_key='your_secret_key'")

# Read from HTTP URL
{:ok, result} = DuckdbEx.query(conn, """
  SELECT * FROM read_csv_auto('https://example.com/data.csv')
  LIMIT 10
""")

# Read from S3
{:ok, result} = DuckdbEx.query(conn, """
  SELECT COUNT(*) FROM parquet_scan('s3://my-bucket/data/*.parquet')
""")

# Read from local HTTP server
{:ok, result} = DuckdbEx.query(conn, """
  CREATE TABLE web_data AS
  SELECT * FROM read_json_auto('http://localhost:8080/api/data.json')
""")
```

### Parquet Support

```elixir
# Load parquet extension
{:ok, _} = DuckdbEx.query(conn, "INSTALL parquet")
{:ok, _} = DuckdbEx.query(conn, "LOAD parquet")

# Read Parquet files
{:ok, result} = DuckdbEx.query(conn, """
  SELECT * FROM parquet_scan('data/*.parquet')
  WHERE year = 2024
  LIMIT 100
""")

# Write to Parquet
{:ok, _} = DuckdbEx.query(conn, """
  COPY (SELECT * FROM my_table WHERE active = true)
  TO 'output.parquet' (FORMAT PARQUET)
""")

# Parquet metadata inspection
{:ok, result} = DuckdbEx.query(conn, """
  SELECT * FROM parquet_metadata('large_file.parquet')
""")

metadata = DuckdbEx.rows(result)
IO.puts("Parquet file metadata:")
Enum.each(metadata, fn row ->
  IO.puts("  #{inspect(row)}")
end)

# Column-specific reading for efficiency
{:ok, result} = DuckdbEx.query(conn, """
  SELECT column1, column3
  FROM parquet_scan('data.parquet', columns=['column1', 'column3'])
""")
```

### JSON Processing

```elixir
# Load json extension
{:ok, _} = DuckdbEx.query(conn, "INSTALL json")
{:ok, _} = DuckdbEx.query(conn, "LOAD json")

# Create table with JSON data
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE json_data (
    id INTEGER,
    metadata JSON,
    tags JSON
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO json_data VALUES
  (1, '{"user": "alice", "score": 95}', '["important", "verified"]'),
  (2, '{"user": "bob", "score": 87, "badges": ["newcomer"]}', '["active"]'),
  (3, '{"user": "charlie", "score": 92, "preferences": {"theme": "dark"}}', '["premium", "verified"]')
""")

# Query JSON data
{:ok, result} = DuckdbEx.query(conn, """
  SELECT
    id,
    json_extract(metadata, '$.user') as username,
    json_extract(metadata, '$.score') as score,
    json_array_length(tags) as tag_count,
    json_extract(metadata, '$.preferences.theme') as theme
  FROM json_data
""")

json_results = DuckdbEx.rows(result)
IO.puts("JSON query results:")
Enum.each(json_results, fn [id, username, score, tag_count, theme] ->
  IO.puts("  ID: #{id}, User: #{username}, Score: #{score}, Tags: #{tag_count}, Theme: #{theme || "default"}")
end)

# Read JSON files
{:ok, result} = DuckdbEx.query(conn, """
  SELECT * FROM read_json_auto('data.json')
""")

# Complex JSON transformations
{:ok, result} = DuckdbEx.query(conn, """
  SELECT
    json_extract(data, '$.items[*].name') as item_names,
    json_transform(data, '$.metadata', '{"processed": true}') as updated_metadata
  FROM json_table
""")
```

### Full-Text Search (fts)

```elixir
# Load FTS extension
{:ok, _} = DuckdbEx.query(conn, "INSTALL fts")
{:ok, _} = DuckdbEx.query(conn, "LOAD fts")

# Create table with text content
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE documents (
    id INTEGER PRIMARY KEY,
    title VARCHAR,
    content TEXT,
    author VARCHAR,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO documents (id, title, content, author) VALUES
  (1, 'Introduction to DuckDB', 'DuckDB is an analytical database that supports SQL...', 'Alice'),
  (2, 'Elixir Programming Guide', 'Elixir is a functional programming language built on Erlang...', 'Bob'),
  (3, 'Data Analysis with DuckDB', 'This guide covers advanced analytical queries using DuckDB...', 'Charlie'),
  (4, 'Functional Programming Concepts', 'Functional programming emphasizes immutability and pure functions...', 'Alice')
""")

# Create FTS index
{:ok, _} = DuckdbEx.query(conn, """
  PRAGMA create_fts_index('documents', 'id', 'title', 'content')
""")

# Perform full-text search
{:ok, result} = DuckdbEx.query(conn, """
  SELECT id, title, author, fts_main_documents.match_bm25(id, 'analytical database') as relevance
  FROM documents
  WHERE fts_main_documents.match_bm25(id, 'analytical database') IS NOT NULL
  ORDER BY relevance DESC
""")

search_results = DuckdbEx.rows(result)
IO.puts("Full-text search results for 'analytical database':")
Enum.each(search_results, fn [id, title, author, relevance] ->
  IO.puts("  #{title} by #{author} (relevance: #{relevance})")
end)
```

### Spatial Analysis (spatial)

```elixir
# Load spatial extension
{:ok, _} = DuckdbEx.query(conn, "INSTALL spatial")
{:ok, _} = DuckdbEx.query(conn, "LOAD spatial")

# Create table with spatial data
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE locations (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    point GEOMETRY,
    region GEOMETRY
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO locations VALUES
  (1, 'Downtown', ST_Point(-122.4194, 37.7749), ST_Buffer(ST_Point(-122.4194, 37.7749), 0.01)),
  (2, 'Airport', ST_Point(-122.3748, 37.6213), ST_Buffer(ST_Point(-122.3748, 37.6213), 0.01)),
  (3, 'Stadium', ST_Point(-122.3892, 37.7800), ST_Buffer(ST_Point(-122.3892, 37.7800), 0.01))
""")

# Spatial queries
{:ok, result} = DuckdbEx.query(conn, """
  SELECT
    id,
    name,
    ST_X(point) as longitude,
    ST_Y(point) as latitude,
    ST_Area(region) as area
  FROM locations
""")

# Distance calculations
{:ok, result} = DuckdbEx.query(conn, """
  SELECT
    l1.name as from_location,
    l2.name as to_location,
    ST_Distance(l1.point, l2.point) as distance
  FROM locations l1
  CROSS JOIN locations l2
  WHERE l1.id < l2.id
  ORDER BY distance
""")

distance_results = DuckdbEx.rows(result)
IO.puts("Distances between locations:")
Enum.each(distance_results, fn [from_loc, to_loc, distance] ->
  IO.puts("  #{from_loc} to #{to_loc}: #{Float.round(distance, 4)} units")
end)
```

## Custom Extension Management

### Extension Information and Management

```elixir
defmodule ExtensionManager do
  def list_all_extensions(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT
        extension_name,
        loaded,
        installed,
        description
      FROM duckdb_extensions()
      ORDER BY extension_name
    """)

    extensions = DuckdbEx.rows(result)

    IO.puts("Available DuckDB Extensions:")
    IO.puts("=" <> String.duplicate("=", 50))

    Enum.each(extensions, fn [name, loaded, installed, description] ->
      status = case {loaded, installed} do
        {true, true} -> "✅ Loaded"
        {false, true} -> "📦 Installed"
        {false, false} -> "❌ Not installed"
      end

      IO.puts("#{name} - #{status}")
      IO.puts("  #{description}")
      IO.puts("")
    end)
  end

  def install_extension_safely(conn, extension_name) do
    # Check if already installed
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT installed FROM duckdb_extensions()
      WHERE extension_name = '#{extension_name}'
    """)

    case DuckdbEx.rows(result) do
      [[true]] ->
        IO.puts("Extension #{extension_name} is already installed")
        :ok

      [[false]] ->
        IO.puts("Installing extension #{extension_name}...")
        case DuckdbEx.query(conn, "INSTALL #{extension_name}") do
          {:ok, _} ->
            IO.puts("Successfully installed #{extension_name}")
            :ok
          {:error, reason} ->
            IO.puts("Failed to install #{extension_name}: #{reason}")
            {:error, reason}
        end

      [] ->
        {:error, "Unknown extension: #{extension_name}"}
    end
  end

  def load_extension_safely(conn, extension_name) do
    # Ensure it's installed first
    case install_extension_safely(conn, extension_name) do
      :ok ->
        case DuckdbEx.query(conn, "LOAD #{extension_name}") do
          {:ok, _} ->
            IO.puts("Successfully loaded #{extension_name}")
            :ok
          {:error, reason} ->
            IO.puts("Failed to load #{extension_name}: #{reason}")
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  def setup_data_science_extensions(conn) do
    extensions = [
      "httpfs",     # External data sources
      "parquet",    # Columnar format
      "json",       # JSON processing
      "fts",        # Full-text search
      "spatial"     # GIS functions
    ]

    IO.puts("Setting up data science extension stack...")

    results = Enum.map(extensions, fn ext ->
      {ext, load_extension_safely(conn, ext)}
    end)

    # Report results
    {successful, failed} = Enum.split_with(results, fn {_, result} -> result == :ok end)

    IO.puts("Successfully loaded: #{length(successful)} extensions")
    IO.puts("Failed to load: #{length(failed)} extensions")

    if length(failed) > 0 do
      IO.puts("Failed extensions:")
      Enum.each(failed, fn {ext, {:error, reason}} ->
        IO.puts("  #{ext}: #{reason}")
      end)
    end

    {:ok, Enum.map(successful, fn {ext, _} -> ext end)}
  end
end

# Usage
ExtensionManager.list_all_extensions(conn)
{:ok, loaded_extensions} = ExtensionManager.setup_data_science_extensions(conn)
IO.puts("Data science stack ready with: #{Enum.join(loaded_extensions, ", ")}")
```

### Extension Configuration

```elixir
defmodule ExtensionConfigurator do
  def configure_httpfs_for_cloud(conn, provider, credentials) do
    case provider do
      :aws ->
        configure_aws_s3(conn, credentials)
      :gcp ->
        configure_gcp_gcs(conn, credentials)
      :azure ->
        configure_azure_blob(conn, credentials)
      _ ->
        {:error, "Unsupported cloud provider: #{provider}"}
    end
  end

  defp configure_aws_s3(conn, %{access_key: key, secret_key: secret, region: region}) do
    settings = [
      {"s3_region", region},
      {"s3_access_key_id", key},
      {"s3_secret_access_key", secret}
    ]

    apply_settings(conn, settings, "AWS S3")
  end

  defp configure_gcp_gcs(conn, %{service_account_key: key_path}) do
    settings = [
      {"gcs_service_account_key", key_path}
    ]

    apply_settings(conn, settings, "Google Cloud Storage")
  end

  defp configure_azure_blob(conn, %{account_name: account, account_key: key}) do
    settings = [
      {"azure_storage_account_name", account},
      {"azure_storage_account_key", key}
    ]

    apply_settings(conn, settings, "Azure Blob Storage")
  end

  defp apply_settings(conn, settings, provider_name) do
    IO.puts("Configuring #{provider_name}...")

    results = Enum.map(settings, fn {setting, value} ->
      case DuckdbEx.query(conn, "SET #{setting}='#{value}'") do
        {:ok, _} -> {:ok, setting}
        {:error, reason} -> {:error, {setting, reason}}
      end
    end)

    failed = Enum.filter(results, &match?({:error, _}, &1))

    if length(failed) == 0 do
      IO.puts("#{provider_name} configured successfully")
      :ok
    else
      IO.puts("Failed to configure some #{provider_name} settings:")
      Enum.each(failed, fn {:error, {setting, reason}} ->
        IO.puts("  #{setting}: #{reason}")
      end)
      {:error, "Configuration partially failed"}
    end
  end

  def configure_fts_analyzer(conn, language \\ "english") do
    case DuckdbEx.query(conn, "SET fts_analyzer='#{language}'") do
      {:ok, _} ->
        IO.puts("FTS analyzer set to #{language}")
        :ok
      {:error, reason} ->
        IO.puts("Failed to set FTS analyzer: #{reason}")
        {:error, reason}
    end
  end
end

# Example configurations
aws_credentials = %{
  access_key: "your_access_key",
  secret_key: "your_secret_key",
  region: "us-west-2"
}

ExtensionConfigurator.configure_httpfs_for_cloud(conn, :aws, aws_credentials)
ExtensionConfigurator.configure_fts_analyzer(conn, "english")
```

## Extension Use Cases

### ETL Pipeline with Extensions

```elixir
defmodule ETLPipeline do
  def run_data_pipeline(conn) do
    # Setup required extensions
    {:ok, _} = ExtensionManager.setup_data_science_extensions(conn)

    # Extract: Read from various sources
    extract_data(conn)

    # Transform: Process and clean data
    transform_data(conn)

    # Load: Write to final destination
    load_data(conn)
  end

  defp extract_data(conn) do
    IO.puts("Extracting data from multiple sources...")

    # Extract from CSV via HTTP
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE raw_sales AS
      SELECT * FROM read_csv_auto('https://example.com/sales.csv')
    """)

    # Extract from Parquet files
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE raw_products AS
      SELECT * FROM parquet_scan('s3://data-bucket/products/*.parquet')
    """)

    # Extract from JSON API
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE raw_customers AS
      SELECT * FROM read_json_auto('https://api.example.com/customers.json')
    """)

    IO.puts("Data extraction completed")
  end

  defp transform_data(conn) do
    IO.puts("Transforming and cleaning data...")

    # Clean and join data
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE clean_sales AS
      SELECT
        s.sale_id,
        s.customer_id,
        s.product_id,
        s.quantity,
        s.sale_date,
        p.product_name,
        p.category,
        c.customer_name,
        json_extract(c.metadata, '$.segment') as customer_segment
      FROM raw_sales s
      JOIN raw_products p ON s.product_id = p.product_id
      JOIN raw_customers c ON s.customer_id = c.customer_id
      WHERE s.quantity > 0 AND s.sale_date >= '2024-01-01'
    """)

    # Create aggregated views
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE sales_summary AS
      SELECT
        category,
        customer_segment,
        COUNT(*) as total_sales,
        SUM(quantity) as total_quantity,
        date_trunc('month', sale_date) as sale_month
      FROM clean_sales
      GROUP BY category, customer_segment, date_trunc('month', sale_date)
    """)

    IO.puts("Data transformation completed")
  end

  defp load_data(conn) do
    IO.puts("Loading data to final destinations...")

    # Export to Parquet for analytics
    {:ok, _} = DuckdbEx.query(conn, """
      COPY sales_summary TO 'output/sales_summary.parquet' (FORMAT PARQUET)
    """)

    # Export to JSON for web applications
    {:ok, _} = DuckdbEx.query(conn, """
      COPY (
        SELECT json_object(
          'category', category,
          'segment', customer_segment,
          'metrics', json_object(
            'sales_count', total_sales,
            'quantity', total_quantity
          )
        ) as data
        FROM sales_summary
      ) TO 'output/sales_api.json' (FORMAT JSON)
    """)

    IO.puts("Data loading completed")
  end
end

# Run the ETL pipeline
ETLPipeline.run_data_pipeline(conn)
```

### Real-time Data Analysis

```elixir
defmodule RealTimeAnalyzer do
  def setup_monitoring_dashboard(conn) do
    # Load required extensions
    {:ok, _} = ExtensionManager.load_extension_safely(conn, "httpfs")
    {:ok, _} = ExtensionManager.load_extension_safely(conn, "json")

    # Create real-time data ingestion
    create_streaming_tables(conn)

    # Setup analytical queries
    setup_analytical_views(conn)

    # Generate dashboard data
    generate_dashboard_data(conn)
  end

  defp create_streaming_tables(conn) do
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE IF NOT EXISTS events (
        timestamp TIMESTAMP,
        event_type VARCHAR,
        user_id VARCHAR,
        session_id VARCHAR,
        properties JSON,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    """)

    # Simulate real-time data ingestion
    simulate_events(conn, 1000)
  end

  defp simulate_events(conn, count) do
    IO.puts("Generating #{count} simulated events...")

    events = Enum.map(1..count, fn i ->
      timestamp = NaiveDateTime.add(NaiveDateTime.utc_now(), -:rand.uniform(3600), :second)
      event_type = Enum.random(["page_view", "click", "purchase", "signup"])
      user_id = "user_#{:rand.uniform(100)}"
      session_id = "session_#{:rand.uniform(50)}"

      properties = case event_type do
        "page_view" -> %{page: "/page_#{:rand.uniform(10)}", duration: :rand.uniform(300)}
        "click" -> %{element: "button_#{:rand.uniform(5)}", x: :rand.uniform(1920), y: :rand.uniform(1080)}
        "purchase" -> %{amount: :rand.uniform(1000), currency: "USD", items: :rand.uniform(5)}
        "signup" -> %{source: Enum.random(["organic", "paid", "social"]), campaign: "campaign_#{:rand.uniform(3)}"}
      end

      [timestamp, event_type, user_id, session_id, Jason.encode!(properties)]
    end)

    # Batch insert events
    {:ok, stmt} = DuckdbEx.PreparedStatement.prepare(conn, """
      INSERT INTO events (timestamp, event_type, user_id, session_id, properties)
      VALUES ($1, $2, $3, $4, $5)
    """)

    Enum.each(events, fn event ->
      {:ok, _} = DuckdbEx.PreparedStatement.execute(stmt, event)
    end)

    :ok = DuckdbEx.PreparedStatement.destroy(stmt)
  end

  defp setup_analytical_views(conn) do
    # Real-time metrics view
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE OR REPLACE VIEW real_time_metrics AS
      SELECT
        date_trunc('minute', timestamp) as minute,
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT session_id) as unique_sessions
      FROM events
      WHERE timestamp >= current_timestamp - INTERVAL '1 hour'
      GROUP BY date_trunc('minute', timestamp), event_type
      ORDER BY minute DESC, event_count DESC
    """)

    # User behavior analysis
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE OR REPLACE VIEW user_behavior AS
      SELECT
        user_id,
        COUNT(*) as total_events,
        COUNT(DISTINCT event_type) as event_types,
        MIN(timestamp) as first_seen,
        MAX(timestamp) as last_seen,
        json_extract(properties, '$.amount') as purchase_amount
      FROM events
      GROUP BY user_id, json_extract(properties, '$.amount')
      HAVING COUNT(*) > 5
      ORDER BY total_events DESC
    """)
  end

  defp generate_dashboard_data(conn) do
    IO.puts("Generating dashboard data...")

    # Get real-time metrics
    {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM real_time_metrics LIMIT 20")
    metrics = DuckdbEx.rows(result)

    IO.puts("Recent activity (last 20 entries):")
    Enum.each(metrics, fn [minute, event_type, count, users, sessions] ->
      IO.puts("  #{minute}: #{event_type} - #{count} events, #{users} users, #{sessions} sessions")
    end)

    # Get top users
    {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM user_behavior LIMIT 10")
    top_users = DuckdbEx.rows(result)

    IO.puts("\nTop active users:")
    Enum.each(top_users, fn [user_id, total_events, event_types, first_seen, last_seen, _purchase] ->
      IO.puts("  #{user_id}: #{total_events} events, #{event_types} types (#{first_seen} - #{last_seen})")
    end)
  end
end

# Setup real-time monitoring
RealTimeAnalyzer.setup_monitoring_dashboard(conn)
```

## Extension Development

### Creating Custom Functions (via SQL)

```elixir
defmodule CustomFunctions do
  def create_business_logic_functions(conn) do
    # Create custom scalar functions
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE OR REPLACE FUNCTION calculate_tax(amount DECIMAL, rate DECIMAL) AS (
        ROUND(amount * rate / 100, 2)
      )
    """)

    {:ok, _} = DuckdbEx.query(conn, """
      CREATE OR REPLACE FUNCTION format_currency(amount DECIMAL, currency VARCHAR DEFAULT 'USD') AS (
        CASE currency
          WHEN 'USD' THEN '$' || CAST(amount AS VARCHAR)
          WHEN 'EUR' THEN '€' || CAST(amount AS VARCHAR)
          ELSE CAST(amount AS VARCHAR) || ' ' || currency
        END
      )
    """)

    # Create macro for complex date calculations
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE OR REPLACE MACRO business_days_between(start_date, end_date) AS (
        CASE
          WHEN start_date > end_date THEN 0
          ELSE (
            (CAST(end_date AS DATE) - CAST(start_date AS DATE)) -
            (FLOOR((CAST(end_date AS DATE) - CAST(start_date AS DATE)) / 7) * 2) -
            CASE WHEN DAYOFWEEK(start_date) = 1 THEN 1 ELSE 0 END -
            CASE WHEN DAYOFWEEK(end_date) = 7 THEN 1 ELSE 0 END
          )
        END
      )
    """)

    IO.puts("Custom business functions created")
  end

  def test_custom_functions(conn) do
    # Test the custom functions
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT
        1000.00 as amount,
        calculate_tax(1000.00, 8.5) as tax,
        format_currency(1000.00, 'USD') as formatted_usd,
        format_currency(1000.00, 'EUR') as formatted_eur,
        business_days_between('2024-01-01', '2024-01-15') as business_days
    """)

    [[amount, tax, usd, eur, days]] = DuckdbEx.rows(result)

    IO.puts("Custom function test results:")
    IO.puts("  Amount: #{amount}")
    IO.puts("  Tax (8.5%): #{tax}")
    IO.puts("  USD Format: #{usd}")
    IO.puts("  EUR Format: #{eur}")
    IO.puts("  Business Days: #{days}")
  end
end

CustomFunctions.create_business_logic_functions(conn)
CustomFunctions.test_custom_functions(conn)
```

## Best Practices

1. **Load Extensions Early**: Load required extensions at database connection time
2. **Use Auto-loading**: Configure auto-loading for development environments
3. **Validate Availability**: Always check if extensions are available before using them
4. **Handle Errors Gracefully**: Extension installation can fail due to network or permission issues
5. **Version Compatibility**: Be aware that extensions may have version compatibility requirements
6. **Security Considerations**: Some extensions (like httpfs) can access external resources
7. **Performance Impact**: Loading extensions has minimal impact, but some features may affect query performance

## Troubleshooting Extensions

### Common Extension Issues

```elixir
defmodule ExtensionTroubleshooting do
  def diagnose_extension_issues(conn) do
    IO.puts("Diagnosing extension issues...")

    # Check DuckDB version
    {:ok, result} = DuckdbEx.query(conn, "SELECT version()")
    [[version]] = DuckdbEx.rows(result)
    IO.puts("DuckDB version: #{version}")

    # Check available extensions
    {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM duckdb_extensions()")
    [[ext_count]] = DuckdbEx.rows(result)
    IO.puts("Available extensions: #{ext_count}")

    # Check platform compatibility
    {:ok, result} = DuckdbEx.query(conn, "SELECT platform FROM duckdb_platform()")
    [[platform]] = DuckdbEx.rows(result)
    IO.puts("Platform: #{platform}")

    # Test network connectivity for extensions that need it
    test_network_connectivity(conn)
  end

  defp test_network_connectivity(conn) do
    case DuckdbEx.query(conn, "INSTALL httpfs") do
      {:ok, _} ->
        case DuckdbEx.query(conn, "LOAD httpfs") do
          {:ok, _} ->
            # Test basic HTTP connectivity
            case DuckdbEx.query(conn, "SELECT 1") do
              {:ok, _} -> IO.puts("Network extensions available")
              {:error, reason} -> IO.puts("Network test failed: #{reason}")
            end
          {:error, reason} ->
            IO.puts("Cannot load httpfs: #{reason}")
        end
      {:error, reason} ->
        IO.puts("Cannot install httpfs: #{reason}")
    end
  end
end

ExtensionTroubleshooting.diagnose_extension_issues(conn)
```

## Next Steps

- Learn about [Performance Optimization](performance.md) with extensions
- Explore [Examples](examples.md) for real-world extension usage
- See [Configuration](configuration.md) for extension-specific settings
