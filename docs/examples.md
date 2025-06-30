# Examples and Use Cases

This guide provides real-world examples and common use cases for DuckdbEx, demonstrating how to solve practical problems with different APIs and patterns. Each example includes detailed explanations of the techniques used and can be adapted for your specific use cases.

## Table of Contents

- [Examples and Use Cases](#examples-and-use-cases)
  - [Table of Contents](#table-of-contents)
  - [Data Analysis Examples](#data-analysis-examples)
    - [Sales Data Analysis](#sales-data-analysis)
    - [Time Series Analysis](#time-series-analysis)
  - [Vector Similarity Search (VSS)](#vector-similarity-search-vss)
    - [Document Semantic Search](#document-semantic-search)
  - [ETL Operations](#etl-operations)
    - [CSV Data Pipeline](#csv-data-pipeline)
    - [Database Migration](#database-migration)
  - [Real-time Data Processing](#real-time-data-processing)
    - [Stream Processing](#stream-processing)
  - [Data Migration](#data-migration)
  - [Reporting and Analytics](#reporting-and-analytics)
  - [Web Application Integration](#web-application-integration)
    - [Phoenix LiveView Dashboard](#phoenix-liveview-dashboard)
  - [Testing and Development](#testing-and-development)
    - [Test Data Generation](#test-data-generation)
  - [Summary](#summary)
    - [Key DuckDB Features Demonstrated](#key-duckdb-features-demonstrated)
    - [Adaptation Guidelines](#adaptation-guidelines)

## Data Analysis Examples

DuckDB excels at analytical workloads with its columnar storage, vectorized execution, and advanced SQL features. These examples demonstrate how to leverage DuckDB's analytical capabilities for business intelligence, reporting, and data exploration tasks.

### Sales Data Analysis

This example shows how to build a comprehensive sales analytics system using DuckDB's advanced SQL features including window functions, CTEs (Common Table Expressions), and time-based aggregations. Perfect for e-commerce platforms, retail businesses, or any system that tracks transactions over time.

```elixir
defmodule SalesAnalyzer do
  @moduledoc """
  Comprehensive sales analytics using DuckDB's advanced SQL features.

  This module demonstrates:
  - Time-based aggregations using DATE_TRUNC and EXTRACT functions
  - Window functions for calculating moving averages and trends
  - Common Table Expressions (CTEs) for complex analytical queries
  - Efficient bulk data loading using the Appender API
  - Statistical analysis for business intelligence reporting

  Perfect for e-commerce platforms, retail analytics, and revenue reporting systems.
  """

  def setup_sales_data(conn) do
    # Create sales table
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE sales (
        id INTEGER PRIMARY KEY,
        product_id INTEGER,
        customer_id INTEGER,
        sale_date DATE,
        amount DECIMAL(10,2),
        quantity INTEGER,
        region VARCHAR(50),
        sales_rep VARCHAR(100)
      )
    """)

    # Load sample data
    load_sample_sales_data(conn)
  end

  def monthly_sales_report(conn, year \\ 2023) do
    sql = """
    SELECT
      DATE_TRUNC('month', sale_date) as month,
      region,
      COUNT(*) as total_transactions,
      SUM(amount) as total_revenue,
      AVG(amount) as avg_transaction_value,
      SUM(quantity) as total_units_sold,
      COUNT(DISTINCT customer_id) as unique_customers
    FROM sales
    WHERE EXTRACT(year FROM sale_date) = ?
    GROUP BY DATE_TRUNC('month', sale_date), region
    ORDER BY month DESC, region
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [year])
    format_sales_report(result.rows)
  end

  def top_performing_products(conn, limit \\ 10) do
    sql = """
    WITH product_performance AS (
      SELECT
        product_id,
        SUM(amount) as total_revenue,
        SUM(quantity) as total_units,
        COUNT(*) as transaction_count,
        AVG(amount) as avg_sale_value,
        COUNT(DISTINCT customer_id) as unique_customers
      FROM sales
      WHERE sale_date >= CURRENT_DATE - INTERVAL '90 days'
      GROUP BY product_id
    )
    SELECT
      product_id,
      total_revenue,
      total_units,
      transaction_count,
      ROUND(avg_sale_value, 2) as avg_sale_value,
      unique_customers,
      ROUND(total_revenue / total_units, 2) as revenue_per_unit
    FROM product_performance
    ORDER BY total_revenue DESC
    LIMIT ?
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [limit])
    result.rows
  end

  def sales_trend_analysis(conn, product_id) do
    sql = """
    SELECT
      DATE_TRUNC('week', sale_date) as week,
      SUM(amount) as weekly_revenue,
      SUM(quantity) as weekly_units,
      AVG(amount) as avg_transaction,
      -- Calculate 4-week moving average
      AVG(SUM(amount)) OVER (
        ORDER BY DATE_TRUNC('week', sale_date)
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ) as moving_avg_revenue
    FROM sales
    WHERE product_id = ?
      AND sale_date >= CURRENT_DATE - INTERVAL '6 months'
    GROUP BY DATE_TRUNC('week', sale_date)
    ORDER BY week
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [product_id])
    result.rows
  end

  defp load_sample_sales_data(conn) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "sales")

    try do
      # Generate sample data
      sample_data = generate_sample_sales_data(1000)

      Enum.each(sample_data, fn {id, product_id, customer_id, date, amount, quantity, region, rep} ->
        :ok = DuckdbEx.Appender.append_int32(appender, id)
        :ok = DuckdbEx.Appender.append_int32(appender, product_id)
        :ok = DuckdbEx.Appender.append_int32(appender, customer_id)
        :ok = DuckdbEx.Appender.append_date(appender, date)
        :ok = DuckdbEx.Appender.append_varchar(appender, Decimal.to_string(amount))
        :ok = DuckdbEx.Appender.append_int32(appender, quantity)
        :ok = DuckdbEx.Appender.append_varchar(appender, region)
        :ok = DuckdbEx.Appender.append_varchar(appender, rep)
        :ok = DuckdbEx.Appender.end_row(appender)
      end)

      DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp generate_sample_sales_data(count) do
    regions = ["North", "South", "East", "West", "Central"]
    reps = ["Alice Smith", "Bob Johnson", "Carol Davis", "David Wilson", "Eve Brown"]

    for id <- 1..count do
      {
        id,
        :rand.uniform(100),                           # product_id
        :rand.uniform(500),                           # customer_id
        Date.add(~D[2023-01-01], :rand.uniform(365)), # sale_date
        Decimal.new(:rand.uniform(1000)),             # amount
        :rand.uniform(10),                            # quantity
        Enum.random(regions),                         # region
        Enum.random(reps)                             # sales_rep
      }
    end
  end

  defp format_sales_report(rows) do
    Enum.map(rows, fn [month, region, transactions, revenue, avg_value, units, customers] ->
      %{
        month: month,
        region: region,
        transactions: transactions,
        revenue: revenue,
        avg_transaction_value: Float.round(avg_value, 2),
        units_sold: units,
        unique_customers: customers
      }
    end)
  end
end
```

### Time Series Analysis

This example demonstrates DuckDB's powerful time-series capabilities for IoT data analysis. It showcases statistical functions, window operations, anomaly detection using z-scores, and rolling averages - essential techniques for monitoring systems, sensor networks, and performance analytics.

```elixir
defmodule TimeSeriesAnalyzer do
  @moduledoc """
  Analyze time series data with DuckDB's time functions.
  This module demonstrates statistical analysis, anomaly detection,
  and time-based aggregations commonly used in IoT and monitoring systems.
  """

  def setup_sensor_data(conn) do
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE sensor_readings (
        sensor_id VARCHAR(50),
        timestamp TIMESTAMP,
        temperature DOUBLE,
        humidity DOUBLE,
        pressure DOUBLE,
        location VARCHAR(100)
      )
    """)

    load_sample_sensor_data(conn)
  end

  def hourly_aggregates(conn, sensor_id, start_time, end_time) do
    sql = """
    SELECT
      DATE_TRUNC('hour', timestamp) as hour,
      AVG(temperature) as avg_temp,
      MIN(temperature) as min_temp,
      MAX(temperature) as max_temp,
      AVG(humidity) as avg_humidity,
      AVG(pressure) as avg_pressure,
      COUNT(*) as reading_count
    FROM sensor_readings
    WHERE sensor_id = ?
      AND timestamp BETWEEN ? AND ?
    GROUP BY DATE_TRUNC('hour', timestamp)
    ORDER BY hour
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [sensor_id, start_time, end_time])
    result.rows
  end

  def detect_anomalies(conn, sensor_id) do
    sql = """
    WITH stats AS (
      SELECT
        AVG(temperature) as mean_temp,
        STDDEV(temperature) as stddev_temp
      FROM sensor_readings
      WHERE sensor_id = ?
        AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '7 days'
    ),
    readings_with_zscore AS (
      SELECT
        timestamp,
        temperature,
        humidity,
        pressure,
        ABS(temperature - stats.mean_temp) / stats.stddev_temp as z_score
      FROM sensor_readings, stats
      WHERE sensor_id = ?
        AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    )
    SELECT
      timestamp,
      temperature,
      humidity,
      pressure,
      ROUND(z_score, 3) as z_score
    FROM readings_with_zscore
    WHERE z_score > 2.0  -- Anomalies beyond 2 standard deviations
    ORDER BY timestamp DESC
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [sensor_id, sensor_id])
    result.rows
  end

  def rolling_averages(conn, sensor_id, window_minutes \\ 60) do
    sql = """
    SELECT
      timestamp,
      temperature,
      AVG(temperature) OVER (
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '#{window_minutes} minutes' PRECEDING
        AND CURRENT ROW
      ) as rolling_avg_temp,
      AVG(humidity) OVER (
        ORDER BY timestamp
        RANGE BETWEEN INTERVAL '#{window_minutes} minutes' PRECEDING
        AND CURRENT ROW
      ) as rolling_avg_humidity
    FROM sensor_readings
    WHERE sensor_id = ?
      AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    ORDER BY timestamp
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [sensor_id])
    result.rows
  end

  defp load_sample_sensor_data(conn) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "sensor_readings")

    try do
      # Generate 24 hours of sample data (every 5 minutes)
      base_time = NaiveDateTime.utc_now() |> NaiveDateTime.add(-24 * 3600)

      for minutes <- 0..(24*60) do
        if rem(minutes, 5) == 0 do  # Every 5 minutes
          timestamp = NaiveDateTime.add(base_time, minutes * 60)

          # Simulate sensor readings with some noise
          temp = 20.0 + :math.sin(minutes / 120.0) * 5.0 + (:rand.uniform() - 0.5) * 2.0
          humidity = 50.0 + :math.cos(minutes / 180.0) * 20.0 + (:rand.uniform() - 0.5) * 5.0
          pressure = 1013.25 + (:rand.uniform() - 0.5) * 10.0

          :ok = DuckdbEx.Appender.append_varchar(appender, "SENSOR_001")
          :ok = DuckdbEx.Appender.append_timestamp(appender, timestamp)
          :ok = DuckdbEx.Appender.append_double(appender, temp)
          :ok = DuckdbEx.Appender.append_double(appender, humidity)
          :ok = DuckdbEx.Appender.append_double(appender, pressure)
          :ok = DuckdbEx.Appender.append_varchar(appender, "Data Center A")
          :ok = DuckdbEx.Appender.end_row(appender)
        end
      end

      DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end
end
```

## Vector Similarity Search (VSS)

DuckDB's VSS extension enables semantic search, recommendation systems, and AI-powered applications by storing and querying high-dimensional vector embeddings. This example shows how to build a document search system using embeddings from language models like OpenAI's text-embedding models.

**Extension Management**: This example demonstrates proper extension usage with `DuckdbEx.Extension.install/2` and `DuckdbEx.Extension.load/2` functions, which provide better error handling and integration than raw SQL commands.

### Document Semantic Search

```elixir
defmodule SemanticSearch do
  @moduledoc """
  Semantic document search using DuckDB's VSS extension.
  This example demonstrates how to store document embeddings and perform
  similarity searches for AI-powered search applications, recommendation
  systems, and retrieval-augmented generation (RAG) systems.
  """

  @embedding_dimension 1536  # OpenAI text-embedding-ada-002 dimension

  def setup_vector_search(conn) do
    # Install and load the VSS extension using DuckdbEx.Extension
    {:ok, _} = DuckdbEx.Extension.install(conn, "vss")
    {:ok, _} = DuckdbEx.Extension.load(conn, "vss")

    # Create documents table with metadata
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE documents (
        id INTEGER PRIMARY KEY,
        title VARCHAR,
        content TEXT,
        url VARCHAR,
        category VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        metadata JSON
      )
    """)

    # Create embeddings table with vector similarity search index
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE document_embeddings (
        document_id INTEGER,
        embedding FLOAT[#{@embedding_dimension}],
        PRIMARY KEY (document_id),
        FOREIGN KEY (document_id) REFERENCES documents(id)
      )
    """)

    # Create HNSW index for fast similarity search
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE INDEX embeddings_idx ON document_embeddings
      USING HNSW (embedding)
      WITH (metric = 'cosine')
    """)

    load_sample_documents(conn)
  end

  def add_document(conn, title, content, url, category, metadata \\ %{}) do
    # Insert document
    {:ok, result} = DuckdbEx.query(conn, """
      INSERT INTO documents (title, content, url, category, metadata)
      VALUES (?, ?, ?, ?, ?)
      RETURNING id
    """, [title, content, url, category, Jason.encode!(metadata)])

    document_id = result.rows |> List.first() |> List.first()

    # Generate embedding (in real application, call OpenAI API)
    embedding = generate_mock_embedding(content)

    # Store embedding
    {:ok, _} = DuckdbEx.query(conn, """
      INSERT INTO document_embeddings (document_id, embedding)
      VALUES (?, ?)
    """, [document_id, embedding])

    {:ok, document_id}
  end

  def search_documents(conn, query, limit \\ 10, similarity_threshold \\ 0.7) do
    # Generate query embedding (in real application, call OpenAI API)
    query_embedding = generate_mock_embedding(query)

    sql = """
    SELECT
      d.id,
      d.title,
      d.content,
      d.url,
      d.category,
      d.metadata,
      -- Calculate cosine similarity
      array_cosine_similarity(e.embedding, ?::FLOAT[#{@embedding_dimension}]) as similarity_score
    FROM documents d
    JOIN document_embeddings e ON d.id = e.document_id
    WHERE array_cosine_similarity(e.embedding, ?::FLOAT[#{@embedding_dimension}]) > ?
    ORDER BY similarity_score DESC
    LIMIT ?
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [query_embedding, query_embedding, similarity_threshold, limit])

    format_search_results(result.rows)
  end

  def find_similar_documents(conn, document_id, limit \\ 5) do
    sql = """
    WITH target_embedding AS (
      SELECT embedding FROM document_embeddings WHERE document_id = ?
    )
    SELECT
      d.id,
      d.title,
      d.content,
      d.category,
      array_cosine_similarity(e.embedding, t.embedding) as similarity_score
    FROM documents d
    JOIN document_embeddings e ON d.id = e.document_id
    CROSS JOIN target_embedding t
    WHERE d.id != ?  -- Exclude the target document itself
    ORDER BY similarity_score DESC
    LIMIT ?
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [document_id, document_id, limit])
    format_search_results(result.rows)
  end

  def search_by_category_with_semantic_boost(conn, query, category, limit \\ 10) do
    query_embedding = generate_mock_embedding(query)

    sql = """
    SELECT
      d.id,
      d.title,
      d.content,
      d.url,
      d.category,
      d.metadata,
      -- Combine semantic similarity with category matching
      array_cosine_similarity(e.embedding, ?::FLOAT[#{@embedding_dimension}]) *
      CASE WHEN d.category = ? THEN 1.2 ELSE 1.0 END as boosted_score,
      array_cosine_similarity(e.embedding, ?::FLOAT[#{@embedding_dimension}]) as raw_similarity
    FROM documents d
    JOIN document_embeddings e ON d.id = e.document_id
    WHERE array_cosine_similarity(e.embedding, ?::FLOAT[#{@embedding_dimension}]) > 0.5
    ORDER BY boosted_score DESC
    LIMIT ?
    """

    {:ok, result} = DuckdbEx.query(conn, sql, [
      query_embedding, category, query_embedding, query_embedding, limit
    ])

    format_search_results_with_boost(result.rows)
  end

  def get_embedding_statistics(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT
        COUNT(*) as total_embeddings,
        COUNT(DISTINCT d.category) as unique_categories,
        AVG(array_cosine_similarity(e1.embedding, e2.embedding)) as avg_pairwise_similarity
      FROM document_embeddings e1
      CROSS JOIN document_embeddings e2
      JOIN documents d ON e1.document_id = d.id
      WHERE e1.document_id < e2.document_id  -- Avoid duplicate pairs
    """)

    case result.rows do
      [[total, categories, avg_sim]] ->
        %{
          total_embeddings: total,
          unique_categories: categories,
          average_pairwise_similarity: Float.round(avg_sim || 0.0, 4)
        }
      _ -> %{total_embeddings: 0, unique_categories: 0, average_pairwise_similarity: 0.0}
    end
  end

  def cluster_documents_by_similarity(conn, num_clusters \\ 5) do
    # Use k-means clustering on embeddings to group similar documents
    {:ok, result} = DuckdbEx.query(conn, """
    WITH clustered AS (
      SELECT
        document_id,
        -- Simplified clustering using modulo (in practice, use proper k-means)
        (document_id % ?) as cluster_id
      FROM document_embeddings
    )
    SELECT
      c.cluster_id,
      COUNT(*) as document_count,
      STRING_AGG(d.title, ', ') as sample_titles
    FROM clustered c
    JOIN documents d ON c.document_id = d.id
    GROUP BY c.cluster_id
    ORDER BY c.cluster_id
    """, [num_clusters])

    result.rows
  end

  defp load_sample_documents(conn) do
    sample_docs = [
      {"Machine Learning Basics", "Introduction to supervised and unsupervised learning algorithms", "/docs/ml-basics", "AI", %{difficulty: "beginner"}},
      {"Deep Learning with Neural Networks", "Understanding backpropagation and gradient descent", "/docs/deep-learning", "AI", %{difficulty: "advanced"}},
      {"Data Science Pipeline", "From data collection to model deployment", "/docs/data-pipeline", "Data Science", %{difficulty: "intermediate"}},
      {"Web Development with Elixir", "Building scalable web applications with Phoenix", "/docs/elixir-web", "Programming", %{difficulty: "intermediate"}},
      {"Database Optimization Techniques", "Indexing strategies and query optimization", "/docs/db-optimization", "Database", %{difficulty: "advanced"}},
      {"API Design Best Practices", "RESTful API design and documentation", "/docs/api-design", "Programming", %{difficulty: "beginner"}},
      {"Time Series Forecasting", "ARIMA models and seasonal decomposition", "/docs/time-series", "Data Science", %{difficulty: "advanced"}},
      {"Functional Programming Concepts", "Immutability, higher-order functions, and monads", "/docs/functional-programming", "Programming", %{difficulty: "intermediate"}},
      {"Computer Vision Applications", "Image processing and object detection", "/docs/computer-vision", "AI", %{difficulty: "advanced"}},
      {"Microservices Architecture", "Service decomposition and communication patterns", "/docs/microservices", "Architecture", %{difficulty: "intermediate"}}
    ]

    Enum.each(sample_docs, fn {title, content, url, category, metadata} ->
      {:ok, _} = add_document(conn, title, content, url, category, metadata)
    end)
  end

  defp generate_mock_embedding(text) do
    # In a real application, you would call OpenAI's embedding API:
    # response = OpenAI.embeddings(%{model: "text-embedding-ada-002", input: text})
    # response.data |> List.first() |> Map.get(:embedding)

    # For this example, generate a mock embedding based on text content
    :crypto.hash(:sha256, text)
    |> :binary.bin_to_list()
    |> Enum.take(@embedding_dimension)
    |> Enum.map(fn byte -> (byte - 128) / 128.0 end)  # Normalize to [-1, 1]
  end

  defp format_search_results(rows) do
    Enum.map(rows, fn row ->
      case row do
        [id, title, content, url, category, metadata, similarity] ->
          %{
            id: id,
            title: title,
            content: String.slice(content, 0, 200) <> "...",
            url: url,
            category: category,
            metadata: Jason.decode!(metadata || "{}"),
            similarity_score: Float.round(similarity, 4)
          }
        [id, title, content, category, similarity] ->
          %{
            id: id,
            title: title,
            content: String.slice(content, 0, 200) <> "...",
            category: category,
            similarity_score: Float.round(similarity, 4)
          }
      end
    end)
  end

  defp format_search_results_with_boost(rows) do
    Enum.map(rows, fn [id, title, content, url, category, metadata, boosted_score, raw_similarity] ->
      %{
        id: id,
        title: title,
        content: String.slice(content, 0, 200) <> "...",
        url: url,
        category: category,
        metadata: Jason.decode!(metadata || "{}"),
        boosted_score: Float.round(boosted_score, 4),
        raw_similarity: Float.round(raw_similarity, 4)
      }
    end)
  end
end

# Usage Example
defmodule SemanticSearchExample do
  def run_example do
    {:ok, conn} = DuckdbEx.open(":memory:")

    try do
      # Setup vector search
      SemanticSearch.setup_vector_search(conn)

      # Search for AI-related documents
      ai_docs = SemanticSearch.search_documents(conn, "artificial intelligence machine learning", 5)
      IO.puts("AI-related documents:")
      Enum.each(ai_docs, fn doc ->
        IO.puts("  #{doc.title} (similarity: #{doc.similarity_score})")
      end)

      # Find similar documents to a specific one
      similar_docs = SemanticSearch.find_similar_documents(conn, 1, 3)
      IO.puts("\nDocuments similar to first document:")
      Enum.each(similar_docs, fn doc ->
        IO.puts("  #{doc.title} (similarity: #{doc.similarity_score})")
      end)

      # Category-boosted search
      boosted_results = SemanticSearch.search_by_category_with_semantic_boost(
        conn, "programming", "Programming", 5
      )
      IO.puts("\nProgramming documents with category boost:")
      Enum.each(boosted_results, fn doc ->
        IO.puts("  #{doc.title} (boosted: #{doc.boosted_score}, raw: #{doc.raw_similarity})")
      end)

      # Get statistics
      stats = SemanticSearch.get_embedding_statistics(conn)
      IO.puts("\nEmbedding Statistics:")
      IO.inspect(stats)

    after
      DuckdbEx.close(conn)
    end
  end
end
```

## ETL Operations

ETL (Extract, Transform, Load) operations are critical for data integration and processing workflows. DuckDB's excellent CSV support, SQL capabilities, and extension system make it ideal for building efficient data pipelines that can handle large datasets with minimal memory usage.

### CSV Data Pipeline

This example demonstrates a complete ETL pipeline that processes CSV files with data validation, transformation, and quality checking. It showcases DuckDB's fast CSV loading capabilities and SQL-based data cleaning techniques.

```elixir
defmodule CSVPipeline do
  @moduledoc """
  Complete ETL pipeline for processing CSV files with DuckDB.

  This module demonstrates:
  - Fast CSV loading using DuckDB's optimized COPY command
  - Data validation and quality checks during transformation
  - Type casting and data cleaning operations
  - Computed column generation and business rule application
  - Efficient CSV export with proper formatting

  Ideal for data import/export operations, data cleaning workflows,
  and integration between different systems via CSV interchange.
  """

  def process_csv_file(input_path, output_path) do
    {:ok, conn} = DuckdbEx.open(":memory:")

    try do
      # Extract: Load CSV data
      load_csv_data(conn, input_path)

      # Transform: Clean and process data
      transform_data(conn)

      # Load: Export processed data
      export_data(conn, output_path)

      {:ok, :completed}
    after
      DuckdbEx.close(conn)
    end
  end

  defp load_csv_data(conn, csv_path) do
    # Create staging table
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE raw_data (
        id VARCHAR,
        name VARCHAR,
        email VARCHAR,
        age VARCHAR,
        salary VARCHAR,
        department VARCHAR,
        join_date VARCHAR
      )
    """)

    # Load CSV using COPY command (fastest method)
    {:ok, _} = DuckdbEx.query(conn, """
      COPY raw_data FROM '#{csv_path}' (
        FORMAT CSV,
        HEADER true,
        DELIMITER ',',
        QUOTE '"',
        ESCAPE '"'
      )
    """)
  end

  defp transform_data(conn) do
    # Create clean table with proper types
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE clean_data AS
      SELECT
        CAST(id AS INTEGER) as id,
        TRIM(UPPER(name)) as name,
        LOWER(TRIM(email)) as email,
        CAST(age AS INTEGER) as age,
        CAST(REPLACE(salary, '$', '') AS DECIMAL(10,2)) as salary,
        TRIM(department) as department,
        CAST(join_date AS DATE) as join_date,
        -- Add computed columns
        CASE
          WHEN age < 30 THEN 'Young Professional'
          WHEN age < 50 THEN 'Mid Career'
          ELSE 'Senior Professional'
        END as career_stage,
        -- Calculate years of service
        DATE_DIFF('year', CAST(join_date AS DATE), CURRENT_DATE) as years_of_service
      FROM raw_data
      WHERE
        -- Data quality filters
        id IS NOT NULL
        AND name IS NOT NULL
        AND email LIKE '%@%'
        AND age BETWEEN 18 AND 100
        AND salary > 0
    """)

    # Add data quality summary
    {:ok, quality_result} = DuckdbEx.query(conn, """
      SELECT
        COUNT(*) as total_records,
        COUNT(CASE WHEN email LIKE '%@%' THEN 1 END) as valid_emails,
        COUNT(CASE WHEN age BETWEEN 18 AND 100 THEN 1 END) as valid_ages,
        AVG(salary) as avg_salary,
        MIN(join_date) as earliest_join_date,
        MAX(join_date) as latest_join_date
      FROM clean_data
    """)

    IO.puts("Data quality summary: #{inspect(quality_result.rows)}")
  end

  defp export_data(conn, output_path) do
    # Export clean data to new CSV
    {:ok, _} = DuckdbEx.query(conn, """
      COPY clean_data TO '#{output_path}' (
        FORMAT CSV,
        HEADER true,
        DELIMITER ','
      )
    """)
  end
end
```

### Database Migration

This example shows how to use DuckDB as an intermediate layer for migrating data between different database systems. DuckDB's extension system allows it to connect to multiple databases simultaneously, making it an excellent tool for ETL operations and database migrations. This pattern is useful for moving from legacy systems, consolidating databases, or changing database technologies.

**Extension Management**: The example uses `DuckdbEx.Extension` module for proper extension installation and loading, providing better error handling and integration with the DuckdbEx ecosystem.

```elixir
defmodule DatabaseMigration do
  @moduledoc """
  Migrate data between databases using DuckDB as an intermediate layer.
  Demonstrates how to leverage DuckDB's extensions to connect to multiple
  database systems and perform complex data transformations during migration.
  """

  def migrate_postgres_to_sqlite(postgres_config, sqlite_path) do
    {:ok, duckdb_conn} = DuckdbEx.open(":memory:")

    try do
      # Install and load PostgreSQL extension using DuckdbEx.Extension
      {:ok, _} = DuckdbEx.Extension.install(duckdb_conn, "postgres_scanner")
      {:ok, _} = DuckdbEx.Extension.load(duckdb_conn, "postgres_scanner")

      # Connect to PostgreSQL
      attach_postgres(duckdb_conn, postgres_config)

      # Extract data from PostgreSQL
      extract_tables_from_postgres(duckdb_conn)

      # Transform data if needed
      transform_for_sqlite(duckdb_conn)

      # Load into SQLite
      export_to_sqlite(duckdb_conn, sqlite_path)

      {:ok, :migration_completed}
    after
      DuckdbEx.close(duckdb_conn)
    end
  end

  defp attach_postgres(conn, %{host: host, port: port, database: database, user: user, password: password}) do
    {:ok, _} = DuckdbEx.query(conn, """
      ATTACH 'host=#{host} port=#{port} dbname=#{database} user=#{user} password=#{password}'
      AS postgres_db (TYPE postgres_scanner)
    """)
  end

  defp extract_tables_from_postgres(conn) do
    # Get list of tables to migrate
    {:ok, tables_result} = DuckdbEx.query(conn, """
      SELECT table_name
      FROM postgres_db.information_schema.tables
      WHERE table_schema = 'public'
        AND table_type = 'BASE TABLE'
    """)

    tables = Enum.map(tables_result.rows, fn [table_name] -> table_name end)

    # Copy each table
    Enum.each(tables, fn table_name ->
      IO.puts("Migrating table: #{table_name}")

      {:ok, _} = DuckdbEx.query(conn, """
        CREATE TABLE #{table_name} AS
        SELECT * FROM postgres_db.public.#{table_name}
      """)
    end)
  end

  defp transform_for_sqlite(conn) do
    # Example transformations for SQLite compatibility

    # Convert JSONB to TEXT
    {:ok, jsonb_columns} = DuckdbEx.query(conn, """
      SELECT table_name, column_name
      FROM information_schema.columns
      WHERE data_type = 'JSONB'
    """)

    Enum.each(jsonb_columns.rows, fn [table, column] ->
      {:ok, _} = DuckdbEx.query(conn, """
        ALTER TABLE #{table}
        ALTER COLUMN #{column} TYPE VARCHAR USING #{column}::VARCHAR
      """)
    end)

    # Handle PostgreSQL-specific types
    convert_postgres_types(conn)
  end

  defp convert_postgres_types(conn) do
    # Convert UUID to VARCHAR
    # Convert arrays to JSON strings
    # Convert timestamps with timezone

    {:ok, _} = DuckdbEx.query(conn, """
      -- Example: Convert UUID columns
      UPDATE users SET id = CAST(id AS VARCHAR) WHERE typeof(id) = 'UUID'
    """)
  end

  defp export_to_sqlite(conn, sqlite_path) do
    # Install and load SQLite extension using DuckdbEx.Extension
    {:ok, _} = DuckdbEx.Extension.install(conn, "sqlite_scanner")
    {:ok, _} = DuckdbEx.Extension.load(conn, "sqlite_scanner")

    # Attach SQLite database
    {:ok, _} = DuckdbEx.query(conn, """
      ATTACH '#{sqlite_path}' AS sqlite_db (TYPE sqlite)
    """)

    # Get all tables to export
    {:ok, tables_result} = DuckdbEx.query(conn, """
      SELECT table_name FROM information_schema.tables
      WHERE table_schema = 'main'
    """)

    # Export each table
    Enum.each(tables_result.rows, fn [table_name] ->
      {:ok, _} = DuckdbEx.query(conn, """
        CREATE TABLE sqlite_db.#{table_name} AS
        SELECT * FROM #{table_name}
      """)
    end)
  end
end
```

## Real-time Data Processing

While DuckDB is primarily designed for analytical workloads, it can effectively handle real-time data processing scenarios through buffering, batch processing, and incremental aggregation patterns. This approach is ideal for analytics dashboards, monitoring systems, and applications that need to process high-volume event streams.

### Stream Processing

This example demonstrates how to build a real-time event processing system using GenServer for state management and DuckDB for efficient data aggregation. The pattern includes event buffering, periodic aggregation, and automatic data cleanup - essential for building scalable analytics systems.

```elixir
defmodule StreamProcessor do
  use GenServer

  @moduledoc """
  Process real-time data streams using DuckDB for analytics.
  """

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def process_event(event) do
    GenServer.cast(__MODULE__, {:process_event, event})
  end

  def get_recent_stats(window_minutes \\ 5) do
    GenServer.call(__MODULE__, {:get_stats, window_minutes})
  end

  def init(opts) do
    database_path = Keyword.get(opts, :database_path, ":memory:")
    {:ok, conn} = DuckdbEx.open(database_path)

    setup_tables(conn)

    # Schedule periodic aggregation
    Process.send_after(self(), :aggregate_data, 60_000)  # Every minute

    {:ok, %{conn: conn, event_buffer: []}}
  end

  def handle_cast({:process_event, event}, state) do
    # Buffer events for batch processing
    new_buffer = [event | state.event_buffer]

    # Process buffer when it reaches threshold
    if length(new_buffer) >= 100 do
      process_event_buffer(state.conn, new_buffer)
      {:noreply, %{state | event_buffer: []}}
    else
      {:noreply, %{state | event_buffer: new_buffer}}
    end
  end

  def handle_call({:get_stats, window_minutes}, _from, state) do
    stats = get_window_statistics(state.conn, window_minutes)
    {:reply, stats, state}
  end

  def handle_info(:aggregate_data, state) do
    # Process any remaining events
    if length(state.event_buffer) > 0 do
      process_event_buffer(state.conn, state.event_buffer)
    end

    # Perform aggregations
    create_minute_aggregates(state.conn)
    cleanup_old_data(state.conn)

    # Schedule next aggregation
    Process.send_after(self(), :aggregate_data, 60_000)

    {:noreply, %{state | event_buffer: []}}
  end

  defp setup_tables(conn) do
    # Raw events table
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE IF NOT EXISTS events (
        id VARCHAR PRIMARY KEY,
        event_type VARCHAR,
        user_id VARCHAR,
        timestamp TIMESTAMP,
        properties JSON,
        processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      )
    """)

    # Aggregated metrics table
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE IF NOT EXISTS minute_aggregates (
        minute_timestamp TIMESTAMP,
        event_type VARCHAR,
        event_count INTEGER,
        unique_users INTEGER,
        avg_events_per_user DOUBLE,
        PRIMARY KEY (minute_timestamp, event_type)
      )
    """)
  end

  defp process_event_buffer(conn, events) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "events")

    try do
      Enum.each(events, fn %{id: id, event_type: type, user_id: user_id, timestamp: timestamp, properties: props} ->
        :ok = DuckdbEx.Appender.append_varchar(appender, id)
        :ok = DuckdbEx.Appender.append_varchar(appender, type)
        :ok = DuckdbEx.Appender.append_varchar(appender, user_id)
        :ok = DuckdbEx.Appender.append_timestamp(appender, timestamp)
        :ok = DuckdbEx.Appender.append_varchar(appender, Jason.encode!(props))
        :ok = DuckdbEx.Appender.append_timestamp(appender, NaiveDateTime.utc_now())
        :ok = DuckdbEx.Appender.end_row(appender)
      end)

      DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp create_minute_aggregates(conn) do
    {:ok, _} = DuckdbEx.query(conn, """
      INSERT OR REPLACE INTO minute_aggregates
      SELECT
        DATE_TRUNC('minute', timestamp) as minute_timestamp,
        event_type,
        COUNT(*) as event_count,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(*) / COUNT(DISTINCT user_id) as avg_events_per_user
      FROM events
      WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '2 minutes'
        AND processed_at >= CURRENT_TIMESTAMP - INTERVAL '2 minutes'
      GROUP BY DATE_TRUNC('minute', timestamp), event_type
    """)
  end

  defp get_window_statistics(conn, window_minutes) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT
        event_type,
        SUM(event_count) as total_events,
        SUM(unique_users) as total_unique_users,
        AVG(avg_events_per_user) as avg_events_per_user,
        COUNT(*) as minutes_with_data
      FROM minute_aggregates
      WHERE minute_timestamp >= CURRENT_TIMESTAMP - INTERVAL '#{window_minutes} minutes'
      GROUP BY event_type
      ORDER BY total_events DESC
    """)

    result.rows
  end

  defp cleanup_old_data(conn) do
    # Keep only last 24 hours of raw events
    {:ok, _} = DuckdbEx.query(conn, """
      DELETE FROM events
      WHERE timestamp < CURRENT_TIMESTAMP - INTERVAL '24 hours'
    """)

    # Keep aggregates for 7 days
    {:ok, _} = DuckdbEx.query(conn, """
      DELETE FROM minute_aggregates
      WHERE minute_timestamp < CURRENT_TIMESTAMP - INTERVAL '7 days'
    """)
  end
end

# Usage example
defmodule EventGenerator do
  def simulate_events do
    for _ <- 1..1000 do
      event = %{
        id: UUID.uuid4(),
        event_type: Enum.random(["page_view", "click", "purchase", "signup"]),
        user_id: "user_#{:rand.uniform(100)}",
        timestamp: NaiveDateTime.utc_now(),
        properties: %{
          page: "/page_#{:rand.uniform(10)}",
          source: Enum.random(["web", "mobile", "tablet"])
        }
      }

      StreamProcessor.process_event(event)
      Process.sleep(10)  # Simulate real-time flow
    end
  end
end
```

## Data Migration

See the Database Migration example in the ETL Operations section above for comprehensive data migration patterns.

## Reporting and Analytics

See the Sales Data Analysis and Time Series Analysis examples in the Data Analysis Examples section above for comprehensive reporting and analytics patterns.

## Web Application Integration

DuckDB's fast query performance and ability to handle analytical workloads make it excellent for powering real-time dashboards and analytics interfaces in web applications. This section shows how to integrate DuckDB with Phoenix LiveView to create responsive, data-driven user interfaces.

### Phoenix LiveView Dashboard

This example demonstrates building a real-time analytics dashboard that updates automatically as new data arrives. It combines Phoenix LiveView's real-time capabilities with DuckDB's analytical power to create responsive dashboards for business intelligence, monitoring, and reporting applications.

```elixir
defmodule MyAppWeb.AnalyticsLive do
  use MyAppWeb, :live_view

  @moduledoc """
  Real-time analytics dashboard using DuckDB and Phoenix LiveView.
  """

  def mount(_params, _session, socket) do
    if connected?(socket) do
      # Subscribe to real-time updates
      Phoenix.PubSub.subscribe(MyApp.PubSub, "analytics_updates")

      # Schedule periodic updates
      Process.send_after(self(), :update_metrics, 1000)
    end

    {:ok, conn} = DuckdbEx.open(Application.get_env(:my_app, :analytics_db_path))

    socket =
      socket
      |> assign(:conn, conn)
      |> assign(:metrics, %{})
      |> assign(:loading, true)
      |> load_initial_data()

    {:ok, socket}
  end

  def handle_info(:update_metrics, socket) do
    socket = update_dashboard_data(socket)
    Process.send_after(self(), :update_metrics, 5000)  # Update every 5 seconds
    {:noreply, socket}
  end

  def handle_info({:new_analytics_data, _data}, socket) do
    {:noreply, update_dashboard_data(socket)}
  end

  def render(assigns) do
    ~H"""
    <div class="analytics-dashboard">
      <.header>Analytics Dashboard</.header>

      <div class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
        <.metric_card
          title="Total Users"
          value={@metrics.total_users}
          change={@metrics.user_growth}
          loading={@loading} />

        <.metric_card
          title="Page Views (24h)"
          value={@metrics.page_views_24h}
          change={@metrics.page_view_growth}
          loading={@loading} />

        <.metric_card
          title="Conversion Rate"
          value={"#{@metrics.conversion_rate}%"}
          change={@metrics.conversion_change}
          loading={@loading} />
      </div>

      <div class="grid grid-cols-1 lg:grid-cols-2 gap-6">
        <.chart_card title="Traffic Over Time" data={@metrics.traffic_chart} />
        <.chart_card title="Top Pages" data={@metrics.top_pages} />
      </div>
    </div>
    """
  end

  defp load_initial_data(socket) do
    conn = socket.assigns.conn

    metrics = %{
      total_users: get_total_users(conn),
      page_views_24h: get_page_views_24h(conn),
      conversion_rate: get_conversion_rate(conn),
      user_growth: get_user_growth(conn),
      page_view_growth: get_page_view_growth(conn),
      conversion_change: get_conversion_change(conn),
      traffic_chart: get_traffic_chart_data(conn),
      top_pages: get_top_pages(conn)
    }

    assign(socket, metrics: metrics, loading: false)
  end

  defp update_dashboard_data(socket) do
    # Only update specific metrics to avoid full reload
    conn = socket.assigns.conn

    updated_metrics = %{
      socket.assigns.metrics |
      page_views_24h: get_page_views_24h(conn),
      traffic_chart: get_traffic_chart_data(conn)
    }

    assign(socket, metrics: updated_metrics)
  end

  defp get_total_users(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT COUNT(DISTINCT user_id) as total_users
      FROM user_events
    """)

    case result.rows do
      [[count]] -> count
      _ -> 0
    end
  end

  defp get_page_views_24h(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT COUNT(*) as page_views
      FROM user_events
      WHERE event_type = 'page_view'
        AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
    """)

    case result.rows do
      [[count]] -> count
      _ -> 0
    end
  end

  defp get_conversion_rate(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      WITH conversions AS (
        SELECT
          COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) as converters,
          COUNT(DISTINCT user_id) as total_users
        FROM user_events
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '30 days'
      )
      SELECT ROUND((converters * 100.0 / total_users), 2) as conversion_rate
      FROM conversions
    """)

    case result.rows do
      [[rate]] -> rate || 0
      _ -> 0
    end
  end

  defp get_traffic_chart_data(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT
        DATE_TRUNC('hour', timestamp) as hour,
        COUNT(*) as events
      FROM user_events
      WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
      GROUP BY DATE_TRUNC('hour', timestamp)
      ORDER BY hour
    """)

    Enum.map(result.rows, fn [hour, events] ->
      %{timestamp: hour, value: events}
    end)
  end

  defp get_top_pages(conn) do
    {:ok, result} = DuckdbEx.query(conn, """
      SELECT
        JSON_EXTRACT(properties, '$.page') as page,
        COUNT(*) as views
      FROM user_events
      WHERE event_type = 'page_view'
        AND timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
      GROUP BY JSON_EXTRACT(properties, '$.page')
      ORDER BY views DESC
      LIMIT 10
    """)

    Enum.map(result.rows, fn [page, views] ->
      %{page: page, views: views}
    end)
  end

  # Additional helper functions for growth calculations...
  defp get_user_growth(conn), do: calculate_growth(conn, "user_events", "COUNT(DISTINCT user_id)")
  defp get_page_view_growth(conn), do: calculate_growth(conn, "user_events", "COUNT(*)")
  defp get_conversion_change(conn), do: 0  # Implement conversion rate change calculation

  defp calculate_growth(conn, table, metric) do
    {:ok, result} = DuckdbEx.query(conn, """
      WITH periods AS (
        SELECT
          #{metric} as current_period
        FROM #{table}
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
      ),
      previous AS (
        SELECT
          #{metric} as previous_period
        FROM #{table}
        WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '48 hours'
          AND timestamp < CURRENT_TIMESTAMP - INTERVAL '24 hours'
      )
      SELECT
        ROUND(((current_period - previous_period) * 100.0 / previous_period), 1) as growth_rate
      FROM periods, previous
    """)

    case result.rows do
      [[growth]] -> growth || 0
      _ -> 0
    end
  end
end
```

## Testing and Development

Generating realistic test data is crucial for development, testing, and performance optimization. DuckDB's fast data loading capabilities and statistical functions make it ideal for creating comprehensive test datasets that mirror production data patterns without exposing sensitive information.

### Test Data Generation

This example shows how to generate large volumes of realistic test data with proper statistical distributions, relationships, and time-based patterns. It's particularly useful for load testing, development environments, and creating demo datasets for analytics applications.

```elixir
defmodule TestDataGenerator do
  @moduledoc """
  Generate realistic test data for development and testing.
  """

  def generate_user_analytics_data(conn, num_users \\ 1000, days \\ 30) do
    setup_analytics_tables(conn)

    # Generate users
    users = generate_users(num_users)
    load_users(conn, users)

    # Generate events
    events = generate_user_events(users, days)
    load_events(conn, events)

    IO.puts("Generated #{num_users} users and #{length(events)} events over #{days} days")
  end

  defp setup_analytics_tables(conn) do
    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE IF NOT EXISTS users (
        user_id VARCHAR PRIMARY KEY,
        email VARCHAR,
        signup_date DATE,
        user_type VARCHAR,
        country VARCHAR,
        age_group VARCHAR
      )
    """)

    {:ok, _} = DuckdbEx.query(conn, """
      CREATE TABLE IF NOT EXISTS user_events (
        event_id VARCHAR PRIMARY KEY,
        user_id VARCHAR,
        event_type VARCHAR,
        timestamp TIMESTAMP,
        properties JSON,
        session_id VARCHAR
      )
    """)
  end

  defp generate_users(count) do
    countries = ["US", "UK", "DE", "FR", "CA", "AU", "JP", "BR"]
    user_types = ["free", "premium", "enterprise"]
    age_groups = ["18-24", "25-34", "35-44", "45-54", "55+"]

    for i <- 1..count do
      signup_date = Date.add(Date.utc_today(), -:rand.uniform(365))

      %{
        user_id: "user_#{i}",
        email: "user#{i}@example.com",
        signup_date: signup_date,
        user_type: Enum.random(user_types),
        country: Enum.random(countries),
        age_group: Enum.random(age_groups)
      }
    end
  end

  defp load_users(conn, users) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "users")

    try do
      Enum.each(users, fn user ->
        :ok = DuckdbEx.Appender.append_varchar(appender, user.user_id)
        :ok = DuckdbEx.Appender.append_varchar(appender, user.email)
        :ok = DuckdbEx.Appender.append_date(appender, user.signup_date)
        :ok = DuckdbEx.Appender.append_varchar(appender, user.user_type)
        :ok = DuckdbEx.Appender.append_varchar(appender, user.country)
        :ok = DuckdbEx.Appender.append_varchar(appender, user.age_group)
        :ok = DuckdbEx.Appender.end_row(appender)
      end)

      DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp generate_user_events(users, days) do
    event_types = [
      {"page_view", 0.7},
      {"click", 0.2},
      {"purchase", 0.05},
      {"signup", 0.05}
    ]

    pages = ["/home", "/products", "/about", "/contact", "/pricing", "/features"]

    events = for user <- users,
                 day <- 0..days,
                 _event <- 1..:rand.uniform(10) do  # 1-10 events per user per day

      event_type = weighted_random(event_types)
      timestamp = random_timestamp_for_day(day)

      properties = case event_type do
        "page_view" -> %{page: Enum.random(pages), referrer: "google.com"}
        "click" -> %{element: "button_#{:rand.uniform(5)}", page: Enum.random(pages)}
        "purchase" -> %{amount: :rand.uniform(1000), product_id: "prod_#{:rand.uniform(50)}"}
        "signup" -> %{plan: Enum.random(["free", "pro", "enterprise"])}
      end

      %{
        event_id: UUID.uuid4(),
        user_id: user.user_id,
        event_type: event_type,
        timestamp: timestamp,
        properties: properties,
        session_id: "session_#{:rand.uniform(1000)}"
      }
    end

    # Remove nil events and shuffle
    events |> Enum.filter(&(&1 != nil)) |> Enum.shuffle()
  end

  defp load_events(conn, events) do
    {:ok, appender} = DuckdbEx.Appender.create(conn, nil, "user_events")

    try do
      Enum.each(events, fn event ->
        :ok = DuckdbEx.Appender.append_varchar(appender, event.event_id)
        :ok = DuckdbEx.Appender.append_varchar(appender, event.user_id)
        :ok = DuckdbEx.Appender.append_varchar(appender, event.event_type)
        :ok = DuckdbEx.Appender.append_timestamp(appender, event.timestamp)
        :ok = DuckdbEx.Appender.append_varchar(appender, Jason.encode!(event.properties))
        :ok = DuckdbEx.Appender.append_varchar(appender, event.session_id)
        :ok = DuckdbEx.Appender.end_row(appender)
      end)

      DuckdbEx.Appender.close(appender)
    after
      DuckdbEx.Appender.destroy(appender)
    end
  end

  defp weighted_random(weighted_list) do
    total_weight = Enum.sum(Enum.map(weighted_list, fn {_item, weight} -> weight end))
    random_value = :rand.uniform() * total_weight

    {item, _} = Enum.reduce_while(weighted_list, {nil, 0}, fn {item, weight}, {_, acc} ->
      new_acc = acc + weight
      if random_value <= new_acc do
        {:halt, {item, new_acc}}
      else
        {:cont, {item, new_acc}}
      end
    end)

    item
  end

  defp random_timestamp_for_day(days_ago) do
    base_date = NaiveDateTime.utc_now() |> NaiveDateTime.add(-days_ago * 24 * 3600)
    random_seconds = :rand.uniform(24 * 3600)  # Random time within the day
    NaiveDateTime.add(base_date, random_seconds)
  end
end
```

---

## Summary

These examples demonstrate the versatility and power of DuckdbEx across various domains and use cases:

- **Data Analysis**: Leverage DuckDB's analytical SQL capabilities for business intelligence and reporting
- **Vector Search**: Build AI-powered applications with semantic search and similarity matching
- **ETL Operations**: Create efficient data pipelines with validation, transformation, and quality checking
- **Real-time Processing**: Handle streaming data with buffering and incremental aggregation patterns
- **Web Integration**: Power responsive dashboards and analytics interfaces in Phoenix applications
- **Database Migration**: Use DuckDB as an intermediary for complex database migrations and transformations
- **Testing**: Generate realistic test datasets for development and performance testing

Each example includes:

- **Detailed explanations** of the techniques and patterns used
- **Production-ready code** following Elixir best practices
- **Performance considerations** and optimization techniques
- **Error handling** and data validation strategies
- **Extension integration** showcasing DuckDB's ecosystem

### Key DuckDB Features Demonstrated

- **Fast CSV Processing**: Optimized data loading and export capabilities
- **Advanced SQL**: Window functions, CTEs, time-series operations, and statistical functions
- **Extension System**: PostgreSQL scanner, SQLite integration, and VSS for vector operations using `DuckdbEx.Extension` module
- **Appender API**: Efficient bulk data insertion for high-performance scenarios
- **In-Memory Processing**: Fast analytical queries without persistence overhead
- **ACID Transactions**: Data consistency and reliability for production systems

### Adaptation Guidelines

When adapting these examples for your use cases:

1. **Replace mock data generation** with your actual data sources
2. **Customize SQL queries** to match your specific business logic and requirements
3. **Add appropriate error handling** for your production environment
4. **Consider performance implications** for your expected data volumes
5. **Implement proper security measures** for database connections and sensitive data
6. **Add comprehensive logging** for monitoring and debugging
7. **Write tests** to validate functionality and performance characteristics
8. **Use DuckdbEx.Extension module** for extension management instead of raw SQL commands

These patterns can be combined and extended to build sophisticated data processing applications that leverage DuckDB's unique capabilities for analytical workloads.
