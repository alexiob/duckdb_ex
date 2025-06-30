# Examples and Use Cases

This guide provides real-world examples and common use cases for DuckdbEx, demonstrating how to solve practical problems with different APIs and patterns.

## Table of Contents

- [Data Analysis Examples](#data-analysis-examples)
- [ETL Operations](#etl-operations)
- [Real-time Data Processing](#real-time-data-processing)
- [Web Application Integration](#web-application-integration)
- [Data Migration](#data-migration)
- [Reporting and Analytics](#reporting-and-analytics)
- [Testing and Development](#testing-and-development)

## Data Analysis Examples

### Sales Data Analysis

```elixir
defmodule SalesAnalyzer do
  @moduledoc """
  Analyze sales data using DuckDB's analytical capabilities.
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

```elixir
defmodule TimeSeriesAnalyzer do
  @moduledoc """
  Analyze time series data with DuckDB's time functions.
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

## ETL Operations

### CSV Data Pipeline

```elixir
defmodule CSVPipeline do
  @moduledoc """
  ETL pipeline for processing CSV files with DuckDB.
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

```elixir
defmodule DatabaseMigration do
  @moduledoc """
  Migrate data between databases using DuckDB as an intermediate layer.
  """

  def migrate_postgres_to_sqlite(postgres_config, sqlite_path) do
    {:ok, duckdb_conn} = DuckdbEx.open(":memory:")

    try do
      # Load PostgreSQL extension
      {:ok, _} = DuckdbEx.query(duckdb_conn, "INSTALL postgres_scanner")
      {:ok, _} = DuckdbEx.query(duckdb_conn, "LOAD postgres_scanner")

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
    # Install SQLite extension
    {:ok, _} = DuckdbEx.query(conn, "INSTALL sqlite_scanner")
    {:ok, _} = DuckdbEx.query(conn, "LOAD sqlite_scanner")

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

### Stream Processing

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

### Phoenix LiveView Dashboard

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

### Test Data Generation

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

These examples demonstrate real-world usage patterns for DuckdbEx across various domains. Each example can be adapted and extended based on your specific requirements and use cases.
