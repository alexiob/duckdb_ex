defmodule DirtyNifTest do
  use ExUnit.Case

  describe "dirty NIFs and scheduler behavior" do
    test "concurrent database operations don't block scheduler" do
      # Open shared database
      {:ok, db} = DuckdbEx.open()

      # Test concurrent database operations with separate connections
      tasks =
        for i <- 1..5 do
          Task.async(fn ->
            {:ok, conn} = DuckdbEx.connect(db)

            # Create table and insert data (using dirty NIFs)
            {:ok, _} =
              DuckdbEx.query(conn, "CREATE TABLE IF NOT EXISTS test_#{i} (id INT, value VARCHAR)")

            {:ok, stmt} = DuckdbEx.prepare(conn, "INSERT INTO test_#{i} VALUES (?, ?)")

            # Insert multiple rows
            for j <- 1..10 do
              {:ok, _} = DuckdbEx.execute(stmt, [j, "value_#{j}"])
            end

            {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM test_#{i}")
            rows = DuckdbEx.rows(result)
            {count} = hd(rows)

            DuckdbEx.destroy_result(result)
            DuckdbEx.destroy_prepared_statement(stmt)
            DuckdbEx.close_connection(conn)

            count
          end)
        end

      # All tasks should complete successfully
      results = Task.await_many(tasks, 30_000)

      # Verify all tasks inserted the expected number of rows
      assert length(results) == 5
      Enum.each(results, fn count -> assert count == 10 end)

      # Verify final state
      {:ok, conn_final} = DuckdbEx.connect(db)

      {:ok, result} =
        DuckdbEx.query(conn_final, """
          SELECT table_name FROM information_schema.tables
          WHERE table_name LIKE 'test_%'
          ORDER BY table_name
        """)

      tables = DuckdbEx.rows(result)
      assert length(tables) == 5

      DuckdbEx.destroy_result(result)
      DuckdbEx.close_connection(conn_final)
      DuckdbEx.close_database(db)
    end

    test "CPU-intensive work runs concurrently with database operations" do
      # This test demonstrates that database operations don't block the scheduler
      start_time = System.monotonic_time(:millisecond)

      # Start CPU-intensive tasks
      cpu_tasks =
        for _i <- 1..3 do
          Task.async(fn ->
            cpu_start = System.monotonic_time(:millisecond)

            # Do some CPU work
            for j <- 1..50_000 do
              _result = :math.sqrt(j) * :math.sin(j)
            end

            cpu_end = System.monotonic_time(:millisecond)
            cpu_end - cpu_start
          end)
        end

      # Start database tasks (these use dirty NIFs)
      db_tasks =
        for i <- 1..3 do
          Task.async(fn ->
            db_start = System.monotonic_time(:millisecond)

            {:ok, db} = DuckdbEx.open()
            {:ok, conn} = DuckdbEx.connect(db)

            {:ok, _} = DuckdbEx.query(conn, "CREATE TABLE load_test_#{i} (id INT, data VARCHAR)")

            {:ok, stmt} = DuckdbEx.prepare(conn, "INSERT INTO load_test_#{i} VALUES (?, ?)")

            # Insert many rows to create some load
            for j <- 1..500 do
              {:ok, _} = DuckdbEx.execute(stmt, [j, "data_#{j}"])
            end

            {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM load_test_#{i}")
            rows = DuckdbEx.rows(result)
            {count} = hd(rows)

            DuckdbEx.destroy_result(result)
            DuckdbEx.destroy_prepared_statement(stmt)
            DuckdbEx.close_connection(conn)
            DuckdbEx.close_database(db)

            db_end = System.monotonic_time(:millisecond)
            {count, db_end - db_start}
          end)
        end

      # Wait for all tasks
      cpu_results = Task.await_many(cpu_tasks, 30_000)
      db_results = Task.await_many(db_tasks, 30_000)

      end_time = System.monotonic_time(:millisecond)
      total_duration = end_time - start_time

      # Verify CPU tasks completed (they should be fast)
      assert length(cpu_results) == 3

      Enum.each(cpu_results, fn duration ->
        assert is_integer(duration)
        assert duration >= 0
      end)

      # Verify DB tasks completed successfully
      assert length(db_results) == 3

      Enum.each(db_results, fn {count, duration} ->
        assert count == 500
        assert is_integer(duration)
        assert duration >= 0
      end)

      # The total time should be reasonable (indicating concurrency)
      # If operations were blocking, this would take much longer
      # Should complete within 5 seconds
      assert total_duration < 5000
    end

    test "file database operations with dirty NIFs" do
      # Test that file operations use dirty NIFs properly

      # Multiple tasks working with file databases concurrently
      tasks =
        for i <- 1..3 do
          Task.async(fn ->
            task_path = "/tmp/test_dirty_#{i}_#{:rand.uniform(1_000_000)}.db"

            # File database operations should use dirty NIFs
            {:ok, db} = DuckdbEx.open(task_path)
            {:ok, conn} = DuckdbEx.connect(db)

            {:ok, _} = DuckdbEx.query(conn, "CREATE TABLE file_test (id INT, msg VARCHAR)")

            {:ok, stmt} = DuckdbEx.prepare(conn, "INSERT INTO file_test VALUES (?, ?)")
            {:ok, _} = DuckdbEx.execute(stmt, [i, "message_#{i}"])

            {:ok, result} = DuckdbEx.query(conn, "SELECT * FROM file_test")
            rows = DuckdbEx.rows(result)

            DuckdbEx.destroy_result(result)
            DuckdbEx.destroy_prepared_statement(stmt)
            DuckdbEx.close_connection(conn)
            DuckdbEx.close_database(db)

            # Verify file was created
            file_exists = File.exists?(task_path)

            # Clean up file
            File.rm(task_path)

            {length(rows), file_exists}
          end)
        end

      results = Task.await_many(tasks, 30_000)

      # All tasks should have inserted 1 row each and created files
      assert length(results) == 3

      Enum.each(results, fn {count, file_existed} ->
        assert count == 1
        assert file_existed == true
      end)
    end

    test "prepared statement parameter binding with dirty NIFs" do
      {:ok, db} = DuckdbEx.open()
      {:ok, conn} = DuckdbEx.connect(db)

      {:ok, _} =
        DuckdbEx.query(conn, """
          CREATE TABLE param_test (
            id INTEGER,
            name VARCHAR,
            value DOUBLE,
            active BOOLEAN,
            note VARCHAR
          )
        """)

      # Test concurrent parameter binding (using dirty NIFs)
      tasks =
        for i <- 1..10 do
          Task.async(fn ->
            {:ok, stmt} = DuckdbEx.prepare(conn, "INSERT INTO param_test VALUES (?, ?, ?, ?, ?)")

            {:ok, _} =
              DuckdbEx.execute(stmt, [
                i,
                "name_#{i}",
                i * 10.5,
                rem(i, 2) == 0,
                "note for #{i}"
              ])

            DuckdbEx.destroy_prepared_statement(stmt)
            i
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert length(results) == 10
      assert Enum.sort(results) == Enum.to_list(1..10)

      # Verify all data was inserted correctly
      {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM param_test")
      rows = DuckdbEx.rows(result)
      {count} = hd(rows)
      assert count == 10

      DuckdbEx.destroy_result(result)
      DuckdbEx.close_connection(conn)
      DuckdbEx.close_database(db)
    end
  end
end
