# Transactions

DuckDB provides ACID-compliant transaction support with basic transaction control operations. DuckdbEx exposes these capabilities through the `DuckdbEx.Transaction` module.

## Important Limitations

DuckDB operates with ACID compliance but uses a simpler transaction model focused on analytical workloads.

## Basic Transaction Operations

### Manual Transaction Control

```elixir
alias DuckdbEx.Transaction

{:ok, db} = DuckdbEx.open()
{:ok, conn} = DuckdbEx.connect(db)

# Create test table
{:ok, _} = DuckdbEx.query(conn, """
  CREATE TABLE accounts (
    id INTEGER PRIMARY KEY,
    name VARCHAR,
    balance DECIMAL(10,2)
  )
""")

{:ok, _} = DuckdbEx.query(conn, """
  INSERT INTO accounts VALUES
  (1, 'Alice', 1000.00),
  (2, 'Bob', 500.00)
""")

# Begin transaction
:ok = Transaction.begin(conn)

try do
  # Perform operations within transaction
  {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance - 100 WHERE id = 1")
  {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance + 100 WHERE id = 2")

  # Verify changes
  {:ok, result} = DuckdbEx.query(conn, "SELECT name, balance FROM accounts ORDER BY id")
  rows = DuckdbEx.rows(result)
  IO.puts("Balances in transaction: #{inspect(rows)}")

  # Commit transaction
  :ok = Transaction.commit(conn)
  IO.puts("Transaction committed successfully")

rescue
  exception ->
    # Rollback on error
    :ok = Transaction.rollback(conn)
    IO.puts("Transaction rolled back due to error: #{Exception.message(exception)}")
end
```

### Transaction Rollback

```elixir
# Begin transaction
:ok = Transaction.begin(conn)

# Perform some operations
{:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance - 200 WHERE id = 1")

# Check intermediate state
{:ok, result} = DuckdbEx.query(conn, "SELECT balance FROM accounts WHERE id = 1")
[[balance_in_transaction]] = DuckdbEx.rows(result)
IO.puts("Balance during transaction: #{balance_in_transaction}")

# Decide to rollback
:ok = Transaction.rollback(conn)

# Check final state
{:ok, result} = DuckdbEx.query(conn, "SELECT balance FROM accounts WHERE id = 1")
[[final_balance]] = DuckdbEx.rows(result)
IO.puts("Balance after rollback: #{final_balance}")
```

## Transaction Helper Function

### Using `with_transaction/2`

The `with_transaction/2` function provides a convenient way to execute code within a transaction with automatic cleanup:

```elixir
# Simple successful transaction
result = Transaction.with_transaction(conn, fn ->
  {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance - 50 WHERE id = 1")
  {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance + 50 WHERE id = 2")
  {:ok, "Transfer completed"}
end)

case result do
  {:ok, message} -> IO.puts("Success: #{message}")
  {:error, reason} -> IO.puts("Failed: #{reason}")
end
```

### Error Handling in Transactions

```elixir
# Transaction that will fail and rollback
result = Transaction.with_transaction(conn, fn ->
  {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance - 1500 WHERE id = 1")

  # Check if balance would go negative
  {:ok, result} = DuckdbEx.query(conn, "SELECT balance FROM accounts WHERE id = 1")
  [[new_balance]] = DuckdbEx.rows(result)

  if Decimal.lt?(new_balance, Decimal.new(0)) do
    {:error, "Insufficient funds"}
  else
    {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance + 1500 WHERE id = 2")
    {:ok, "Large transfer completed"}
  end
end)

case result do
  {:ok, message} ->
    IO.puts("Transaction succeeded: #{message}")
  {:error, reason} ->
    IO.puts("Transaction failed: #{reason}")
    # Check that balances remain unchanged
    {:ok, result} = DuckdbEx.query(conn, "SELECT name, balance FROM accounts ORDER BY id")
    IO.puts("Current balances: #{inspect(DuckdbEx.rows(result))}")
end
```

### Exception Handling

```elixir
# Transaction with exception handling
result = Transaction.with_transaction(conn, fn ->
  {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance - 100 WHERE id = 1")

  # This will cause an exception
  raise "Simulated error during transaction"

  # This code will never be reached
  {:ok, _} = DuckdbEx.query(conn, "UPDATE accounts SET balance = balance + 100 WHERE id = 2")
  {:ok, "Should not reach here"}
end)

case result do
  {:ok, message} ->
    IO.puts("Unexpected success: #{message}")
  {:error, reason} ->
    IO.puts("Transaction failed as expected: #{reason}")
    # Verify rollback occurred
    {:ok, result} = DuckdbEx.query(conn, "SELECT balance FROM accounts WHERE id = 1")
    [[balance]] = DuckdbEx.rows(result)
    IO.puts("Alice's balance after exception: #{balance}")
end
```

## Advanced Transaction Patterns

### Conditional Transactions

```elixir
defmodule BankingOperations do
  alias DuckdbEx.Transaction

  def transfer_funds(conn, from_id, to_id, amount) do
    Transaction.with_transaction(conn, fn ->
      # Check source account balance
      {:ok, result} = DuckdbEx.query(conn,
        "SELECT balance FROM accounts WHERE id = #{from_id}")

      case DuckdbEx.rows(result) do
        [[current_balance]] ->
          if Decimal.gte?(current_balance, amount) do
            # Sufficient funds, proceed with transfer
            {:ok, _} = DuckdbEx.query(conn,
              "UPDATE accounts SET balance = balance - #{amount} WHERE id = #{from_id}")
            {:ok, _} = DuckdbEx.query(conn,
              "UPDATE accounts SET balance = balance + #{amount} WHERE id = #{to_id}")

            {:ok, "Transferred #{amount} from account #{from_id} to #{to_id}"}
          else
            {:error, "Insufficient funds: has #{current_balance}, needs #{amount}"}
          end

        [] ->
          {:error, "Source account #{from_id} not found"}
      end
    end)
  end

  def batch_transfer(conn, transfers) do
    Transaction.with_transaction(conn, fn ->
      results = Enum.map(transfers, fn {from_id, to_id, amount} ->
        case transfer_funds_internal(conn, from_id, to_id, amount) do
          :ok -> {:ok, {from_id, to_id, amount}}
          {:error, reason} -> {:error, {from_id, to_id, amount, reason}}
        end
      end)

      failed_transfers = Enum.filter(results, &match?({:error, _}, &1))

      if length(failed_transfers) > 0 do
        {:error, "Some transfers failed: #{inspect(failed_transfers)}"}
      else
        {:ok, "All #{length(transfers)} transfers completed"}
      end
    end)
  end

  defp transfer_funds_internal(conn, from_id, to_id, amount) do
    # Internal transfer logic without transaction wrapper
    {:ok, result} = DuckdbEx.query(conn,
      "SELECT balance FROM accounts WHERE id = #{from_id}")

    case DuckdbEx.rows(result) do
      [[current_balance]] when current_balance >= amount ->
        {:ok, _} = DuckdbEx.query(conn,
          "UPDATE accounts SET balance = balance - #{amount} WHERE id = #{from_id}")
        {:ok, _} = DuckdbEx.query(conn,
          "UPDATE accounts SET balance = balance + #{amount} WHERE id = #{to_id}")
        :ok

      [[current_balance]] ->
        {:error, "insufficient_funds"}

      [] ->
        {:error, "account_not_found"}
    end
  end
end

# Example usage
transfers = [
  {1, 2, Decimal.new("50.00")},
  {2, 1, Decimal.new("25.00")},
  {1, 2, Decimal.new("10.00")}
]

case BankingOperations.batch_transfer(conn, transfers) do
  {:ok, message} -> IO.puts("Batch transfer success: #{message}")
  {:error, reason} -> IO.puts("Batch transfer failed: #{reason}")
end
```

### Transaction with Bulk Operations

```elixir
defmodule BulkTransactionOperations do
  alias DuckdbEx.{Transaction, Appender}

  def bulk_insert_with_transaction(conn, table_name, data) do
    Transaction.with_transaction(conn, fn ->
      # Create appender for bulk insert
      {:ok, appender} = Appender.create(conn, nil, table_name)

      try do
        # Insert all data
        :ok = Appender.append_rows(appender, data)
        :ok = Appender.close(appender)

        # Verify insert count
        {:ok, result} = DuckdbEx.query(conn, "SELECT COUNT(*) FROM #{table_name}")
        [[count]] = DuckdbEx.rows(result)

        {:ok, "Inserted #{length(data)} rows, total count: #{count}"}

      after
        # Always clean up appender
        Appender.destroy(appender)
      end
    end)
  end

  def conditional_bulk_update(conn, updates, condition) do
    Transaction.with_transaction(conn, fn ->
      # Count affected rows before update
      {:ok, result} = DuckdbEx.query(conn,
        "SELECT COUNT(*) FROM accounts WHERE #{condition}")
      [[before_count]] = DuckdbEx.rows(result)

      if before_count > 0 do
        # Perform bulk updates
        Enum.each(updates, fn update_sql ->
          {:ok, _} = DuckdbEx.query(conn, update_sql)
        end)

        # Verify changes
        {:ok, result} = DuckdbEx.query(conn,
          "SELECT COUNT(*) FROM accounts WHERE #{condition}")
        [[after_count]] = DuckdbEx.rows(result)

        {:ok, "Updated #{before_count} accounts, #{after_count} match condition after update"}
      else
        {:error, "No accounts match condition: #{condition}"}
      end
    end)
  end
end

# Example: Bulk insert new accounts
new_accounts = [
  [3, "Charlie", Decimal.new("750.00")],
  [4, "Diana", Decimal.new("1200.00")],
  [5, "Eve", Decimal.new("300.00")]
]

case BulkTransactionOperations.bulk_insert_with_transaction(conn, "accounts", new_accounts) do
  {:ok, message} -> IO.puts("Bulk insert success: #{message}")
  {:error, reason} -> IO.puts("Bulk insert failed: #{reason}")
end

# Example: Conditional bulk update
updates = [
  "UPDATE accounts SET balance = balance * 1.05 WHERE balance > 500",
  "UPDATE accounts SET name = UPPER(name) WHERE balance > 1000"
]

case BulkTransactionOperations.conditional_bulk_update(conn, updates, "balance > 500") do
  {:ok, message} -> IO.puts("Bulk update success: #{message}")
  {:error, reason} -> IO.puts("Bulk update failed: #{reason}")
end
```

## Transaction State Management

### Checking Transaction Status

```elixir
defmodule TransactionStatus do
  def get_transaction_info(conn) do
    # DuckDB doesn't provide a direct way to check transaction status
    # but we can use a pattern to track it

    try do
      # Try to start a transaction - will fail if already in one
      case DuckdbEx.Transaction.begin(conn) do
        :ok ->
          # We successfully started a transaction, so we weren't in one
          :ok = DuckdbEx.Transaction.rollback(conn)
          {:ok, :not_in_transaction}

        {:error, reason} ->
          # Likely already in a transaction
          {:ok, :in_transaction, reason}
      end
    rescue
      exception ->
        {:error, "Could not determine transaction status: #{Exception.message(exception)}"}
    end
  end

  def ensure_transaction(conn, fun) do
    case get_transaction_info(conn) do
      {:ok, :not_in_transaction} ->
        # Start a new transaction
        DuckdbEx.Transaction.with_transaction(conn, fun)

      {:ok, :in_transaction, _} ->
        # Already in transaction, just execute the function
        fun.()

      {:error, reason} ->
        {:error, "Cannot ensure transaction: #{reason}"}
    end
  end
end

# Example usage
result = TransactionStatus.ensure_transaction(conn, fn ->
  {:ok, _} = DuckdbEx.query(conn, "INSERT INTO accounts (id, name, balance) VALUES (6, 'Frank', 400)")
  {:ok, "Account created"}
end)
```

## Performance Considerations

### Transaction Overhead

```elixir
defmodule TransactionPerformance do
  def benchmark_with_and_without_transactions(conn, operation_count) do
    # Setup test table
    {:ok, _} = DuckdbEx.query(conn, "CREATE OR REPLACE TABLE perf_test (id INTEGER, value INTEGER)")

    # Benchmark without transaction (auto-commit)
    {time_without, _} = :timer.tc(fn ->
      Enum.each(1..operation_count, fn i ->
        {:ok, _} = DuckdbEx.query(conn, "INSERT INTO perf_test VALUES (#{i}, #{i * 2})")
      end)
    end)

    # Clear table
    {:ok, _} = DuckdbEx.query(conn, "DELETE FROM perf_test")

    # Benchmark with single transaction
    {time_with, _} = :timer.tc(fn ->
      DuckdbEx.Transaction.with_transaction(conn, fn ->
        Enum.each(1..operation_count, fn i ->
          {:ok, _} = DuckdbEx.query(conn, "INSERT INTO perf_test VALUES (#{i}, #{i * 2})")
        end)
        {:ok, "All inserts completed"}
      end)
    end)

    IO.puts("#{operation_count} operations:")
    IO.puts("Without transaction: #{time_without / 1000} ms")
    IO.puts("With transaction: #{time_with / 1000} ms")
    IO.puts("Speedup: #{Float.round(time_without / time_with, 2)}x")

    # Cleanup
    {:ok, _} = DuckdbEx.query(conn, "DROP TABLE perf_test")
  end
end

# Run performance test
TransactionPerformance.benchmark_with_and_without_transactions(conn, 1000)
```

## Error Recovery Patterns

### Retry with Exponential Backoff

```elixir
defmodule TransactionRetry do
  def retry_transaction(conn, fun, max_retries \\ 3, base_delay \\ 100) do
    retry_transaction_internal(conn, fun, 0, max_retries, base_delay)
  end

  defp retry_transaction_internal(conn, fun, attempt, max_retries, base_delay) do
    case DuckdbEx.Transaction.with_transaction(conn, fun) do
      {:ok, result} ->
        {:ok, result}

      {:error, reason} when attempt < max_retries ->
        # Log retry attempt
        IO.puts("Transaction attempt #{attempt + 1} failed: #{reason}")
        IO.puts("Retrying in #{base_delay * :math.pow(2, attempt)} ms...")

        # Exponential backoff
        delay = round(base_delay * :math.pow(2, attempt))
        :timer.sleep(delay)

        retry_transaction_internal(conn, fun, attempt + 1, max_retries, base_delay)

      {:error, reason} ->
        {:error, "Transaction failed after #{max_retries + 1} attempts: #{reason}"}
    end
  end
end

# Example usage with retry
result = TransactionRetry.retry_transaction(conn, fn ->
  # Simulate a potentially failing operation
  if :rand.uniform() < 0.3 do
    # 30% chance of simulated failure
    {:error, "Simulated network timeout"}
  else
    {:ok, _} = DuckdbEx.query(conn, "INSERT INTO accounts (id, name, balance) VALUES (7, 'Grace', 600)")
    {:ok, "Operation successful"}
  end
end, 5, 50)

case result do
  {:ok, message} -> IO.puts("Success after retries: #{message}")
  {:error, reason} -> IO.puts("Final failure: #{reason}")
end
```

## Best Practices

1. **Keep Transactions Short**: Minimize the duration of transactions to reduce lock contention
2. **Use `with_transaction/2`**: Prefer the helper function for automatic cleanup
3. **Handle Errors Gracefully**: Always provide meaningful error messages
4. **Avoid Nested Transactions**: DuckDB doesn't support them
5. **Batch Operations**: Group related operations in a single transaction for better performance
6. **Use Bulk Operations**: Combine transactions with the Appender API for maximum performance
7. **Plan for Rollback**: Ensure your operations are designed to be safely rolled back

## Troubleshooting

### Common Transaction Issues

```elixir
# Issue: Transaction already active
case DuckdbEx.Transaction.begin(conn) do
  :ok -> IO.puts("Transaction started")
  {:error, reason} -> IO.puts("Could not start transaction: #{reason}")
end

# Issue: No active transaction
case DuckdbEx.Transaction.commit(conn) do
  :ok -> IO.puts("Transaction committed")
  {:error, reason} -> IO.puts("Could not commit: #{reason}")
end

# Issue: Long-running transaction
timeout_result = Task.async(fn ->
  DuckdbEx.Transaction.with_transaction(conn, fn ->
    # Simulate long operation
    :timer.sleep(30_000)
    {:ok, "Long operation completed"}
  end)
end)
|> Task.await(5_000)  # 5 second timeout

case timeout_result do
  {:ok, message} -> IO.puts("Completed: #{message}")
  {:error, :timeout} -> IO.puts("Transaction timed out")
end
```

## Next Steps

- Learn about [Prepared Statements](prepared_statements.md) for efficient parameterized queries in transactions
- Explore [Bulk Loading](bulk_loading.md) for high-performance data insertion within transactions
- See [Examples](examples.md) for real-world transaction usage patterns
