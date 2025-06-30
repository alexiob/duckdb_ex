# NIF Build Optimization Fix

## Problem

The DuckdbEx package was rebuilding its Native Implemented Function (NIF) on every compilation, including when starting `iex -S mix` or running `mix test`. This caused unnecessary compilation delays and showed confusing log messages about downloading/building NIFs even when they already existed.

## Root Cause

1. **Aggressive Mix Alias**: The `mix.exs` had an alias that always ran `download_nif` after compilation
2. **No Existence Check**: The NIF downloader didn't check if a valid NIF already existed before attempting to download/build
3. **Makefile Always Builds**: The Makefile didn't check if the NIF was already up-to-date

## Solution

### 1. Smart NIF Detection

Added intelligent checks in `DuckdbEx.NifDownloader`:

```elixir
def download_nif() do
  # First check if NIF already exists and is valid
  if nif_exists?() and not should_force_rebuild?() do
    Logger.debug("NIF already exists, skipping download/build")
    :ok
  else
    # Proceed with download/build logic
  end
end
```

### 2. Makefile Optimization

Modified the Makefile to check if the NIF is up-to-date:

```makefile
check-nif:
    @if [ -n "$(DUCKDB_EX_FORCE_REBUILD)" ] || [ ! -f "priv/duckdb_ex$(SO_EXT)" ] || [ "c_src/duckdb_ex.c" -nt "priv/duckdb_ex$(SO_EXT)" ]; then \
        echo "Building NIF..."; \
        $(MAKE) priv/duckdb_ex$(SO_EXT); \
    else \
        echo "NIF is up to date, skipping build"; \
    fi
```

### 3. Improved Mix Tasks

Replaced the aggressive compilation alias with targeted tasks:

```elixir
defp aliases do
  [
    "deps.get": ["deps.get", "nif.download"],  # Only during initial install
    "nif.download": &download_nif_task/1,      # Manual download
    "nif.rebuild": &rebuild_nif_task/1,        # Force rebuild
    "nif.clean": &clean_nif_task/1,           # Clean artifacts
    clean: ["clean", "nif.clean"]
  ]
end
```

### 4. Environment Variable Controls

Added environment variables for fine-grained control:

- `DUCKDB_EX_BUILD=true`: Always build from source
- `DUCKDB_EX_FORCE_REBUILD=true`: Force rebuild even if NIF exists

## Behavior After Fix

### Normal Operation

- **First Install**: `mix deps.get` downloads/builds NIF once
- **Subsequent Compilations**: NIF build is skipped if up-to-date
- **iex/test Runs**: No unnecessary rebuilding

### Manual Control

```bash
# Download NIF if missing
mix nif.download

# Force rebuild (e.g., after DuckDB update)
mix nif.rebuild

# Clean all NIF artifacts
mix nif.clean

# Force rebuild from source
DUCKDB_EX_BUILD=true mix nif.rebuild
```

### Output Examples

**Before Fix** (every time):

```text
cc -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes -fPIC...
Copying DuckDB dynamic library to priv directory...
[info] Attempting to download precompiled NIF...
[warning] Failed to download precompiled NIF: HTTP 404
[info] Falling back to building from source...
```

**After Fix** (only when needed):

```text
NIF is up to date, skipping build
```

## Benefits

- **Faster Development**: No unnecessary NIF rebuilds
- **Cleaner Logs**: No confusing download/build messages during normal use
- **Predictable Behavior**: Clear understanding of when NIFs are built
- **Better Control**: Environment variables and tasks for manual management
- **Backwards Compatible**: Existing workflows continue to work

## Testing

All existing functionality confirmed working:

- ✅ NIF builds on first install
- ✅ NIF skipped on subsequent runs
- ✅ Force rebuild works with environment variable
- ✅ Manual tasks work correctly
- ✅ All tests pass without NIF rebuilds
- ✅ iex starts quickly without rebuilding
