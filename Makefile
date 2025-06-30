ERLANG_PATH = $(shell erl -eval 'io:format("~s", [lists:concat([code:root_dir(), "/erts-", erlang:system_info(version), "/include"])])' -s init stop -noshell)
CFLAGS ?= -O3 -std=c99 -finline-functions -Wall -Wmissing-prototypes
CFLAGS += -fPIC -I $(ERLANG_PATH)

# DuckDB paths - can be overridden by environment variables
DUCKDB_INCLUDE ?= $(or $(DUCKDB_INCLUDE_DIR), ./duckdb_sources)
DUCKDB_LIB_PATH ?= $(or $(DUCKDB_LIB_DIR), ./duckdb_sources)
DUCKDB_LIB ?= duckdb
DUCKDB_VERSION ?= v1.3.1

ifneq ($(OS),Windows_NT)
	CFLAGS += -fPIC

	ifeq ($(shell uname),Darwin)
		LDFLAGS += -dynamiclib -undefined dynamic_lookup
		# Add both build-time and runtime library paths
		LDFLAGS += -Wl,-rpath,$(realpath $(DUCKDB_LIB_PATH)) -Wl,-rpath,@loader_path/
		SO_EXT = .so
		DUCKDB_PLATFORM = osx-universal
	else
		LDFLAGS += -shared
		# Add both build-time and runtime library paths
		LDFLAGS += -Wl,-rpath,$(realpath $(DUCKDB_LIB_PATH)) -Wl,-rpath,$$ORIGIN/
		SO_EXT = .so
		ifeq ($(shell uname -m),aarch64)
			DUCKDB_PLATFORM = linux-aarch64
		else
			DUCKDB_PLATFORM = linux-amd64
		endif
	endif
else
	LDFLAGS += -shared
	SO_EXT = .dll
	DUCKDB_PLATFORM = windows-amd64
endif

.PHONY: all clean download-duckdb force-build

all: check-nif

# Check if NIF needs to be built
check-nif:
	@if [ -n "$(DUCKDB_EX_FORCE_REBUILD)" ] || [ ! -f "priv/duckdb_ex$(SO_EXT)" ] || [ "c_src/duckdb_ex.c" -nt "priv/duckdb_ex$(SO_EXT)" ]; then \
		echo "Building NIF..."; \
		$(MAKE) priv/duckdb_ex$(SO_EXT); \
	else \
		echo "NIF is up to date, skipping build"; \
	fi

# Force build target (for when DUCKDB_EX_FORCE_REBUILD is set)
force-build:
	$(MAKE) priv/duckdb_ex$(SO_EXT)

# Download DuckDB if not available locally
download-duckdb:
	@echo "Downloading DuckDB $(DUCKDB_VERSION) for $(DUCKDB_PLATFORM)..."
	@mkdir -p duckdb_sources
	@cd duckdb_sources && \
		curl -L -o duckdb.zip "https://github.com/duckdb/duckdb/releases/download/$(DUCKDB_VERSION)/libduckdb-$(DUCKDB_PLATFORM).zip" && \
		unzip -o duckdb.zip && \
		rm duckdb.zip
	@echo "DuckDB downloaded to duckdb_sources/"

# Check if DuckDB is available, download if not
ensure-duckdb:
	@if [ ! -f "$(DUCKDB_LIB_PATH)/libduckdb.so" ] && [ ! -f "$(DUCKDB_LIB_PATH)/libduckdb.dylib" ] && [ ! -f "$(DUCKDB_LIB_PATH)/duckdb.dll" ] && [ ! -f "$(DUCKDB_LIB_PATH)/libduckdb.a" ]; then \
		echo "DuckDB not found, downloading..."; \
		$(MAKE) download-duckdb; \
		export DUCKDB_INCLUDE=./duckdb_sources; \
		export DUCKDB_LIB_PATH=./duckdb_sources; \
	fi

priv/duckdb_ex$(SO_EXT): c_src/duckdb_ex.c ensure-duckdb
	@mkdir -p priv
	$(CC) $(CFLAGS) -shared -I$(DUCKDB_INCLUDE) -L$(DUCKDB_LIB_PATH) -l$(DUCKDB_LIB) $< -o $@ $(LDFLAGS)
	@echo "Copying DuckDB dynamic library to priv directory..."
	@if [ -f "$(DUCKDB_LIB_PATH)/libduckdb.dylib" ]; then \
		cp "$(DUCKDB_LIB_PATH)/libduckdb.dylib" "priv/libduckdb.dylib"; \
	elif [ -f "$(DUCKDB_LIB_PATH)/libduckdb.so" ]; then \
		cp "$(DUCKDB_LIB_PATH)/libduckdb.so" "priv/libduckdb.so"; \
	elif [ -f "$(DUCKDB_LIB_PATH)/duckdb.dll" ]; then \
		cp "$(DUCKDB_LIB_PATH)/duckdb.dll" "priv/libduckdb.dll"; \
	fi

clean:
	@rm -rf priv/duckdb_ex$(SO_EXT)
	@rm -rf priv/libduckdb.*

clean-all: clean
	@rm -rf duckdb_sources
