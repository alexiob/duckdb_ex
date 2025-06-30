#include <erl_nif.h>
#include <string.h>
#include <stdio.h>
#include <math.h>
#include <limits.h>
#include "duckdb.h"

// Resource types
static ErlNifResourceType *database_resource_type;
static ErlNifResourceType *connection_resource_type;
static ErlNifResourceType *result_resource_type;
static ErlNifResourceType *prepared_statement_resource_type;
static ErlNifResourceType *data_chunk_resource_type;
static ErlNifResourceType *appender_resource_type;
static ErlNifResourceType *config_resource_type;

// Resource wrappers
typedef struct
{
	duckdb_database db;
} DatabaseResource;

typedef struct
{
	duckdb_connection conn;
} ConnectionResource;

typedef struct
{
	duckdb_result result;
} ResultResource;

typedef struct
{
	duckdb_prepared_statement stmt;
} PreparedStatementResource;

typedef struct
{
	duckdb_data_chunk chunk;
} DataChunkResource;

typedef struct
{
	duckdb_appender appender;
} AppenderResource;

typedef struct
{
	duckdb_config config;
} ConfigResource;

// Atoms
// Atoms
static ERL_NIF_TERM atom_ok;
static ERL_NIF_TERM atom_error;
static ERL_NIF_TERM atom_nil;
static ERL_NIF_TERM atom_memory;

// Type atoms for columns
static ERL_NIF_TERM atom_boolean;
static ERL_NIF_TERM atom_tinyint;
static ERL_NIF_TERM atom_smallint;
static ERL_NIF_TERM atom_integer;
static ERL_NIF_TERM atom_bigint;
static ERL_NIF_TERM atom_utinyint;
static ERL_NIF_TERM atom_usmallint;
static ERL_NIF_TERM atom_uinteger;
static ERL_NIF_TERM atom_ubigint;
static ERL_NIF_TERM atom_float;
static ERL_NIF_TERM atom_double;
static ERL_NIF_TERM atom_varchar;
static ERL_NIF_TERM atom_blob;
static ERL_NIF_TERM atom_date;
static ERL_NIF_TERM atom_time;
static ERL_NIF_TERM atom_timestamp;
static ERL_NIF_TERM atom_interval;
static ERL_NIF_TERM atom_hugeint;
static ERL_NIF_TERM atom_uhugeint;
static ERL_NIF_TERM atom_list;
static ERL_NIF_TERM atom_array;
static ERL_NIF_TERM atom_struct;
static ERL_NIF_TERM atom_map;
static ERL_NIF_TERM atom_union;
static ERL_NIF_TERM atom_decimal;
static ERL_NIF_TERM atom_enum;
static ERL_NIF_TERM atom_uuid;
static ERL_NIF_TERM atom_bit;
static ERL_NIF_TERM atom_time_tz;
static ERL_NIF_TERM atom_timestamp_s;
static ERL_NIF_TERM atom_timestamp_ms;
static ERL_NIF_TERM atom_timestamp_ns;
static ERL_NIF_TERM atom_timestamp_tz;
static ERL_NIF_TERM atom_unknown;

// Helper functions
static ERL_NIF_TERM make_error(ErlNifEnv *env, const char *error_msg)
{
	ErlNifBinary bin;
	size_t len = strlen(error_msg);
	enif_alloc_binary(len, &bin);
	memcpy(bin.data, error_msg, len);
	ERL_NIF_TERM error_term = enif_make_binary(env, &bin);
	return enif_make_tuple2(env, atom_error, error_term);
}

static ERL_NIF_TERM make_ok(ErlNifEnv *env, ERL_NIF_TERM term)
{
	return enif_make_tuple2(env, atom_ok, term);
}

// Helper function to convert hugeint to Elixir integer with full precision
// Helper function to convert hugeint using varchar extraction (preserves full precision)
static ERL_NIF_TERM hugeint_to_elixir_integer_via_varchar(ErlNifEnv *env, duckdb_result *result, idx_t col, idx_t row)
{
	// Use DuckDB's built-in string conversion which preserves full precision
	char *str = duckdb_value_varchar(result, col, row);
	if (str == NULL)
	{
		return atom_nil;
	}

	// Try to parse as int64 first for efficiency
	char *endptr;
	long long val = strtoll(str, &endptr, 10);

	// If the entire string was consumed and fits in int64, return as integer
	if (*endptr == '\0' && val != LLONG_MAX && val != LLONG_MIN)
	{
		duckdb_free(str);
		return enif_make_int64(env, val);
	}

	// For very large numbers, return as binary string for TypeConverter to parse
	ErlNifBinary bin;
	size_t len = strlen(str);
	enif_alloc_binary(len, &bin);
	memcpy(bin.data, str, len);
	duckdb_free(str);
	return enif_make_binary(env, &bin);
}

// Forward declaration for robust type extraction
// Declaration removed - function integrated into main switch statement

static ERL_NIF_TERM duckdb_type_to_atom(duckdb_type type)
{
	switch (type)
	{
	case DUCKDB_TYPE_BOOLEAN:
		return atom_boolean;
	case DUCKDB_TYPE_TINYINT:
		return atom_tinyint;
	case DUCKDB_TYPE_SMALLINT:
		return atom_smallint;
	case DUCKDB_TYPE_INTEGER:
		return atom_integer;
	case DUCKDB_TYPE_BIGINT:
		return atom_bigint;
	case DUCKDB_TYPE_UTINYINT:
		return atom_utinyint;
	case DUCKDB_TYPE_USMALLINT:
		return atom_usmallint;
	case DUCKDB_TYPE_UINTEGER:
		return atom_uinteger;
	case DUCKDB_TYPE_UBIGINT:
		return atom_ubigint;
	case DUCKDB_TYPE_FLOAT:
		return atom_float;
	case DUCKDB_TYPE_DOUBLE:
		return atom_double;
	case DUCKDB_TYPE_VARCHAR:
		return atom_varchar;
	case DUCKDB_TYPE_BLOB:
		return atom_blob;
	case DUCKDB_TYPE_DATE:
		return atom_date;
	case DUCKDB_TYPE_TIME:
		return atom_time;
	case DUCKDB_TYPE_TIMESTAMP:
		return atom_timestamp;
	case DUCKDB_TYPE_INTERVAL:
		return atom_interval;
	case DUCKDB_TYPE_HUGEINT:
		return atom_hugeint;
	case DUCKDB_TYPE_UHUGEINT:
		return atom_uhugeint;
	case DUCKDB_TYPE_LIST:
		return atom_list;
	case DUCKDB_TYPE_ARRAY:
		return atom_array;
	case DUCKDB_TYPE_STRUCT:
		return atom_struct;
	case DUCKDB_TYPE_MAP:
		return atom_map;
	case DUCKDB_TYPE_UNION:
		return atom_union;
	case DUCKDB_TYPE_DECIMAL:
		return atom_decimal;
	case DUCKDB_TYPE_ENUM:
		return atom_enum;
	case DUCKDB_TYPE_UUID:
		return atom_uuid;
	case DUCKDB_TYPE_BIT:
		return atom_bit;
	case DUCKDB_TYPE_TIME_TZ:
		return atom_time_tz;
	case DUCKDB_TYPE_TIMESTAMP_S:
		return atom_timestamp_s;
	case DUCKDB_TYPE_TIMESTAMP_MS:
		return atom_timestamp_ms;
	case DUCKDB_TYPE_TIMESTAMP_NS:
		return atom_timestamp_ns;
	case DUCKDB_TYPE_TIMESTAMP_TZ:
		return atom_timestamp_tz;
	default:
		return atom_unknown;
	}
}

// Resource destructors
static void database_resource_destructor(ErlNifEnv *env, void *obj)
{
	DatabaseResource *res = (DatabaseResource *)obj;
	if (res->db)
	{
		duckdb_close(&res->db);
	}
}

static void connection_resource_destructor(ErlNifEnv *env, void *obj)
{
	ConnectionResource *res = (ConnectionResource *)obj;
	if (res->conn)
	{
		duckdb_disconnect(&res->conn);
	}
}

static void result_resource_destructor(ErlNifEnv *env, void *obj)
{
	ResultResource *res = (ResultResource *)obj;
	duckdb_destroy_result(&res->result);
}

static void prepared_statement_resource_destructor(ErlNifEnv *env, void *obj)
{
	PreparedStatementResource *res = (PreparedStatementResource *)obj;
	duckdb_destroy_prepare(&res->stmt);
}

static void data_chunk_resource_destructor(ErlNifEnv *env, void *obj)
{
	DataChunkResource *res = (DataChunkResource *)obj;
	if (res->chunk)
	{
		duckdb_destroy_data_chunk(&res->chunk);
	}
}

static void appender_resource_destructor(ErlNifEnv *env, void *obj)
{
	AppenderResource *res = (AppenderResource *)obj;
	if (res->appender)
	{
		duckdb_appender_destroy(&res->appender);
	}
}

static void config_resource_destructor(ErlNifEnv *env, void *obj)
{
	ConfigResource *res = (ConfigResource *)obj;
	if (res->config)
	{
		duckdb_destroy_config(&res->config);
	}
}

// Database operations
static ERL_NIF_TERM database_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	char path[4096];
	const char *db_path = NULL;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	// Handle nil/null case for in-memory database
	if (enif_compare(argv[0], atom_nil) == 0)
	{
		db_path = NULL; // In-memory database
	}
	else
	{
		// Try to get as binary first (for UTF-8 paths)
		ErlNifBinary path_bin;
		if (enif_inspect_binary(env, argv[0], &path_bin))
		{
			if (path_bin.size >= sizeof(path))
			{
				return make_error(env, "Path too long");
			}
			memcpy(path, path_bin.data, path_bin.size);
			path[path_bin.size] = '\0';
			db_path = path;
		}
		else
		{
			// Fall back to string for compatibility
			if (enif_get_string(env, argv[0], path, sizeof(path), ERL_NIF_LATIN1) <= 0)
			{
				return enif_make_badarg(env);
			}
			db_path = path;
		}
	}

	DatabaseResource *res = enif_alloc_resource(database_resource_type, sizeof(DatabaseResource));
	res->db = NULL;

	duckdb_state state = duckdb_open(db_path, &res->db);
	if (state == DuckDBError)
	{
		enif_release_resource(res);
		return make_error(env, "Failed to open database");
	}

	ERL_NIF_TERM result = enif_make_resource(env, res);
	enif_release_resource(res);
	return make_ok(env, result);
}

// Database operations with configuration
static ERL_NIF_TERM database_open_ext_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	char path[4096];
	const char *db_path = NULL;
	ConfigResource *config_res = NULL;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	// Handle path argument
	if (enif_compare(argv[0], atom_nil) == 0)
	{
		db_path = NULL; // In-memory database
	}
	else
	{
		// Try to get as binary first (for UTF-8 paths)
		ErlNifBinary path_bin;
		if (enif_inspect_binary(env, argv[0], &path_bin))
		{
			if (path_bin.size >= sizeof(path))
			{
				return make_error(env, "Path too long");
			}
			memcpy(path, path_bin.data, path_bin.size);
			path[path_bin.size] = '\0';
			db_path = path;
		}
		else
		{
			// Fall back to string for compatibility
			if (enif_get_string(env, argv[0], path, sizeof(path), ERL_NIF_LATIN1) <= 0)
			{
				return enif_make_badarg(env);
			}
			db_path = path;
		}
	}

	// Get config resource
	if (!enif_get_resource(env, argv[1], config_resource_type, (void **)&config_res))
	{
		return enif_make_badarg(env);
	}

	DatabaseResource *res = enif_alloc_resource(database_resource_type, sizeof(DatabaseResource));
	res->db = NULL;

	char *error_message = NULL;
	duckdb_state state = duckdb_open_ext(db_path, &res->db, config_res->config, &error_message);
	if (state == DuckDBError)
	{
		enif_release_resource(res);
		if (error_message)
		{
			ERL_NIF_TERM error_term = make_error(env, error_message);
			duckdb_free(error_message);
			return error_term;
		}
		return make_error(env, "Failed to open database");
	}

	ERL_NIF_TERM result = enif_make_resource(env, res);
	enif_release_resource(res);
	return make_ok(env, result);
}

// Configuration operations
static ERL_NIF_TERM config_create_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	if (argc != 0)
	{
		return enif_make_badarg(env);
	}

	ConfigResource *res = enif_alloc_resource(config_resource_type, sizeof(ConfigResource));
	res->config = NULL;

	duckdb_state state = duckdb_create_config(&res->config);
	if (state == DuckDBError)
	{
		enif_release_resource(res);
		return make_error(env, "Failed to create configuration");
	}

	ERL_NIF_TERM result = enif_make_resource(env, res);
	enif_release_resource(res);
	return make_ok(env, result);
}

static ERL_NIF_TERM config_set_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConfigResource *config_res;
	char name[256];
	char value[1024];

	if (argc != 3)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], config_resource_type, (void **)&config_res))
	{
		return enif_make_badarg(env);
	}

	// Get name as binary or string
	ErlNifBinary name_bin;
	if (enif_inspect_binary(env, argv[1], &name_bin))
	{
		if (name_bin.size >= sizeof(name))
		{
			return make_error(env, "Configuration name too long");
		}
		memcpy(name, name_bin.data, name_bin.size);
		name[name_bin.size] = '\0';
	}
	else if (enif_get_string(env, argv[1], name, sizeof(name), ERL_NIF_LATIN1) <= 0)
	{
		return enif_make_badarg(env);
	}

	// Get value as binary or string
	ErlNifBinary value_bin;
	if (enif_inspect_binary(env, argv[2], &value_bin))
	{
		if (value_bin.size >= sizeof(value))
		{
			return make_error(env, "Configuration value too long");
		}
		memcpy(value, value_bin.data, value_bin.size);
		value[value_bin.size] = '\0';
	}
	else if (enif_get_string(env, argv[2], value, sizeof(value), ERL_NIF_LATIN1) <= 0)
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_set_config(config_res->config, name, value);
	if (state == DuckDBError)
	{
		return make_error(env, "Failed to set configuration option");
	}

	return atom_ok;
}

// Connection operations
static ERL_NIF_TERM connection_open_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	DatabaseResource *db_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], database_resource_type, (void **)&db_res))
	{
		return enif_make_badarg(env);
	}

	ConnectionResource *res = enif_alloc_resource(connection_resource_type, sizeof(ConnectionResource));
	res->conn = NULL;

	duckdb_state state = duckdb_connect(db_res->db, &res->conn);
	if (state == DuckDBError)
	{
		enif_release_resource(res);
		return make_error(env, "Failed to connect to database");
	}

	ERL_NIF_TERM result = enif_make_resource(env, res);
	enif_release_resource(res);
	return make_ok(env, result);
}

static ERL_NIF_TERM connection_query_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConnectionResource *conn_res;
	ErlNifBinary sql_bin;
	char *sql = NULL;
	bool allocated_sql = false;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], connection_resource_type, (void **)&conn_res))
	{
		return enif_make_badarg(env);
	}

	// Try to get as binary first (for longer strings)
	if (enif_inspect_binary(env, argv[1], &sql_bin))
	{
		sql = enif_alloc(sql_bin.size + 1);
		if (!sql)
		{
			return make_error(env, "Failed to allocate memory for SQL string");
		}
		memcpy(sql, sql_bin.data, sql_bin.size);
		sql[sql_bin.size] = '\0';
		allocated_sql = true;
	}
	else
	{
		// Fall back to string for smaller strings
		static char sql_buffer[8192];
		if (enif_get_string(env, argv[1], sql_buffer, sizeof(sql_buffer), ERL_NIF_LATIN1) <= 0)
		{
			return enif_make_badarg(env);
		}
		sql = sql_buffer;
	}

	ResultResource *res = enif_alloc_resource(result_resource_type, sizeof(ResultResource));

	duckdb_state state = duckdb_query(conn_res->conn, sql, &res->result);

	if (allocated_sql)
	{
		enif_free(sql);
	}

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_result_error(&res->result);
		ERL_NIF_TERM error_term = make_error(env, error_msg ? error_msg : "Query failed");
		duckdb_destroy_result(&res->result);
		enif_release_resource(res);
		return error_term;
	}

	ERL_NIF_TERM result = enif_make_resource(env, res);
	enif_release_resource(res);
	return make_ok(env, result);
}

// Prepared statement operations
static ERL_NIF_TERM prepared_statement_prepare_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConnectionResource *conn_res;
	ErlNifBinary sql_bin;
	char *sql = NULL;
	bool allocated_sql = false;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], connection_resource_type, (void **)&conn_res))
	{
		return enif_make_badarg(env);
	}

	// Try to get as binary first (for longer strings)
	if (enif_inspect_binary(env, argv[1], &sql_bin))
	{
		sql = enif_alloc(sql_bin.size + 1);
		if (!sql)
		{
			return make_error(env, "Failed to allocate memory for SQL string");
		}
		memcpy(sql, sql_bin.data, sql_bin.size);
		sql[sql_bin.size] = '\0';
		allocated_sql = true;
	}
	else
	{
		// Fall back to string for smaller strings
		static char sql_buffer[8192];
		if (enif_get_string(env, argv[1], sql_buffer, sizeof(sql_buffer), ERL_NIF_LATIN1) <= 0)
		{
			return enif_make_badarg(env);
		}
		sql = sql_buffer;
	}

	PreparedStatementResource *res =
		enif_alloc_resource(prepared_statement_resource_type, sizeof(PreparedStatementResource));

	duckdb_state state = duckdb_prepare(conn_res->conn, sql, &res->stmt);

	if (allocated_sql)
	{
		enif_free(sql);
	}

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_prepare_error(res->stmt);
		ERL_NIF_TERM error_term = make_error(env, error_msg ? error_msg : "Failed to prepare statement");
		duckdb_destroy_prepare(&res->stmt);
		enif_release_resource(res);
		return error_term;
	}

	ERL_NIF_TERM result = enif_make_resource(env, res);
	enif_release_resource(res);
	return make_ok(env, result);
}

// Helper function to bind a parameter based on Elixir term type
static duckdb_state bind_parameter(duckdb_prepared_statement stmt, idx_t param_idx, ErlNifEnv *env,
								   ERL_NIF_TERM param)
{
	// Check if it's nil (NULL)
	if (enif_compare(param, atom_nil) == 0)
	{
		return duckdb_bind_null(stmt, param_idx);
	}

	// Check if it's a boolean
	if (enif_compare(param, enif_make_atom(env, "true")) == 0)
	{
		return duckdb_bind_boolean(stmt, param_idx, true);
	}
	if (enif_compare(param, enif_make_atom(env, "false")) == 0)
	{
		return duckdb_bind_boolean(stmt, param_idx, false);
	}

	// Check if it's an integer
	long long_val;
	if (enif_get_long(env, param, &long_val))
	{
		return duckdb_bind_int64(stmt, param_idx, (int64_t)long_val);
	}

	// Check if it's a double
	double double_val;
	if (enif_get_double(env, param, &double_val))
	{
		return duckdb_bind_double(stmt, param_idx, double_val);
	}

	// Check if it's a binary/string
	ErlNifBinary bin;
	if (enif_inspect_binary(env, param, &bin))
	{
		// Create a null-terminated string
		char *str = enif_alloc(bin.size + 1);
		if (!str)
		{
			return DuckDBError;
		}
		memcpy(str, bin.data, bin.size);
		str[bin.size] = '\0';

		duckdb_state result = duckdb_bind_varchar(stmt, param_idx, str);
		enif_free(str);
		return result;
	}

	// Try as a string (for atoms converted to strings)
	char str_buffer[1024];
	if (enif_get_string(env, param, str_buffer, sizeof(str_buffer), ERL_NIF_LATIN1) > 0)
	{
		return duckdb_bind_varchar(stmt, param_idx, str_buffer);
	}

	// Unsupported type
	return DuckDBError;
}

static ERL_NIF_TERM prepared_statement_execute_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	PreparedStatementResource *stmt_res;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], prepared_statement_resource_type, (void **)&stmt_res))
	{
		return enif_make_badarg(env);
	}

	// Handle parameter binding from argv[1] list
	if (!enif_is_list(env, argv[1]))
	{
		return enif_make_badarg(env);
	}

	// Get parameter count
	idx_t param_count = duckdb_nparams(stmt_res->stmt);

	// Get list length
	unsigned int list_length;
	if (!enif_get_list_length(env, argv[1], &list_length))
	{
		return enif_make_badarg(env);
	}

	// Check parameter count matches
	if (list_length != param_count)
	{
		char error_msg[256];
		snprintf(error_msg, sizeof(error_msg), "Parameter count mismatch: expected %llu, got %u",
				 (unsigned long long)param_count, list_length);
		return make_error(env, error_msg);
	}

	// Bind parameters
	ERL_NIF_TERM list = argv[1];
	ERL_NIF_TERM head, tail;

	for (idx_t i = 0; i < param_count; i++)
	{
		if (!enif_get_list_cell(env, list, &head, &tail))
		{
			return make_error(env, "Failed to get parameter from list");
		}

		duckdb_state bind_state = bind_parameter(stmt_res->stmt, i + 1, env, head); // DuckDB uses 1-based indexing
		if (bind_state == DuckDBError)
		{
			char error_msg[256];
			snprintf(error_msg, sizeof(error_msg), "Failed to bind parameter %llu", (unsigned long long)(i + 1));
			return make_error(env, error_msg);
		}

		list = tail;
	}

	ResultResource *res = enif_alloc_resource(result_resource_type, sizeof(ResultResource));

	duckdb_state state = duckdb_execute_prepared(stmt_res->stmt, &res->result);
	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_result_error(&res->result);
		ERL_NIF_TERM error_term = make_error(env, error_msg ? error_msg : "Failed to execute prepared statement");
		duckdb_destroy_result(&res->result);
		enif_release_resource(res);
		return error_term;
	}

	ERL_NIF_TERM result = enif_make_resource(env, res);
	enif_release_resource(res);
	return make_ok(env, result);
}

// Result operations
static ERL_NIF_TERM result_columns_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ResultResource *res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], result_resource_type, (void **)&res))
	{
		return enif_make_badarg(env);
	}

	idx_t column_count = duckdb_column_count(&res->result);
	ERL_NIF_TERM *columns = enif_alloc(sizeof(ERL_NIF_TERM) * column_count);

	for (idx_t i = 0; i < column_count; i++)
	{
		const char *name = duckdb_column_name(&res->result, i);
		duckdb_type type = duckdb_column_type(&res->result, i);

		// Create binary for column name
		ErlNifBinary name_bin;
		size_t name_len = strlen(name);
		enif_alloc_binary(name_len, &name_bin);
		memcpy(name_bin.data, name, name_len);
		ERL_NIF_TERM name_term = enif_make_binary(env, &name_bin);

		ERL_NIF_TERM type_term = duckdb_type_to_atom(type);

		ERL_NIF_TERM keys[] = {enif_make_atom(env, "name"), enif_make_atom(env, "type")};
		ERL_NIF_TERM values[] = {name_term, type_term};

		enif_make_map_from_arrays(env, keys, values, 2, &columns[i]);
	}

	ERL_NIF_TERM result = enif_make_list_from_array(env, columns, column_count);
	enif_free(columns);
	return result;
}

static ERL_NIF_TERM result_rows_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ResultResource *res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], result_resource_type, (void **)&res))
	{
		return enif_make_badarg(env);
	}

	idx_t row_count = duckdb_row_count(&res->result);
	idx_t column_count = duckdb_column_count(&res->result);

	// WORKAROUND: DuckDB has a memory corruption bug when extracting UUID values in multi-column contexts.
	// Any attempt to call duckdb_value_varchar (or other extraction functions) on a result that contains
	// UUID columns causes segfaults when processing subsequent columns. Until this is fixed in DuckDB,
	// we need to be very careful with UUID extraction.
	// The chunked API works correctly, so this issue is specific to the regular result API.
	bool has_uuid = false;
	for (idx_t c = 0; c < column_count; c++)
	{
		duckdb_type type = duckdb_column_type(&res->result, c);
		if (type == DUCKDB_TYPE_UUID)
		{
			has_uuid = true;
			break;
		}
	}

	ERL_NIF_TERM *rows = enif_alloc(sizeof(ERL_NIF_TERM) * row_count);

	for (idx_t r = 0; r < row_count; r++)
	{
		ERL_NIF_TERM *row_values = enif_alloc(sizeof(ERL_NIF_TERM) * column_count);

		if (has_uuid && column_count > 1)
		{
			// If any column is UUID in multi-column context, set all columns to nil to avoid DuckDB corruption bug
			for (idx_t c = 0; c < column_count; c++)
			{
				row_values[c] = atom_nil;
			}
		}
		else
		{
			// Normal processing for non-UUID results or single-column UUID
			for (idx_t c = 0; c < column_count; c++)
			{
				duckdb_type type = duckdb_column_type(&res->result, c);

				// Check for NULL first for all types
				bool is_null = duckdb_value_is_null(&res->result, c, r);

				if (is_null)
				{
					row_values[c] = atom_nil;
					continue;
				}

				// Handle UUID specially - only in single column context
				if (type == DUCKDB_TYPE_UUID)
				{
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					continue;
				}

				// For all other types, try the appropriate extraction method

				switch (type)
				{
				case DUCKDB_TYPE_BOOLEAN:
				{
					// Use varchar extraction instead of deprecated duckdb_value_boolean
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						bool val = (strcmp(str, "true") == 0 || strcmp(str, "1") == 0);
						row_values[c] = val ? enif_make_atom(env, "true") : enif_make_atom(env, "false");
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_TINYINT:
				{
					// Use direct data access instead of deprecated duckdb_value_int8
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						int8_t val = (int8_t)strtol(str, NULL, 10);
						row_values[c] = enif_make_int(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_SMALLINT:
				{
					// Use direct data access instead of deprecated duckdb_value_int16
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						int16_t val = (int16_t)strtol(str, NULL, 10);
						row_values[c] = enif_make_int(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_INTEGER:
				{
					// Use direct data access instead of deprecated duckdb_value_int32
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						int32_t val = (int32_t)strtol(str, NULL, 10);
						row_values[c] = enif_make_int(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_BIGINT:
				{
					// Use direct data access instead of deprecated duckdb_value_int64
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						int64_t val = strtoll(str, NULL, 10);
						row_values[c] = enif_make_long(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_UTINYINT:
				{
					// Use direct data access instead of deprecated duckdb_value_uint8
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						uint8_t val = (uint8_t)strtoul(str, NULL, 10);
						row_values[c] = enif_make_uint(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_USMALLINT:
				{
					// Use direct data access instead of deprecated duckdb_value_uint16
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						uint16_t val = (uint16_t)strtoul(str, NULL, 10);
						row_values[c] = enif_make_uint(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_UINTEGER:
				{
					// Use direct data access instead of deprecated duckdb_value_uint32
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						uint32_t val = (uint32_t)strtoul(str, NULL, 10);
						row_values[c] = enif_make_uint(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_UBIGINT:
				{
					// Use direct data access instead of deprecated duckdb_value_uint64
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						uint64_t val = strtoull(str, NULL, 10);
						row_values[c] = enif_make_uint64(env, val);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_DECIMAL:
				{
					// Extract DECIMAL as varchar for precision
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				case DUCKDB_TYPE_TIMESTAMP:
				case DUCKDB_TYPE_TIMESTAMP_S:
				case DUCKDB_TYPE_TIMESTAMP_MS:
				case DUCKDB_TYPE_TIMESTAMP_NS:
				case DUCKDB_TYPE_TIMESTAMP_TZ:
				{
					// Extract all timestamp types as varchar for consistency
					if (duckdb_value_is_null(&res->result, c, r))
					{
						row_values[c] = atom_nil;
					}
					else
					{
						char *str = duckdb_value_varchar(&res->result, c, r);
						if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
						{
							ErlNifBinary bin;
							size_t len = strlen(str);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, str, len);
							row_values[c] = enif_make_binary(env, &bin);
							duckdb_free(str);
						}
						else
						{
							// Varchar extraction failed for non-NULL timestamp, return placeholder
							char buffer[64];
							snprintf(buffer, sizeof(buffer), "<timestamp_extraction_failed>");
							ErlNifBinary bin;
							size_t len = strlen(buffer);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, buffer, len);
							row_values[c] = enif_make_binary(env, &bin);
							if (str)
								duckdb_free(str);
						}
					}
					break;
				}
				case DUCKDB_TYPE_HUGEINT:
				{
					// Use varchar extraction to preserve full precision
					row_values[c] = hugeint_to_elixir_integer_via_varchar(env, &res->result, c, r);
					break;
				}
				case DUCKDB_TYPE_FLOAT:
				{
					// Use varchar extraction instead of deprecated duckdb_value_float
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						float val = strtof(str, NULL);
						// Handle special float values (infinity, NaN)
						if (isnan(val))
						{
							row_values[c] = enif_make_atom(env, "nan");
						}
						else if (isinf(val))
						{
							if (val > 0)
							{
								row_values[c] = enif_make_atom(env, "infinity");
							}
							else
							{
								row_values[c] = enif_make_atom(env, "negative_infinity");
							}
						}
						else
						{
							row_values[c] = enif_make_double(env, (double)val);
						}
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_DOUBLE:
				{
					// Use varchar extraction instead of deprecated duckdb_value_double
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL)
					{
						double val = strtod(str, NULL);
						// Handle special double values (infinity, NaN)
						if (isnan(val))
						{
							row_values[c] = enif_make_atom(env, "nan");
						}
						else if (isinf(val))
						{
							if (val > 0)
							{
								row_values[c] = enif_make_atom(env, "infinity");
							}
							else
							{
								row_values[c] = enif_make_atom(env, "negative_infinity");
							}
						}
						else
						{
							row_values[c] = enif_make_double(env, val);
						}
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}
				case DUCKDB_TYPE_DATE:
				{
					// Use varchar extraction instead of deprecated duckdb_value_date
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						// Return the date string directly
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				case DUCKDB_TYPE_TIME:
				{
					// Use varchar extraction instead of deprecated duckdb_value_time
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						// Return the time string directly
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}

				case DUCKDB_TYPE_INTERVAL:
				{
					// Use varchar extraction instead of deprecated duckdb_value_interval
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						// Return the interval string directly
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				case DUCKDB_TYPE_BLOB:
				{
					duckdb_blob blob = duckdb_value_blob(&res->result, c, r);
					if (blob.data != NULL && blob.size > 0)
					{
						ErlNifBinary bin;
						enif_alloc_binary(blob.size, &bin);
						memcpy(bin.data, blob.data, blob.size);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(blob.data);
					}
					else
					{
						// Empty blob
						ErlNifBinary bin;
						enif_alloc_binary(0, &bin);
						row_values[c] = enif_make_binary(env, &bin);
					}
					break;
				}
				case DUCKDB_TYPE_VARCHAR:
				{
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
					}
					break;
				}

				case DUCKDB_TYPE_TIME_TZ:
				{
					// Time with timezone - use varchar for string representation
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				case DUCKDB_TYPE_BIT:
				{
					// Bit string - use varchar for string representation
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				case DUCKDB_TYPE_UHUGEINT:
				{
					// Unsigned huge integer - use varchar for string representation
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				case DUCKDB_TYPE_ENUM:
				{
					// For ENUMs, duckdb_value_varchar() may not work reliably
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						if (str)
							duckdb_free(str);

						if (duckdb_value_is_null(&res->result, c, r))
						{
							row_values[c] = atom_nil;
						}
						else
						{
							// ENUM extraction failed with regular API, return placeholder
							char buffer[64];
							snprintf(buffer, sizeof(buffer), "<regular_api_enum_limitation>");
							ErlNifBinary bin;
							size_t len = strlen(buffer);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, buffer, len);
							row_values[c] = enif_make_binary(env, &bin);
						}
					}
					break;
				}
				case DUCKDB_TYPE_UUID:
				{
					// For UUIDs, duckdb_value_varchar() doesn't work reliably
					// Use a special placeholder that the TypeConverter will handle
					if (duckdb_value_is_null(&res->result, c, r))
					{
						row_values[c] = atom_nil;
					}
					else
					{
						// Not null, but we can't extract the value reliably with regular API
						// Return a special placeholder for TypeConverter to handle
						char buffer[64];
						snprintf(buffer, sizeof(buffer), "<regular_api_uuid_limitation>");
						ErlNifBinary bin;
						size_t len = strlen(buffer);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, buffer, len);
						row_values[c] = enif_make_binary(env, &bin);
					}
					break;
				}
				case DUCKDB_TYPE_LIST:
				{
					// For LIST types, first check if it's truly NULL using duckdb_value_is_null
					if (duckdb_value_is_null(&res->result, c, r))
					{
						row_values[c] = atom_nil;
					}
					else
					{
						// Not NULL, try varchar representation
						char *str = duckdb_value_varchar(&res->result, c, r);
						if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
						{
							ErlNifBinary bin;
							size_t len = strlen(str);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, str, len);
							row_values[c] = enif_make_binary(env, &bin);
							duckdb_free(str);
						}
						else
						{
							// Varchar extraction failed for non-NULL list, return placeholder
							char buffer[64];
							snprintf(buffer, sizeof(buffer), "<unsupported_list_type>");
							ErlNifBinary bin;
							size_t len = strlen(buffer);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, buffer, len);
							row_values[c] = enif_make_binary(env, &bin);
							if (str)
								duckdb_free(str);
						}
					}
					break;
				}
				case DUCKDB_TYPE_STRUCT:
				{
					// For STRUCT types, first check if it's truly NULL using duckdb_value_is_null
					if (duckdb_value_is_null(&res->result, c, r))
					{
						row_values[c] = atom_nil;
					}
					else
					{
						// Not NULL, try varchar representation
						char *str = duckdb_value_varchar(&res->result, c, r);
						if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
						{
							ErlNifBinary bin;
							size_t len = strlen(str);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, str, len);
							row_values[c] = enif_make_binary(env, &bin);
							duckdb_free(str);
						}
						else
						{
							// Varchar extraction failed for non-NULL struct, return placeholder
							char buffer[64];
							snprintf(buffer, sizeof(buffer), "<unsupported_struct_type>");
							ErlNifBinary bin;
							size_t len = strlen(buffer);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, buffer, len);
							row_values[c] = enif_make_binary(env, &bin);
							if (str)
								duckdb_free(str);
						}
					}
					break;
				}
				case DUCKDB_TYPE_MAP:
				{
					// For MAP types, first check if it's truly NULL using duckdb_value_is_null
					if (duckdb_value_is_null(&res->result, c, r))
					{
						row_values[c] = atom_nil;
					}
					else
					{
						// Not NULL, try varchar representation
						char *str = duckdb_value_varchar(&res->result, c, r);
						if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
						{
							ErlNifBinary bin;
							size_t len = strlen(str);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, str, len);
							row_values[c] = enif_make_binary(env, &bin);
							duckdb_free(str);
						}
						else
						{
							// Varchar extraction failed for non-NULL map, return placeholder
							char buffer[64];
							snprintf(buffer, sizeof(buffer), "<unsupported_map_type>");
							ErlNifBinary bin;
							size_t len = strlen(buffer);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, buffer, len);
							row_values[c] = enif_make_binary(env, &bin);
							if (str)
								duckdb_free(str);
						}
					}
					break;
				}
				case DUCKDB_TYPE_ARRAY:
				{
					// For ARRAY types, first check if it's truly NULL using duckdb_value_is_null
					if (duckdb_value_is_null(&res->result, c, r))
					{
						row_values[c] = atom_nil;
					}
					else
					{
						// Not NULL, try varchar representation
						char *str = duckdb_value_varchar(&res->result, c, r);
						if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
						{
							ErlNifBinary bin;
							size_t len = strlen(str);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, str, len);
							row_values[c] = enif_make_binary(env, &bin);
							duckdb_free(str);
						}
						else
						{
							// Varchar extraction failed for non-NULL array, return placeholder
							char buffer[64];
							snprintf(buffer, sizeof(buffer), "<unsupported_array_type>");
							ErlNifBinary bin;
							size_t len = strlen(buffer);
							enif_alloc_binary(len, &bin);
							memcpy(bin.data, buffer, len);
							row_values[c] = enif_make_binary(env, &bin);
							if (str)
								duckdb_free(str);
						}
					}
					break;
				}
				case DUCKDB_TYPE_UNION:
				{
					// For UNION types, get varchar representation
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				default:
				{
					// For unsupported types, try varchar extraction and fallback to nil
					char *str = duckdb_value_varchar(&res->result, c, r);
					if (str != NULL && strlen(str) > 0 && strcmp(str, "NULL") != 0)
					{
						ErlNifBinary bin;
						size_t len = strlen(str);
						enif_alloc_binary(len, &bin);
						memcpy(bin.data, str, len);
						row_values[c] = enif_make_binary(env, &bin);
						duckdb_free(str);
					}
					else
					{
						row_values[c] = atom_nil;
						if (str)
							duckdb_free(str);
					}
					break;
				}
				}
			}
		}

		rows[r] = enif_make_tuple_from_array(env, row_values, column_count);
		enif_free(row_values);
	}

	ERL_NIF_TERM result = enif_make_list_from_array(env, rows, row_count);
	enif_free(rows);
	return result;
}

static ERL_NIF_TERM result_row_count_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ResultResource *res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], result_resource_type, (void **)&res))
	{
		return enif_make_badarg(env);
	}

	idx_t count = duckdb_row_count(&res->result);
	return enif_make_uint64(env, count);
}

static ERL_NIF_TERM result_column_count_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ResultResource *res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], result_resource_type, (void **)&res))
	{
		return enif_make_badarg(env);
	}

	idx_t count = duckdb_column_count(&res->result);
	return enif_make_uint64(env, count);
}

// Chunked API functions for better complex type support
static ERL_NIF_TERM result_chunk_count_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ResultResource *res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], result_resource_type, (void **)&res))
	{
		return enif_make_badarg(env);
	}

	idx_t chunk_count = duckdb_result_chunk_count(res->result);
	return enif_make_uint64(env, chunk_count);
}

static ERL_NIF_TERM result_get_chunk_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ResultResource *res;
	unsigned long chunk_index;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], result_resource_type, (void **)&res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_ulong(env, argv[1], &chunk_index))
	{
		return enif_make_badarg(env);
	}

	duckdb_data_chunk chunk = duckdb_result_get_chunk(res->result, (idx_t)chunk_index);
	if (!chunk)
	{
		return make_error(env, "Invalid chunk index or no chunk available");
	}

	// Create data chunk resource
	DataChunkResource *chunk_res = enif_alloc_resource(data_chunk_resource_type, sizeof(DataChunkResource));
	chunk_res->chunk = chunk;

	ERL_NIF_TERM chunk_term = enif_make_resource(env, chunk_res);
	enif_release_resource(chunk_res);

	return make_ok(env, chunk_term);
}

// Helper function to extract value from vector based on type and position
static ERL_NIF_TERM extract_vector_value(ErlNifEnv *env, duckdb_vector vector, duckdb_logical_type logical_type,
										 idx_t row_idx)
{
	duckdb_type type_id = duckdb_get_type_id(logical_type);
	void *data = duckdb_vector_get_data(vector);
	uint64_t *validity = duckdb_vector_get_validity(vector);

	// For complex types like STRUCT, LIST, MAP, the data pointer might be NULL
	// because they store data differently. Only check data for primitive types.
	bool is_complex_type = (type_id == DUCKDB_TYPE_STRUCT || type_id == DUCKDB_TYPE_LIST ||
							type_id == DUCKDB_TYPE_ARRAY || type_id == DUCKDB_TYPE_MAP);

	// Check if data is NULL (only for non-complex types)
	if (!is_complex_type && !data)
	{
		return atom_nil;
	}

	// Check if value is NULL
	if (validity && !duckdb_validity_row_is_valid(validity, row_idx))
	{
		return atom_nil;
	}

	switch (type_id)
	{
	case DUCKDB_TYPE_BOOLEAN:
	{
		bool *bool_data = (bool *)data;
		return bool_data[row_idx] ? enif_make_atom(env, "true") : enif_make_atom(env, "false");
	}
	case DUCKDB_TYPE_TINYINT:
	{
		int8_t *int8_data = (int8_t *)data;
		return enif_make_int(env, int8_data[row_idx]);
	}
	case DUCKDB_TYPE_SMALLINT:
	{
		int16_t *int16_data = (int16_t *)data;
		return enif_make_int(env, int16_data[row_idx]);
	}
	case DUCKDB_TYPE_INTEGER:
	{
		int32_t *int32_data = (int32_t *)data;
		return enif_make_int(env, int32_data[row_idx]);
	}
	case DUCKDB_TYPE_BIGINT:
	{
		int64_t *int64_data = (int64_t *)data;
		return enif_make_long(env, int64_data[row_idx]);
	}
	case DUCKDB_TYPE_UTINYINT:
	{
		uint8_t *uint8_data = (uint8_t *)data;
		return enif_make_uint(env, uint8_data[row_idx]);
	}
	case DUCKDB_TYPE_USMALLINT:
	{
		uint16_t *uint16_data = (uint16_t *)data;
		return enif_make_uint(env, uint16_data[row_idx]);
	}
	case DUCKDB_TYPE_UINTEGER:
	{
		uint32_t *uint32_data = (uint32_t *)data;
		return enif_make_uint(env, uint32_data[row_idx]);
	}
	case DUCKDB_TYPE_UBIGINT:
	{
		uint64_t *uint64_data = (uint64_t *)data;
		return enif_make_uint64(env, uint64_data[row_idx]);
	}
	case DUCKDB_TYPE_HUGEINT:
	{
		duckdb_hugeint *hugeint_data = (duckdb_hugeint *)data;
		duckdb_hugeint value = hugeint_data[row_idx];

		// Check if it fits in a 64-bit signed integer first
		if (value.upper == 0 && value.lower <= 9223372036854775807ULL)
		{
			// Positive number that fits in int64
			return enif_make_int64(env, (int64_t)value.lower);
		}
		else if (value.upper == -1 && value.lower >= 9223372036854775808ULL)
		{
			// Negative number that fits in int64 (two's complement)
			return enif_make_int64(env, (int64_t)value.lower);
		}
		else
		{
			// For large numbers, we need exact precision
			// Since the chunked API doesn't allow varchar conversion,
			// we'll return the raw components as a special format
			// that the TypeConverter can handle
			char buffer[128];
			snprintf(buffer, sizeof(buffer), "hugeint:%lld:%llu", (long long)value.upper,
					 (unsigned long long)value.lower);

			// Return as binary string for TypeConverter to handle
			ErlNifBinary bin;
			size_t len = strlen(buffer);
			enif_alloc_binary(len, &bin);
			memcpy(bin.data, buffer, len);
			return enif_make_binary(env, &bin);
		}
	}
	case DUCKDB_TYPE_FLOAT:
	{
		float *float_data = (float *)data;
		return enif_make_double(env, (double)float_data[row_idx]);
	}
	case DUCKDB_TYPE_DOUBLE:
	{
		double *double_data = (double *)data;
		return enif_make_double(env, double_data[row_idx]);
	}
	case DUCKDB_TYPE_DECIMAL:
	{
		// For DECIMAL, we need to get the scale and width from the logical type
		// and treat the underlying data as the internal storage type
		uint8_t width = duckdb_decimal_width(logical_type);
		uint8_t scale = duckdb_decimal_scale(logical_type);
		duckdb_type internal_type = duckdb_decimal_internal_type(logical_type);

		char buffer[64];
		int64_t raw_value = 0;

		// Get the raw value based on internal storage type
		switch (internal_type)
		{
		case DUCKDB_TYPE_SMALLINT:
		{
			int16_t *int16_data = (int16_t *)data;
			raw_value = int16_data[row_idx];
			break;
		}
		case DUCKDB_TYPE_INTEGER:
		{
			int32_t *int32_data = (int32_t *)data;
			raw_value = int32_data[row_idx];
			break;
		}
		case DUCKDB_TYPE_BIGINT:
		{
			int64_t *int64_data = (int64_t *)data;
			raw_value = int64_data[row_idx];
			break;
		}
		case DUCKDB_TYPE_HUGEINT:
		{
			// For hugeint, fall back to double conversion
			duckdb_hugeint *hugeint_data = (duckdb_hugeint *)data;
			duckdb_decimal decimal_val = {width, scale, hugeint_data[row_idx]};
			double decimal_double = duckdb_decimal_to_double(decimal_val);
			snprintf(buffer, sizeof(buffer), "%.10g", decimal_double);
			ErlNifBinary bin;
			size_t len = strlen(buffer);
			enif_alloc_binary(len, &bin);
			memcpy(bin.data, buffer, len);
			return enif_make_binary(env, &bin);
		}
		default:
			snprintf(buffer, sizeof(buffer), "unsupported_decimal_internal_type_%d", (int)internal_type);
			ErlNifBinary bin;
			size_t len = strlen(buffer);
			enif_alloc_binary(len, &bin);
			memcpy(bin.data, buffer, len);
			return enif_make_binary(env, &bin);
		}

		// Format the decimal value
		if (scale == 0)
		{
			// No fractional part, return as integer
			return enif_make_long(env, raw_value);
		}
		else
		{
			// Has fractional part, calculate as double and return as float
			double divisor = 1.0;
			for (int i = 0; i < scale; i++)
			{
				divisor *= 10.0;
			}
			double decimal_value = (double)raw_value / divisor;
			return enif_make_double(env, decimal_value);
		}
	}
	case DUCKDB_TYPE_DATE:
	{
		duckdb_date *date_data = (duckdb_date *)data;
		duckdb_date date = date_data[row_idx];

		// Convert date to proper ISO format using DuckDB's date conversion
		duckdb_date_struct date_struct = duckdb_from_date(date);
		char buffer[32];
		snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d", date_struct.year, date_struct.month, date_struct.day);

		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_TIME:
	{
		duckdb_time *time_data = (duckdb_time *)data;
		duckdb_time time = time_data[row_idx];

		// Convert time to proper ISO format using DuckDB's time conversion
		duckdb_time_struct time_struct = duckdb_from_time(time);
		char buffer[32];
		snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d.%06d", time_struct.hour, time_struct.min, time_struct.sec,
				 time_struct.micros);

		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_TIMESTAMP:
	{
		duckdb_timestamp *timestamp_data = (duckdb_timestamp *)data;
		duckdb_timestamp timestamp = timestamp_data[row_idx];

		// Convert timestamp to proper ISO format using DuckDB's timestamp conversion
		duckdb_timestamp_struct ts_struct = duckdb_from_timestamp(timestamp);
		char buffer[64];
		snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d %02d:%02d:%02d.%06d", ts_struct.date.year,
				 ts_struct.date.month, ts_struct.date.day, ts_struct.time.hour, ts_struct.time.min, ts_struct.time.sec,
				 ts_struct.time.micros);

		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_TIMESTAMP_S:
	{
		duckdb_timestamp_s *timestamp_data = (duckdb_timestamp_s *)data;
		duckdb_timestamp_s timestamp = timestamp_data[row_idx];

		// Convert timestamp (seconds) to string representation
		char buffer[32];
		snprintf(buffer, sizeof(buffer), "%lld", (long long)timestamp.seconds);

		// Return as binary instead of charlist
		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_TIMESTAMP_MS:
	{
		duckdb_timestamp_ms *timestamp_data = (duckdb_timestamp_ms *)data;
		duckdb_timestamp_ms timestamp = timestamp_data[row_idx];

		// Convert timestamp (milliseconds) to string representation
		char buffer[32];
		snprintf(buffer, sizeof(buffer), "%lld", (long long)timestamp.millis);

		// Return as binary instead of charlist
		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_TIMESTAMP_NS:
	{
		duckdb_timestamp_ns *timestamp_data = (duckdb_timestamp_ns *)data;
		duckdb_timestamp_ns timestamp = timestamp_data[row_idx];

		// Convert timestamp (nanoseconds) to string representation
		char buffer[32];
		snprintf(buffer, sizeof(buffer), "%lld", (long long)timestamp.nanos);

		// Return as binary instead of charlist
		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_TIMESTAMP_TZ:
	{
		// TIMESTAMP_TZ is not directly supported as a C structure in DuckDB
		// Return as unsupported for now
		char buffer[64];
		snprintf(buffer, sizeof(buffer), "unsupported_timestamp_tz_type");
		return enif_make_atom(env, buffer);
	}
	case DUCKDB_TYPE_TIME_TZ:
	{
		duckdb_time_tz *time_data = (duckdb_time_tz *)data;
		duckdb_time_tz time = time_data[row_idx];

		// Return time with timezone as a tuple {micros, offset}
		duckdb_time_tz_struct decomposed = duckdb_from_time_tz(time);
		ERL_NIF_TERM micros = enif_make_long(env, decomposed.time.micros);
		ERL_NIF_TERM offset = enif_make_int(env, decomposed.offset);

		return enif_make_tuple2(env, micros, offset);
	}
	case DUCKDB_TYPE_UUID:
	{
		duckdb_hugeint *uuid_data = (duckdb_hugeint *)data;
		duckdb_hugeint uuid = uuid_data[row_idx];

		// Convert UUID (stored as hugeint) to standard UUID string format
		// Format as xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
		char buffer[64];
		snprintf(buffer, sizeof(buffer), "%08llx-%04llx-%04llx-%04llx-%012llx",
				 (unsigned long long)((uuid.upper >> 32) & 0xFFFFFFFF), // first 32 bits
				 (unsigned long long)((uuid.upper >> 16) & 0xFFFF),		// next 16 bits
				 (unsigned long long)(uuid.upper & 0xFFFF),				// next 16 bits
				 (unsigned long long)((uuid.lower >> 48) & 0xFFFF),		// next 16 bits
				 (unsigned long long)(uuid.lower & 0xFFFFFFFFFFFFLL));	// last 48 bits

		// Return as binary instead of charlist
		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_ENUM:
	{
		// Enum values are stored as their underlying integer type
		// We need to get the internal type and dictionary to convert back to string
		duckdb_type internal_type = duckdb_enum_internal_type(logical_type);
		uint32_t dict_size = duckdb_enum_dictionary_size(logical_type);

		uint32_t enum_index = 0;
		switch (internal_type)
		{
		case DUCKDB_TYPE_UTINYINT:
		{
			uint8_t *uint8_data = (uint8_t *)data;
			enum_index = uint8_data[row_idx];
			break;
		}
		case DUCKDB_TYPE_USMALLINT:
		{
			uint16_t *uint16_data = (uint16_t *)data;
			enum_index = uint16_data[row_idx];
			break;
		}
		case DUCKDB_TYPE_UINTEGER:
		{
			uint32_t *uint32_data = (uint32_t *)data;
			enum_index = uint32_data[row_idx];
			break;
		}
		default:
			return enif_make_atom(env, "unsupported_enum_internal_type");
		}

		if (enum_index < dict_size)
		{
			char *enum_string = duckdb_enum_dictionary_value(logical_type, enum_index);
			if (enum_string)
			{
				size_t len = strlen(enum_string);
				ErlNifBinary bin;
				enif_alloc_binary(len, &bin);
				memcpy(bin.data, enum_string, len);
				duckdb_free(enum_string);
				return enif_make_binary(env, &bin);
			}
		}

		return enif_make_atom(env, "invalid_enum_value");
	}
	case DUCKDB_TYPE_BIT:
	{
		// Bit strings are typically stored as binary data
		duckdb_string_t *bit_data = (duckdb_string_t *)data;
		const char *bit_ptr = duckdb_string_t_data(&bit_data[row_idx]);
		uint32_t bit_len = duckdb_string_t_length(bit_data[row_idx]);

		// Return bit string as binary data
		ErlNifBinary bin;
		enif_alloc_binary(bit_len, &bin);
		memcpy(bin.data, bit_ptr, bit_len);

		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_ARRAY:
	{
		// Handle ARRAY type (similar to LIST but with fixed size)
		idx_t array_size = duckdb_array_type_array_size(logical_type);

		if (array_size == 0)
		{
			return enif_make_list(env, 0);
		}

		// Get child vector and type
		duckdb_vector child_vector = duckdb_array_vector_get_child(vector);
		if (!child_vector)
		{
			return enif_make_list(env, 0);
		}

		duckdb_logical_type child_type = duckdb_array_type_child_type(logical_type);

		// Build Elixir list from DuckDB array
		ERL_NIF_TERM *array_elements = enif_alloc(sizeof(ERL_NIF_TERM) * array_size);
		if (!array_elements)
		{
			duckdb_destroy_logical_type(&child_type);
			return atom_nil;
		}

		for (idx_t i = 0; i < array_size; i++)
		{
			array_elements[i] = extract_vector_value(env, child_vector, child_type, row_idx * array_size + i);
		}

		ERL_NIF_TERM result_list = enif_make_list_from_array(env, array_elements, array_size);
		enif_free(array_elements);
		duckdb_destroy_logical_type(&child_type);

		return result_list;
	}
	case DUCKDB_TYPE_UHUGEINT:
	{
		duckdb_uhugeint *uhugeint_data = (duckdb_uhugeint *)data;
		duckdb_uhugeint value = uhugeint_data[row_idx];

		// Convert uhugeint to string representation
		char buffer[64];
		if (value.upper == 0)
		{
			// Simple case: value fits in lower 64 bits
			snprintf(buffer, sizeof(buffer), "%llu", (unsigned long long)value.lower);
		}
		else
		{
			// Complex case: use approximation for very large numbers
			// For unsigned, we can safely use the upper and lower parts
			snprintf(buffer, sizeof(buffer), "%llu%016llx", (unsigned long long)value.upper,
					 (unsigned long long)value.lower);
		}

		ErlNifBinary bin;
		size_t len = strlen(buffer);
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, buffer, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_INTERVAL:
	{
		duckdb_interval *interval_data = (duckdb_interval *)data;
		duckdb_interval interval = interval_data[row_idx];

		// Return interval as a tuple {months, days, micros}
		ERL_NIF_TERM months = enif_make_int(env, interval.months);
		ERL_NIF_TERM days = enif_make_int(env, interval.days);
		ERL_NIF_TERM micros = enif_make_long(env, interval.micros);

		return enif_make_tuple3(env, months, days, micros);
	}
	case DUCKDB_TYPE_BLOB:
	{
		duckdb_string_t *blob_data = (duckdb_string_t *)data;
		const char *blob_ptr = duckdb_string_t_data(&blob_data[row_idx]);
		uint32_t blob_len = duckdb_string_t_length(blob_data[row_idx]);

		// Return blob as binary data
		ErlNifBinary bin;
		enif_alloc_binary(blob_len, &bin);
		memcpy(bin.data, blob_ptr, blob_len);

		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_VARCHAR:
	{
		duckdb_string_t *string_data = (duckdb_string_t *)data;
		const char *str = duckdb_string_t_data(&string_data[row_idx]);
		uint32_t len = duckdb_string_t_length(string_data[row_idx]);

		ErlNifBinary bin;
		enif_alloc_binary(len, &bin);
		memcpy(bin.data, str, len);
		return enif_make_binary(env, &bin);
	}
	case DUCKDB_TYPE_LIST:
	{
		// Handle LIST type
		duckdb_list_entry *list_data = (duckdb_list_entry *)data;
		duckdb_list_entry entry = list_data[row_idx];

		// Safety check for list length
		if (entry.length == 0)
		{
			return enif_make_list(env, 0);
		}

		// Get child vector and type
		duckdb_vector child_vector = duckdb_list_vector_get_child(vector);
		if (!child_vector)
		{
			return enif_make_list(env, 0);
		}

		duckdb_logical_type child_type = duckdb_list_type_child_type(logical_type);

		// Build Elixir list from DuckDB list
		ERL_NIF_TERM *list_elements = enif_alloc(sizeof(ERL_NIF_TERM) * entry.length);
		if (!list_elements)
		{
			duckdb_destroy_logical_type(&child_type);
			return atom_nil;
		}

		for (idx_t i = 0; i < entry.length; i++)
		{
			list_elements[i] = extract_vector_value(env, child_vector, child_type, entry.offset + i);
		}

		ERL_NIF_TERM result_list = enif_make_list_from_array(env, list_elements, entry.length);
		enif_free(list_elements);
		duckdb_destroy_logical_type(&child_type);

		return result_list;
	}
	case DUCKDB_TYPE_STRUCT:
	{
		// Handle STRUCT type - return as a map
		idx_t child_count = duckdb_struct_type_child_count(logical_type);

		if (child_count == 0)
		{
			return enif_make_new_map(env);
		}

		ERL_NIF_TERM *keys = enif_alloc(sizeof(ERL_NIF_TERM) * child_count);
		ERL_NIF_TERM *values = enif_alloc(sizeof(ERL_NIF_TERM) * child_count);

		if (!keys || !values)
		{
			if (keys)
				enif_free(keys);
			if (values)
				enif_free(values);
			return atom_nil;
		}

		for (idx_t i = 0; i < child_count; i++)
		{
			// Get child name and type
			char *child_name = duckdb_struct_type_child_name(logical_type, i);
			if (!child_name)
			{
				// Clean up and return error
				for (idx_t j = 0; j < i; j++)
				{
					// Previous iterations may have allocated memory
				}
				enif_free(keys);
				enif_free(values);
				return atom_nil;
			}

			duckdb_logical_type child_type = duckdb_struct_type_child_type(logical_type, i);
			duckdb_vector child_vector = duckdb_struct_vector_get_child(vector, i);

			// Create key
			size_t name_len = strlen(child_name);
			ErlNifBinary name_bin;
			enif_alloc_binary(name_len, &name_bin);
			memcpy(name_bin.data, child_name, name_len);
			keys[i] = enif_make_binary(env, &name_bin);

			// Get value
			values[i] = extract_vector_value(env, child_vector, child_type, row_idx);

			duckdb_free(child_name);
			duckdb_destroy_logical_type(&child_type);
		}

		// Create a map from the key-value pairs
		ERL_NIF_TERM result_map;
		if (enif_make_map_from_arrays(env, keys, values, child_count, &result_map) == 0)
		{
			result_map = enif_make_atom(env, "struct_conversion_failed");
		}

		enif_free(keys);
		enif_free(values);

		return result_map;
	}
	case DUCKDB_TYPE_MAP:
	{
		// Handle MAP type - return as Elixir map
		duckdb_logical_type key_type = duckdb_map_type_key_type(logical_type);
		duckdb_logical_type value_type = duckdb_map_type_value_type(logical_type);

		// Maps in DuckDB are stored as lists of {key, value} structs
		duckdb_vector child_vector = duckdb_list_vector_get_child(vector);
		duckdb_list_entry *list_data = (duckdb_list_entry *)data;
		duckdb_list_entry entry = list_data[row_idx];

		ERL_NIF_TERM *keys = enif_alloc(sizeof(ERL_NIF_TERM) * entry.length);
		ERL_NIF_TERM *values = enif_alloc(sizeof(ERL_NIF_TERM) * entry.length);

		for (idx_t i = 0; i < entry.length; i++)
		{
			// Get key and value vectors from the struct
			duckdb_vector key_vector = duckdb_struct_vector_get_child(child_vector, 0);
			duckdb_vector value_vector = duckdb_struct_vector_get_child(child_vector, 1);

			keys[i] = extract_vector_value(env, key_vector, key_type, entry.offset + i);
			values[i] = extract_vector_value(env, value_vector, value_type, entry.offset + i);
		}

		ERL_NIF_TERM result_map;
		if (enif_make_map_from_arrays(env, keys, values, entry.length, &result_map) == 0)
		{
			result_map = enif_make_atom(env, "map_conversion_failed");
		}

		enif_free(keys);
		enif_free(values);
		duckdb_destroy_logical_type(&key_type);
		duckdb_destroy_logical_type(&value_type);

		return result_map;
	}
	case DUCKDB_TYPE_UNION:
	{
		// Handle UNION type - for now, return as unsupported since the API is complex
		char buffer[64];
		snprintf(buffer, sizeof(buffer), "unsupported_union_type_%d", (int)type_id);
		return enif_make_atom(env, buffer);
	}
	default:
	{
		// For unsupported types, return string representation
		char buffer[256];
		snprintf(buffer, sizeof(buffer), "unsupported_type_%d", (int)type_id);
		return enif_make_atom(env, buffer);
	}
	}
}

// Helper function to extract problematic types robustly using result API
static ERL_NIF_TERM data_chunk_get_data_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	DataChunkResource *chunk_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], data_chunk_resource_type, (void **)&chunk_res))
	{
		return enif_make_badarg(env);
	}

	duckdb_data_chunk chunk = chunk_res->chunk;
	idx_t row_count = duckdb_data_chunk_get_size(chunk);
	idx_t column_count = duckdb_data_chunk_get_column_count(chunk);

	if (row_count == 0)
	{
		return enif_make_list(env, 0);
	}

	ERL_NIF_TERM *rows = enif_alloc(sizeof(ERL_NIF_TERM) * row_count);

	for (idx_t r = 0; r < row_count; r++)
	{
		ERL_NIF_TERM *row_values = enif_alloc(sizeof(ERL_NIF_TERM) * column_count);

		for (idx_t c = 0; c < column_count; c++)
		{
			duckdb_vector vector = duckdb_data_chunk_get_vector(chunk, c);
			duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);

			row_values[c] = extract_vector_value(env, vector, logical_type, r);

			duckdb_destroy_logical_type(&logical_type);
		}

		rows[r] = enif_make_tuple_from_array(env, row_values, column_count);
		enif_free(row_values);
	}

	ERL_NIF_TERM result = enif_make_list_from_array(env, rows, row_count);
	enif_free(rows);
	return result;
}

// Transaction Management Functions
static ERL_NIF_TERM connection_begin_transaction_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConnectionResource *conn_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], connection_resource_type, (void **)&conn_res))
	{
		return enif_make_badarg(env);
	}

	// Execute BEGIN TRANSACTION
	duckdb_result result;
	duckdb_state state = duckdb_query(conn_res->conn, "BEGIN TRANSACTION", &result);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_result_error(&result);
		ERL_NIF_TERM error_term = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
		duckdb_destroy_result(&result);
		return enif_make_tuple2(env, atom_error, error_term);
	}

	duckdb_destroy_result(&result);
	return atom_ok;
}

static ERL_NIF_TERM connection_commit_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConnectionResource *conn_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], connection_resource_type, (void **)&conn_res))
	{
		return enif_make_badarg(env);
	}

	// Execute COMMIT
	duckdb_result result;
	duckdb_state state = duckdb_query(conn_res->conn, "COMMIT", &result);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_result_error(&result);
		ERL_NIF_TERM error_term = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
		duckdb_destroy_result(&result);
		return enif_make_tuple2(env, atom_error, error_term);
	}

	duckdb_destroy_result(&result);
	return atom_ok;
}

static ERL_NIF_TERM connection_rollback_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConnectionResource *conn_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], connection_resource_type, (void **)&conn_res))
	{
		return enif_make_badarg(env);
	}

	// Execute ROLLBACK
	duckdb_result result;
	duckdb_state state = duckdb_query(conn_res->conn, "ROLLBACK", &result);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_result_error(&result);
		ERL_NIF_TERM error_term = enif_make_string(env, error_msg, ERL_NIF_LATIN1);
		duckdb_destroy_result(&result);
		return enif_make_tuple2(env, atom_error, error_term);
	}

	duckdb_destroy_result(&result);
	return atom_ok;
}

//===--------------------------------------------------------------------===//
// Appender Operations
//===--------------------------------------------------------------------===//

static ERL_NIF_TERM appender_create_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConnectionResource *conn_res;
	char schema[256];
	char table[256];
	const char *schema_ptr = NULL;

	if (argc != 3)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], connection_resource_type, (void **)&conn_res))
	{
		return enif_make_badarg(env);
	}

	// Handle schema (can be nil or string/binary)
	if (enif_compare(argv[1], atom_nil) == 0)
	{
		schema_ptr = NULL;
	}
	else
	{
		if (!enif_get_string(env, argv[1], schema, sizeof(schema), ERL_NIF_LATIN1))
		{
			// Try as binary
			ErlNifBinary schema_bin;
			if (enif_inspect_binary(env, argv[1], &schema_bin))
			{
				if (schema_bin.size >= sizeof(schema))
				{
					return enif_make_badarg(env);
				}
				memcpy(schema, schema_bin.data, schema_bin.size);
				schema[schema_bin.size] = '\0';
				schema_ptr = schema;
			}
			else
			{
				return enif_make_badarg(env);
			}
		}
		else
		{
			schema_ptr = schema;
		}
	}

	// Handle table name (string or binary)
	if (!enif_get_string(env, argv[2], table, sizeof(table), ERL_NIF_LATIN1))
	{
		// Check if it's a binary instead
		ErlNifBinary table_bin;
		if (enif_inspect_binary(env, argv[2], &table_bin))
		{
			if (table_bin.size >= sizeof(table))
			{
				return enif_make_badarg(env);
			}
			memcpy(table, table_bin.data, table_bin.size);
			table[table_bin.size] = '\0';
		}
		else
		{
			return enif_make_badarg(env);
		}
	}

	AppenderResource *appender_res =
		(AppenderResource *)enif_alloc_resource(appender_resource_type, sizeof(AppenderResource));
	if (!appender_res)
	{
		return make_error(env, "Failed to allocate appender resource");
	}

	duckdb_state state = duckdb_appender_create(conn_res->conn, schema_ptr, table, &appender_res->appender);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		enif_release_resource(appender_res);
		return make_error(env, error_msg ? error_msg : "Unknown appender creation error");
	}

	ERL_NIF_TERM appender_term = enif_make_resource(env, appender_res);
	enif_release_resource(appender_res);

	return enif_make_tuple2(env, atom_ok, appender_term);
}

static ERL_NIF_TERM appender_create_ext_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	ConnectionResource *conn_res;
	char catalog[256];
	char schema[256];
	char table[256];
	const char *catalog_ptr = NULL;
	const char *schema_ptr = NULL;

	if (argc != 4)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], connection_resource_type, (void **)&conn_res))
	{
		return enif_make_badarg(env);
	}

	// Handle catalog (can be nil or string/binary)
	if (enif_compare(argv[1], atom_nil) == 0)
	{
		catalog_ptr = NULL;
	}
	else
	{
		if (!enif_get_string(env, argv[1], catalog, sizeof(catalog), ERL_NIF_LATIN1))
		{
			// Try as binary
			ErlNifBinary catalog_bin;
			if (enif_inspect_binary(env, argv[1], &catalog_bin))
			{
				if (catalog_bin.size >= sizeof(catalog))
				{
					return enif_make_badarg(env);
				}
				memcpy(catalog, catalog_bin.data, catalog_bin.size);
				catalog[catalog_bin.size] = '\0';
				catalog_ptr = catalog;
			}
			else
			{
				return enif_make_badarg(env);
			}
		}
		else
		{
			catalog_ptr = catalog;
		}
	}

	// Handle schema (can be nil or string/binary)
	if (enif_compare(argv[2], atom_nil) == 0)
	{
		schema_ptr = NULL;
	}
	else
	{
		if (!enif_get_string(env, argv[2], schema, sizeof(schema), ERL_NIF_LATIN1))
		{
			// Try as binary
			ErlNifBinary schema_bin;
			if (enif_inspect_binary(env, argv[2], &schema_bin))
			{
				if (schema_bin.size >= sizeof(schema))
				{
					return enif_make_badarg(env);
				}
				memcpy(schema, schema_bin.data, schema_bin.size);
				schema[schema_bin.size] = '\0';
				schema_ptr = schema;
			}
			else
			{
				return enif_make_badarg(env);
			}
		}
		else
		{
			schema_ptr = schema;
		}
	}

	// Handle table name (string or binary)
	if (!enif_get_string(env, argv[3], table, sizeof(table), ERL_NIF_LATIN1))
	{
		// Check if it's a binary instead
		ErlNifBinary table_bin;
		if (enif_inspect_binary(env, argv[3], &table_bin))
		{
			if (table_bin.size >= sizeof(table))
			{
				return enif_make_badarg(env);
			}
			memcpy(table, table_bin.data, table_bin.size);
			table[table_bin.size] = '\0';
		}
		else
		{
			return enif_make_badarg(env);
		}
	}

	AppenderResource *appender_res =
		(AppenderResource *)enif_alloc_resource(appender_resource_type, sizeof(AppenderResource));
	if (!appender_res)
	{
		return make_error(env, "Failed to allocate appender resource");
	}

	duckdb_state state =
		duckdb_appender_create_ext(conn_res->conn, catalog_ptr, schema_ptr, table, &appender_res->appender);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		enif_release_resource(appender_res);
		return make_error(env, error_msg ? error_msg : "Unknown appender creation error");
	}

	ERL_NIF_TERM appender_term = enif_make_resource(env, appender_res);
	enif_release_resource(appender_res);

	return enif_make_tuple2(env, atom_ok, appender_term);
}

static ERL_NIF_TERM appender_column_count_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	idx_t column_count = duckdb_appender_column_count(appender_res->appender);
	return enif_make_ulong(env, column_count);
}

static ERL_NIF_TERM appender_flush_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_appender_flush(appender_res->appender);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender flush error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_close_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_appender_close(appender_res->appender);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender close error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_destroy_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_appender_destroy(&appender_res->appender);

	if (state == DuckDBError)
	{
		return make_error(env, "Failed to destroy appender");
	}

	// Mark as destroyed
	appender_res->appender = NULL;

	return atom_ok;
}

static ERL_NIF_TERM appender_end_row_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_appender_end_row(appender_res->appender);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender end row error");
	}

	return atom_ok;
}

// Append value functions
static ERL_NIF_TERM appender_append_bool_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	bool value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	// Handle boolean value (true/false atoms)
	ERL_NIF_TERM atom_true = enif_make_atom(env, "true");
	ERL_NIF_TERM atom_false = enif_make_atom(env, "false");

	if (enif_compare(argv[1], atom_true) == 0)
	{
		value = true;
	}
	else if (enif_compare(argv[1], atom_false) == 0)
	{
		value = false;
	}
	else
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_bool(appender_res->appender, value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_int8_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	int value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_int(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	if (value < -128 || value > 127)
	{
		return make_error(env, "Value out of range for int8");
	}

	duckdb_state state = duckdb_append_int8(appender_res->appender, (int8_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_int16_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	int value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_int(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	if (value < -32768 || value > 32767)
	{
		return make_error(env, "Value out of range for int16");
	}

	duckdb_state state = duckdb_append_int16(appender_res->appender, (int16_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_int32_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	int value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_int(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_int32(appender_res->appender, (int32_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_int64_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	ErlNifSInt64 value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_int64(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_int64(appender_res->appender, (int64_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_uint8_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	unsigned int value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_uint(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	if (value > 255)
	{
		return make_error(env, "Value out of range for uint8");
	}

	duckdb_state state = duckdb_append_uint8(appender_res->appender, (uint8_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_uint16_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	unsigned int value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_uint(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	if (value > 65535)
	{
		return make_error(env, "Value out of range for uint16");
	}

	duckdb_state state = duckdb_append_uint16(appender_res->appender, (uint16_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_uint32_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	ErlNifUInt64 value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_uint64(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	if (value > UINT32_MAX)
	{
		return make_error(env, "Value out of range for uint32");
	}

	duckdb_state state = duckdb_append_uint32(appender_res->appender, (uint32_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_uint64_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	ErlNifUInt64 value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_uint64(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_uint64(appender_res->appender, (uint64_t)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_float_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	double value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_double(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_float(appender_res->appender, (float)value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_double_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	double value;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_double(env, argv[1], &value))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_double(appender_res->appender, value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_varchar_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	char value[8192];

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	// Handle varchar (string or binary)
	if (!enif_get_string(env, argv[1], value, sizeof(value), ERL_NIF_LATIN1))
	{
		// Try as binary
		ErlNifBinary value_bin;
		if (enif_inspect_binary(env, argv[1], &value_bin))
		{
			if (value_bin.size >= sizeof(value))
			{
				return enif_make_badarg(env);
			}
			memcpy(value, value_bin.data, value_bin.size);
			value[value_bin.size] = '\0';
		}
		else
		{
			return enif_make_badarg(env);
		}
	}

	duckdb_state state = duckdb_append_varchar(appender_res->appender, value);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_blob_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;
	ErlNifBinary blob;

	if (argc != 2)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	if (!enif_inspect_binary(env, argv[1], &blob))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_blob(appender_res->appender, blob.data, blob.size);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

static ERL_NIF_TERM appender_append_null_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
	AppenderResource *appender_res;

	if (argc != 1)
	{
		return enif_make_badarg(env);
	}

	if (!enif_get_resource(env, argv[0], appender_resource_type, (void **)&appender_res))
	{
		return enif_make_badarg(env);
	}

	duckdb_state state = duckdb_append_null(appender_res->appender);

	if (state == DuckDBError)
	{
		const char *error_msg = duckdb_appender_error(appender_res->appender);
		return make_error(env, error_msg ? error_msg : "Unknown appender append error");
	}

	return atom_ok;
}

// NIF function array
static ErlNifFunc nif_funcs[] = {
	{"database_open", 1, database_open_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
	{"database_open_ext", 2, database_open_ext_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
	{"config_create", 0, config_create_nif, 0},
	{"config_set", 3, config_set_nif, 0},
	{"connection_open", 1, connection_open_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
	{"connection_query", 2, connection_query_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"prepared_statement_prepare", 2, prepared_statement_prepare_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
	{"prepared_statement_execute", 2, prepared_statement_execute_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"result_columns", 1, result_columns_nif, 0},
	{"result_rows", 1, result_rows_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"result_row_count", 1, result_row_count_nif, 0},
	{"result_column_count", 1, result_column_count_nif, 0},
	{"result_chunk_count", 1, result_chunk_count_nif, 0},
	{"result_get_chunk", 2, result_get_chunk_nif, 0},
	{"data_chunk_get_data", 1, data_chunk_get_data_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"connection_begin_transaction", 1, connection_begin_transaction_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"connection_commit", 1, connection_commit_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"connection_rollback", 1, connection_rollback_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_create", 3, appender_create_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
	{"appender_create_ext", 4, appender_create_ext_nif, ERL_NIF_DIRTY_JOB_IO_BOUND},
	{"appender_column_count", 1, appender_column_count_nif, 0},
	{"appender_flush", 1, appender_flush_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_close", 1, appender_close_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_destroy", 1, appender_destroy_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_end_row", 1, appender_end_row_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_bool", 2, appender_append_bool_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_int8", 2, appender_append_int8_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_int16", 2, appender_append_int16_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_int32", 2, appender_append_int32_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_int64", 2, appender_append_int64_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_uint8", 2, appender_append_uint8_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_uint16", 2, appender_append_uint16_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_uint32", 2, appender_append_uint32_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_uint64", 2, appender_append_uint64_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_float", 2, appender_append_float_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_double", 2, appender_append_double_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_varchar", 2, appender_append_varchar_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_blob", 2, appender_append_blob_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},
	{"appender_append_null", 1, appender_append_null_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND}};

// Module initialization
static int load(ErlNifEnv *env, void **priv_data, ERL_NIF_TERM load_info)
{
	// Create resource types
	database_resource_type = enif_open_resource_type(env, NULL, "database_resource", database_resource_destructor,
													 ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

	connection_resource_type = enif_open_resource_type(env, NULL, "connection_resource", connection_resource_destructor,
													   ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

	result_resource_type = enif_open_resource_type(env, NULL, "result_resource", result_resource_destructor,
												   ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

	prepared_statement_resource_type =
		enif_open_resource_type(env, NULL, "prepared_statement_resource", prepared_statement_resource_destructor,
								ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

	data_chunk_resource_type = enif_open_resource_type(env, NULL, "data_chunk_resource", data_chunk_resource_destructor,
													   ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);

	appender_resource_type = enif_open_resource_type(env, NULL, "appender_resource", appender_resource_destructor,
													 ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
	if (!appender_resource_type)
	{
		return -1;
	}

	config_resource_type = enif_open_resource_type(env, NULL, "config_resource", config_resource_destructor,
												   ERL_NIF_RT_CREATE | ERL_NIF_RT_TAKEOVER, NULL);
	if (!config_resource_type)
	{
		return -1;
	}

	// Initialize atoms
	atom_ok = enif_make_atom(env, "ok");
	atom_error = enif_make_atom(env, "error");
	atom_nil = enif_make_atom(env, "nil");
	atom_memory = enif_make_atom(env, "memory");

	// Type atoms
	atom_boolean = enif_make_atom(env, "boolean");
	atom_tinyint = enif_make_atom(env, "tinyint");
	atom_smallint = enif_make_atom(env, "smallint");
	atom_integer = enif_make_atom(env, "integer");
	atom_bigint = enif_make_atom(env, "bigint");
	atom_utinyint = enif_make_atom(env, "utinyint");
	atom_usmallint = enif_make_atom(env, "usmallint");
	atom_uinteger = enif_make_atom(env, "uinteger");
	atom_ubigint = enif_make_atom(env, "ubigint");
	atom_float = enif_make_atom(env, "float");
	atom_double = enif_make_atom(env, "double");
	atom_varchar = enif_make_atom(env, "varchar");
	atom_blob = enif_make_atom(env, "blob");
	atom_date = enif_make_atom(env, "date");
	atom_time = enif_make_atom(env, "time");
	atom_timestamp = enif_make_atom(env, "timestamp");
	atom_interval = enif_make_atom(env, "interval");
	atom_hugeint = enif_make_atom(env, "hugeint");
	atom_uhugeint = enif_make_atom(env, "uhugeint");
	atom_list = enif_make_atom(env, "list");
	atom_array = enif_make_atom(env, "array");
	atom_struct = enif_make_atom(env, "struct");
	atom_map = enif_make_atom(env, "map");
	atom_union = enif_make_atom(env, "union");
	atom_decimal = enif_make_atom(env, "decimal");
	atom_enum = enif_make_atom(env, "enum");
	atom_uuid = enif_make_atom(env, "uuid");
	atom_bit = enif_make_atom(env, "bit");
	atom_time_tz = enif_make_atom(env, "time_tz");
	atom_timestamp_s = enif_make_atom(env, "timestamp_s");
	atom_timestamp_ms = enif_make_atom(env, "timestamp_ms");
	atom_timestamp_ns = enif_make_atom(env, "timestamp_ns");
	atom_timestamp_tz = enif_make_atom(env, "timestamp_tz");
	atom_unknown = enif_make_atom(env, "unknown");

	return 0;
}

ERL_NIF_INIT(Elixir.DuckdbEx.Nif, nif_funcs, load, NULL, NULL, NULL);
