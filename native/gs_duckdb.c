#include <math.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <duckdb.h>

#define GS_DUCK_MAX_SLOTS 512

#define GS_COL_BOOL   1
#define GS_COL_INT64  2
#define GS_COL_FLOAT  3
#define GS_COL_STRING 4

typedef struct {
    char *name;
    int32_t kind;
    size_t len;
    size_t cap;
    uint8_t *nulls;
    int64_t *i64s;
    double *f64s;
    char **strs;
} gs_duck_col_t;

typedef struct {
    int32_t col_count;
    int64_t row_count;
    gs_duck_col_t *cols;
} gs_duck_res_t;

static pthread_mutex_t g_duck_mu = PTHREAD_MUTEX_INITIALIZER;
static duckdb_database g_dbs[GS_DUCK_MAX_SLOTS];
static duckdb_connection g_conns[GS_DUCK_MAX_SLOTS];
static duckdb_prepared_statement g_stmts[GS_DUCK_MAX_SLOTS];
static gs_duck_res_t *g_res[GS_DUCK_MAX_SLOTS];
static _Thread_local char g_duck_err[1024];

static void gs_duck_set_err(const char *msg)
{
    if (msg == NULL) {
        g_duck_err[0] = '\0';
        return;
    }
    snprintf(g_duck_err, sizeof(g_duck_err), "%s", msg);
}

const char *gs_duck_last_error(void)
{
    return g_duck_err;
}

static int64_t alloc_db_slot(duckdb_database db)
{
    pthread_mutex_lock(&g_duck_mu);
    for (int i = 1; i < GS_DUCK_MAX_SLOTS; i++) {
        if (g_dbs[i] == NULL) {
            g_dbs[i] = db;
            pthread_mutex_unlock(&g_duck_mu);
            return (int64_t)i;
        }
    }
    pthread_mutex_unlock(&g_duck_mu);
    return -1;
}

static int64_t alloc_conn_slot(duckdb_connection conn)
{
    pthread_mutex_lock(&g_duck_mu);
    for (int i = 1; i < GS_DUCK_MAX_SLOTS; i++) {
        if (g_conns[i] == NULL) {
            g_conns[i] = conn;
            pthread_mutex_unlock(&g_duck_mu);
            return (int64_t)i;
        }
    }
    pthread_mutex_unlock(&g_duck_mu);
    return -1;
}

static int64_t alloc_stmt_slot(duckdb_prepared_statement stmt)
{
    pthread_mutex_lock(&g_duck_mu);
    for (int i = 1; i < GS_DUCK_MAX_SLOTS; i++) {
        if (g_stmts[i] == NULL) {
            g_stmts[i] = stmt;
            pthread_mutex_unlock(&g_duck_mu);
            return (int64_t)i;
        }
    }
    pthread_mutex_unlock(&g_duck_mu);
    return -1;
}

static int64_t alloc_res_slot(gs_duck_res_t *r)
{
    pthread_mutex_lock(&g_duck_mu);
    for (int i = 1; i < GS_DUCK_MAX_SLOTS; i++) {
        if (g_res[i] == NULL) {
            g_res[i] = r;
            pthread_mutex_unlock(&g_duck_mu);
            return (int64_t)i;
        }
    }
    pthread_mutex_unlock(&g_duck_mu);
    return -1;
}

static int32_t map_duck_type(duckdb_type dt)
{
    switch (dt) {
        case DUCKDB_TYPE_BOOLEAN:
            return GS_COL_BOOL;
        case DUCKDB_TYPE_TINYINT:
        case DUCKDB_TYPE_SMALLINT:
        case DUCKDB_TYPE_INTEGER:
        case DUCKDB_TYPE_BIGINT:
        case DUCKDB_TYPE_UTINYINT:
        case DUCKDB_TYPE_USMALLINT:
        case DUCKDB_TYPE_UINTEGER:
        case DUCKDB_TYPE_UBIGINT:
        case DUCKDB_TYPE_HUGEINT:
        case DUCKDB_TYPE_DATE:
        case DUCKDB_TYPE_TIMESTAMP:
        case DUCKDB_TYPE_TIMESTAMP_S:
        case DUCKDB_TYPE_TIMESTAMP_MS:
        case DUCKDB_TYPE_TIMESTAMP_NS:
            return GS_COL_INT64;
        case DUCKDB_TYPE_FLOAT:
        case DUCKDB_TYPE_DOUBLE:
        case DUCKDB_TYPE_DECIMAL:
            return GS_COL_FLOAT;
        default:
            return GS_COL_STRING;
    }
}

static void gs_duck_col_free(gs_duck_col_t *c)
{
    free(c->name);
    free(c->nulls);
    free(c->i64s);
    free(c->f64s);
    if (c->strs != NULL) {
        for (size_t i = 0; i < c->len; i++) {
            free(c->strs[i]);
        }
        free(c->strs);
    }
}

static void gs_duck_res_free(gs_duck_res_t *r)
{
    if (r == NULL) return;
    if (r->cols != NULL) {
        for (int32_t i = 0; i < r->col_count; i++) {
            gs_duck_col_free(&r->cols[i]);
        }
        free(r->cols);
    }
    free(r);
}

static int gs_duck_col_reserve(gs_duck_col_t *c, size_t need)
{
    if (need <= c->cap) return 0;
    size_t new_cap = c->cap == 0 ? 256 : c->cap;
    while (new_cap < need) new_cap *= 2;

    uint8_t *nn = (uint8_t *)realloc(c->nulls, new_cap * sizeof(uint8_t));
    if (nn == NULL) return -1;
    c->nulls = nn;

    if (c->kind == GS_COL_BOOL || c->kind == GS_COL_INT64) {
        int64_t *ni = (int64_t *)realloc(c->i64s, new_cap * sizeof(int64_t));
        if (ni == NULL) return -1;
        c->i64s = ni;
    } else if (c->kind == GS_COL_FLOAT) {
        double *nf = (double *)realloc(c->f64s, new_cap * sizeof(double));
        if (nf == NULL) return -1;
        c->f64s = nf;
    } else {
        char **ns = (char **)realloc(c->strs, new_cap * sizeof(char *));
        if (ns == NULL) return -1;
        c->strs = ns;
    }
    c->cap = new_cap;
    return 0;
}

static char *dup_n(const char *s, size_t n)
{
    char *out = (char *)malloc(n + 1);
    if (out == NULL) return NULL;
    if (n > 0) memcpy(out, s, n);
    out[n] = '\0';
    return out;
}

static int64_t materialize_result(duckdb_result *raw)
{
    idx_t ncols = duckdb_column_count(raw);
    gs_duck_res_t *r = (gs_duck_res_t *)calloc(1, sizeof(gs_duck_res_t));
    if (r == NULL) {
        gs_duck_set_err("out of memory");
        return -1;
    }
    r->col_count = (int32_t)ncols;
    r->row_count = 0;

    duckdb_type *raw_types = NULL;
    uint8_t *dec_scales = NULL;
    duckdb_type *dec_internal = NULL;

    if (ncols > 0) {
        r->cols = (gs_duck_col_t *)calloc((size_t)ncols, sizeof(gs_duck_col_t));
        raw_types = (duckdb_type *)calloc((size_t)ncols, sizeof(duckdb_type));
        dec_scales = (uint8_t *)calloc((size_t)ncols, sizeof(uint8_t));
        dec_internal = (duckdb_type *)calloc((size_t)ncols, sizeof(duckdb_type));
        if (!r->cols || !raw_types || !dec_scales || !dec_internal) {
            free(raw_types);
            free(dec_scales);
            free(dec_internal);
            gs_duck_res_free(r);
            gs_duck_set_err("out of memory");
            return -1;
        }
        for (idx_t c = 0; c < ncols; c++) {
            const char *nm = duckdb_column_name(raw, c);
            r->cols[c].name = dup_n(nm ? nm : "", nm ? strlen(nm) : 0);
            raw_types[c] = duckdb_column_type(raw, c);
            r->cols[c].kind = map_duck_type(raw_types[c]);
            if (raw_types[c] == DUCKDB_TYPE_DECIMAL) {
                duckdb_logical_type lt = duckdb_column_logical_type(raw, c);
                dec_scales[c] = duckdb_decimal_scale(lt);
                dec_internal[c] = duckdb_decimal_internal_type(lt);
                duckdb_destroy_logical_type(&lt);
            }
        }
    }

    for (;;) {
        duckdb_data_chunk chunk = duckdb_fetch_chunk(*raw);
        if (chunk == NULL) break;
        idx_t nrows = duckdb_data_chunk_get_size(chunk);
        if (nrows == 0) {
            duckdb_destroy_data_chunk(&chunk);
            break;
        }

        for (idx_t c = 0; c < ncols; c++) {
            gs_duck_col_t *col = &r->cols[c];
            if (gs_duck_col_reserve(col, col->len + (size_t)nrows) != 0) {
                duckdb_destroy_data_chunk(&chunk);
                free(raw_types);
                free(dec_scales);
                free(dec_internal);
                gs_duck_res_free(r);
                gs_duck_set_err("out of memory while materializing chunk");
                return -1;
            }
            duckdb_vector vec = duckdb_data_chunk_get_vector(chunk, c);
            void *data = duckdb_vector_get_data(vec);
            uint64_t *validity = duckdb_vector_get_validity(vec);
            duckdb_type dt = raw_types[c];

            for (idx_t row = 0; row < nrows; row++) {
                size_t dst = col->len + (size_t)row;
                bool valid = duckdb_validity_row_is_valid(validity, row);
                col->nulls[dst] = valid ? 0 : 1;

                if (!valid) {
                    if (col->kind == GS_COL_BOOL || col->kind == GS_COL_INT64) col->i64s[dst] = 0;
                    else if (col->kind == GS_COL_FLOAT) col->f64s[dst] = NAN;
                    else col->strs[dst] = dup_n("", 0);
                    continue;
                }

                if (col->kind == GS_COL_BOOL) {
                    col->i64s[dst] = ((const bool *)data)[row] ? 1 : 0;
                } else if (col->kind == GS_COL_INT64) {
                    int64_t v = 0;
                    switch (dt) {
                        case DUCKDB_TYPE_TINYINT:  v = (int64_t)((const int8_t *)data)[row]; break;
                        case DUCKDB_TYPE_SMALLINT: v = (int64_t)((const int16_t *)data)[row]; break;
                        case DUCKDB_TYPE_INTEGER:
                        case DUCKDB_TYPE_DATE:     v = (int64_t)((const int32_t *)data)[row]; break;
                        case DUCKDB_TYPE_BIGINT:
                        case DUCKDB_TYPE_TIMESTAMP:
                        case DUCKDB_TYPE_TIMESTAMP_S:
                        case DUCKDB_TYPE_TIMESTAMP_MS:
                        case DUCKDB_TYPE_TIMESTAMP_NS:
                            v = ((const int64_t *)data)[row]; break;
                        case DUCKDB_TYPE_UTINYINT:  v = (int64_t)((const uint8_t *)data)[row]; break;
                        case DUCKDB_TYPE_USMALLINT: v = (int64_t)((const uint16_t *)data)[row]; break;
                        case DUCKDB_TYPE_UINTEGER:  v = (int64_t)((const uint32_t *)data)[row]; break;
                        case DUCKDB_TYPE_UBIGINT:   v = (int64_t)((const uint64_t *)data)[row]; break;
                        case DUCKDB_TYPE_HUGEINT: {
                            duckdb_hugeint hi = ((const duckdb_hugeint *)data)[row];
                            v = (int64_t)hi.lower;
                            break;
                        }
                        default: v = 0; break;
                    }
                    col->i64s[dst] = v;
                } else if (col->kind == GS_COL_FLOAT) {
                    double dv = 0.0;
                    if (dt == DUCKDB_TYPE_FLOAT) {
                        dv = (double)((const float *)data)[row];
                    } else if (dt == DUCKDB_TYPE_DOUBLE) {
                        dv = ((const double *)data)[row];
                    } else if (dt == DUCKDB_TYPE_DECIMAL) {
                        double scale = pow(10.0, (double)dec_scales[c]);
                        switch (dec_internal[c]) {
                            case DUCKDB_TYPE_SMALLINT: dv = (double)((const int16_t *)data)[row] / scale; break;
                            case DUCKDB_TYPE_INTEGER:  dv = (double)((const int32_t *)data)[row] / scale; break;
                            case DUCKDB_TYPE_BIGINT:   dv = (double)((const int64_t *)data)[row] / scale; break;
                            case DUCKDB_TYPE_HUGEINT: {
                                duckdb_hugeint hi = ((const duckdb_hugeint *)data)[row];
                                dv = duckdb_hugeint_to_double(hi) / scale;
                                break;
                            }
                            default: dv = 0.0; break;
                        }
                    }
                    col->f64s[dst] = dv;
                } else {
                    if (dt == DUCKDB_TYPE_VARCHAR) {
                        duckdb_string_t st = ((duckdb_string_t *)data)[row];
                        uint32_t slen = duckdb_string_t_length(st);
                        const char *sptr = duckdb_string_t_data(&st);
                        col->strs[dst] = dup_n(sptr ? sptr : "", (size_t)slen);
                    } else {
                        col->strs[dst] = dup_n("", 0);
                    }
                }
            }
            col->len += (size_t)nrows;
        }
        r->row_count += (int64_t)nrows;
        duckdb_destroy_data_chunk(&chunk);
    }

    free(raw_types);
    free(dec_scales);
    free(dec_internal);

    int64_t h = alloc_res_slot(r);
    if (h < 0) {
        gs_duck_res_free(r);
        gs_duck_set_err("too many open DuckDB results");
        return -1;
    }
    return h;
}

int64_t gs_duck_open(const char *path)
{
    g_duck_err[0] = '\0';
    const char *p = (path == NULL || path[0] == '\0' || strcmp(path, ":memory:") == 0) ? NULL : path;
    duckdb_database db = NULL;
    char *err = NULL;
    if (duckdb_open_ext(p, &db, NULL, &err) == DuckDBError) {
        gs_duck_set_err(err ? err : "duckdb_open_ext failed");
        if (err) duckdb_free(err);
        return -1;
    }
    int64_t h = alloc_db_slot(db);
    if (h < 0) {
        duckdb_close(&db);
        gs_duck_set_err("too many open DuckDB databases");
        return -1;
    }
    return h;
}

int32_t gs_duck_close(int64_t db_h)
{
    if (db_h <= 0 || db_h >= GS_DUCK_MAX_SLOTS) return 0;
    pthread_mutex_lock(&g_duck_mu);
    duckdb_database db = g_dbs[db_h];
    g_dbs[db_h] = NULL;
    pthread_mutex_unlock(&g_duck_mu);
    if (db != NULL) duckdb_close(&db);
    return 0;
}

int64_t gs_duck_connect(int64_t db_h)
{
    if (db_h <= 0 || db_h >= GS_DUCK_MAX_SLOTS) {
        gs_duck_set_err("invalid DuckDB database handle");
        return -1;
    }
    pthread_mutex_lock(&g_duck_mu);
    duckdb_database db = g_dbs[db_h];
    pthread_mutex_unlock(&g_duck_mu);
    if (db == NULL) {
        gs_duck_set_err("DuckDB database is closed");
        return -1;
    }
    duckdb_connection conn = NULL;
    if (duckdb_connect(db, &conn) == DuckDBError) {
        gs_duck_set_err("duckdb_connect failed");
        return -1;
    }
    int64_t h = alloc_conn_slot(conn);
    if (h < 0) {
        duckdb_disconnect(&conn);
        gs_duck_set_err("too many open DuckDB connections");
        return -1;
    }
    return h;
}

int32_t gs_duck_disconnect(int64_t conn_h)
{
    if (conn_h <= 0 || conn_h >= GS_DUCK_MAX_SLOTS) return 0;
    pthread_mutex_lock(&g_duck_mu);
    duckdb_connection conn = g_conns[conn_h];
    g_conns[conn_h] = NULL;
    pthread_mutex_unlock(&g_duck_mu);
    if (conn != NULL) duckdb_disconnect(&conn);
    return 0;
}

static duckdb_connection get_conn(int64_t conn_h)
{
    if (conn_h <= 0 || conn_h >= GS_DUCK_MAX_SLOTS) return NULL;
    pthread_mutex_lock(&g_duck_mu);
    duckdb_connection conn = g_conns[conn_h];
    pthread_mutex_unlock(&g_duck_mu);
    return conn;
}

static duckdb_prepared_statement get_stmt(int64_t stmt_h)
{
    if (stmt_h <= 0 || stmt_h >= GS_DUCK_MAX_SLOTS) return NULL;
    pthread_mutex_lock(&g_duck_mu);
    duckdb_prepared_statement st = g_stmts[stmt_h];
    pthread_mutex_unlock(&g_duck_mu);
    return st;
}

static gs_duck_res_t *get_res(int64_t res_h)
{
    if (res_h <= 0 || res_h >= GS_DUCK_MAX_SLOTS) return NULL;
    pthread_mutex_lock(&g_duck_mu);
    gs_duck_res_t *r = g_res[res_h];
    pthread_mutex_unlock(&g_duck_mu);
    return r;
}

int64_t gs_duck_query(int64_t conn_h, const char *sql)
{
    duckdb_connection conn = get_conn(conn_h);
    if (conn == NULL || sql == NULL) {
        gs_duck_set_err("invalid DuckDB connection or SQL");
        return -1;
    }
    duckdb_result raw;
    if (duckdb_query(conn, sql, &raw) == DuckDBError) {
        const char *err = duckdb_result_error(&raw);
        gs_duck_set_err(err ? err : "DuckDB query failed");
        duckdb_destroy_result(&raw);
        return -1;
    }
    int64_t h = materialize_result(&raw);
    duckdb_destroy_result(&raw);
    return h;
}

int64_t gs_duck_prepare(int64_t conn_h, const char *sql)
{
    duckdb_connection conn = get_conn(conn_h);
    if (conn == NULL || sql == NULL) {
        gs_duck_set_err("invalid DuckDB connection or SQL");
        return -1;
    }
    duckdb_prepared_statement stmt = NULL;
    if (duckdb_prepare(conn, sql, &stmt) == DuckDBError) {
        const char *err = stmt ? duckdb_prepare_error(stmt) : NULL;
        gs_duck_set_err(err ? err : "duckdb_prepare failed");
        if (stmt) duckdb_destroy_prepare(&stmt);
        return -1;
    }
    int64_t h = alloc_stmt_slot(stmt);
    if (h < 0) {
        duckdb_destroy_prepare(&stmt);
        gs_duck_set_err("too many open DuckDB prepared statements");
        return -1;
    }
    return h;
}

int32_t gs_duck_bind_i64(int64_t stmt_h, int32_t idx, int64_t val)
{
    duckdb_prepared_statement stmt = get_stmt(stmt_h);
    if (stmt == NULL) return -1;
    return duckdb_bind_int64(stmt, (idx_t)idx, val) == DuckDBSuccess ? 0 : -1;
}

int32_t gs_duck_bind_f64(int64_t stmt_h, int32_t idx, double val)
{
    duckdb_prepared_statement stmt = get_stmt(stmt_h);
    if (stmt == NULL) return -1;
    return duckdb_bind_double(stmt, (idx_t)idx, val) == DuckDBSuccess ? 0 : -1;
}

int32_t gs_duck_bind_str(int64_t stmt_h, int32_t idx, const char *val)
{
    duckdb_prepared_statement stmt = get_stmt(stmt_h);
    if (stmt == NULL || val == NULL) return -1;
    return duckdb_bind_varchar(stmt, (idx_t)idx, val) == DuckDBSuccess ? 0 : -1;
}

int32_t gs_duck_bind_bool(int64_t stmt_h, int32_t idx, int32_t val)
{
    duckdb_prepared_statement stmt = get_stmt(stmt_h);
    if (stmt == NULL) return -1;
    return duckdb_bind_boolean(stmt, (idx_t)idx, val != 0) == DuckDBSuccess ? 0 : -1;
}

int32_t gs_duck_bind_null(int64_t stmt_h, int32_t idx)
{
    duckdb_prepared_statement stmt = get_stmt(stmt_h);
    if (stmt == NULL) return -1;
    return duckdb_bind_null(stmt, (idx_t)idx) == DuckDBSuccess ? 0 : -1;
}

int64_t gs_duck_exec_prepared(int64_t stmt_h)
{
    duckdb_prepared_statement stmt = get_stmt(stmt_h);
    if (stmt == NULL) {
        gs_duck_set_err("invalid DuckDB prepared statement handle");
        return -1;
    }
    duckdb_result raw;
    if (duckdb_execute_prepared(stmt, &raw) == DuckDBError) {
        const char *err = duckdb_result_error(&raw);
        gs_duck_set_err(err ? err : "DuckDB prepared execution failed");
        duckdb_destroy_result(&raw);
        return -1;
    }
    int64_t h = materialize_result(&raw);
    duckdb_destroy_result(&raw);
    return h;
}

int32_t gs_duck_destroy_prepare(int64_t stmt_h)
{
    if (stmt_h <= 0 || stmt_h >= GS_DUCK_MAX_SLOTS) return 0;
    pthread_mutex_lock(&g_duck_mu);
    duckdb_prepared_statement stmt = g_stmts[stmt_h];
    g_stmts[stmt_h] = NULL;
    pthread_mutex_unlock(&g_duck_mu);
    if (stmt != NULL) duckdb_destroy_prepare(&stmt);
    return 0;
}

int32_t gs_duck_res_close(int64_t res_h)
{
    if (res_h <= 0 || res_h >= GS_DUCK_MAX_SLOTS) return 0;
    pthread_mutex_lock(&g_duck_mu);
    gs_duck_res_t *r = g_res[res_h];
    g_res[res_h] = NULL;
    pthread_mutex_unlock(&g_duck_mu);
    gs_duck_res_free(r);
    return 0;
}

int64_t gs_duck_res_row_count(int64_t res_h)
{
    gs_duck_res_t *r = get_res(res_h);
    return r ? r->row_count : 0;
}

int32_t gs_duck_res_col_count(int64_t res_h)
{
    gs_duck_res_t *r = get_res(res_h);
    return r ? r->col_count : 0;
}

const char *gs_duck_res_col_name(int64_t res_h, int32_t col)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || col < 0 || col >= r->col_count) return "";
    return r->cols[col].name ? r->cols[col].name : "";
}

int32_t gs_duck_res_col_kind(int64_t res_h, int32_t col)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || col < 0 || col >= r->col_count) return 0;
    return r->cols[col].kind;
}

int32_t gs_duck_res_is_null(int64_t res_h, int32_t col, int64_t row)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || col < 0 || col >= r->col_count || row < 0 || row >= r->row_count) return 1;
    return r->cols[col].nulls[row] ? 1 : 0;
}

int64_t gs_duck_res_get_i64(int64_t res_h, int32_t col, int64_t row)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || col < 0 || col >= r->col_count || row < 0 || row >= r->row_count) return 0;
    gs_duck_col_t *c = &r->cols[col];
    if (c->nulls[row]) return 0;
    if (c->kind == GS_COL_BOOL || c->kind == GS_COL_INT64) return c->i64s[row];
    if (c->kind == GS_COL_FLOAT) return (int64_t)c->f64s[row];
    return 0;
}

double gs_duck_res_get_f64(int64_t res_h, int32_t col, int64_t row)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || col < 0 || col >= r->col_count || row < 0 || row >= r->row_count) return NAN;
    gs_duck_col_t *c = &r->cols[col];
    if (c->nulls[row]) return NAN;
    if (c->kind == GS_COL_FLOAT) return c->f64s[row];
    if (c->kind == GS_COL_BOOL || c->kind == GS_COL_INT64) return (double)c->i64s[row];
    return NAN;
}

const char *gs_duck_res_get_str(int64_t res_h, int32_t col, int64_t row)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || col < 0 || col >= r->col_count || row < 0 || row >= r->row_count) return "";
    gs_duck_col_t *c = &r->cols[col];
    if (c->nulls[row]) return "";
    if (c->kind == GS_COL_STRING && c->strs[row] != NULL) return c->strs[row];
    return "";
}

// Fast bulk copy into caller's `double[max_rows]` buffer (NULL -> NaN).
int64_t gs_duck_res_copy_f64(int64_t res_h, int32_t col, void *dst_ptr, int64_t max_rows)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || !dst_ptr || col < 0 || col >= r->col_count || max_rows <= 0) return 0;
    gs_duck_col_t *c = &r->cols[col];
    int64_t n = r->row_count < max_rows ? r->row_count : max_rows;
    double *dst = (double *)dst_ptr;
    if (c->kind == GS_COL_FLOAT) {
        memcpy(dst, c->f64s, (size_t)n * sizeof(double));
    } else if (c->kind == GS_COL_INT64 || c->kind == GS_COL_BOOL) {
        for (int64_t i = 0; i < n; i++) {
            dst[i] = c->nulls[i] ? NAN : (double)c->i64s[i];
        }
    } else {
        for (int64_t i = 0; i < n; i++) dst[i] = NAN;
    }
    return n;
}

int64_t gs_duck_res_copy_i64(int64_t res_h, int32_t col, void *dst_ptr, int64_t max_rows)
{
    gs_duck_res_t *r = get_res(res_h);
    if (!r || !dst_ptr || col < 0 || col >= r->col_count || max_rows <= 0) return 0;
    gs_duck_col_t *c = &r->cols[col];
    int64_t n = r->row_count < max_rows ? r->row_count : max_rows;
    int64_t *dst = (int64_t *)dst_ptr;
    if (c->kind == GS_COL_INT64 || c->kind == GS_COL_BOOL) {
        memcpy(dst, c->i64s, (size_t)n * sizeof(int64_t));
    } else if (c->kind == GS_COL_FLOAT) {
        for (int64_t i = 0; i < n; i++) {
            dst[i] = c->nulls[i] ? 0 : (int64_t)c->f64s[i];
        }
    } else {
        memset(dst, 0, (size_t)n * sizeof(int64_t));
    }
    return n;
}
