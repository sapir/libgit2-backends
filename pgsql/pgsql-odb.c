/*
 * This file is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2,
 * as published by the Free Software Foundation.
 *
 * In addition to the permissions in the GNU General Public License,
 * the authors give you unlimited permission to link the compiled
 * version of this file into combinations with other programs,
 * and to distribute those combinations without any restriction
 * coming from the use of this file.  (The General Public License
 * restrictions do apply in other respects; for example, they cover
 * modification of the file, and distribution when not linked into
 * a combined executable.)
 *
 * This file is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; see the file COPYING.  If not, write to
 * the Free Software Foundation, 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301, USA.
 */

#include <assert.h>
#include <string.h>
#include <libpq-fe.h>
#include <endian.h>
#include <git2.h>
#include <git2/sys/odb_backend.h>
#include "helpers.h"


#define GIT2_TABLE_NAME "git2_odb"
#define GIT2_PK_NAME "git2_odb_pkey"
#define GIT2_TYPE_IDX_NAME "git2_odb_idx_type"


typedef struct {
    git_odb_backend parent;
    PGconn *db;
} pgsql_odb_backend;


static void set_giterr_from_pg(pgsql_odb_backend *backend)
{
    giterr_set_str(GITERR_ODB, PQerrorMessage(backend->db));
}

static void pgsql_odb_backend__free(git_odb_backend *_backend)
{
    pgsql_odb_backend *backend = (pgsql_odb_backend*)_backend;
    assert(backend);

    PQfinish(backend->db);
    free(backend);
}

static PGresult *exec_read_stmt(pgsql_odb_backend *backend, const char *stmt_name,
    const git_oid *oid)
{
    const char * const param_values[1] = {oid->id};
    int param_lengths[1] = {GIT_OID_RAWSZ};
    int param_formats[1] = {1};     /* binary */
    return PQexecPrepared(backend->db, stmt_name,
        1, param_values, param_lengths, param_formats,
        /* binary result */ 1);
}


static int pgsql_odb_backend__read_header(size_t *len_p, git_otype *type_p,
    git_odb_backend *_backend, const git_oid *oid)
{
    pgsql_odb_backend *backend = (pgsql_odb_backend*)_backend;
    PGresult *result;
    int error = GIT_ERROR;

    assert(len_p && type_p && backend && oid);

    result = exec_read_stmt(backend, "read", oid);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        error = GIT_ERROR;
        set_giterr_from_pg(backend);
        goto cleanup;
    }

    if (PQntuples(result) == 0) {
        error = GIT_ENOTFOUND;
        goto cleanup;
    }

    if (get_int_from_result(result, type_p, 0, 0)
        || get_int_from_result(result, len_p, 0, 1)) {
        error = GIT_ERROR;
        /* error string already set by function call */
        goto cleanup;
    }

    error = GIT_OK;

cleanup:
    PQclear(result);
    return error;
}

static int pgsql_odb_backend__read(void **data_p, size_t *len_p, git_otype *type_p,
    git_odb_backend *_backend, const git_oid *oid)
{
    pgsql_odb_backend *backend = (pgsql_odb_backend*)_backend;
    PGresult *result;
    int error = GIT_ERROR;
    int value_len;

    assert(len_p && type_p && backend && oid);

    result = exec_read_stmt(backend, "read", oid);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        error = GIT_ERROR;
        set_giterr_from_pg(backend);
        goto cleanup;
    }

    if (PQntuples(result) == 0) {
        error = GIT_ENOTFOUND;
        goto cleanup;
    }

    if (get_int_from_result(result, type_p, 0, 0)) {
        error = GIT_ERROR;
        /* error string already set by function call */
        goto cleanup;
    }

    *len_p = PQgetlength(result, 0, 1);
    if (*len_p > 0) {
        *data_p = git_odb_backend_malloc(_backend, *len_p);
        memcpy(*data_p, PQgetvalue(result, 0, 1), *len_p);
    }

    error = GIT_OK;

cleanup:
    PQclear(result);
    return error;
}

static int pgsql_odb_backend__exists(git_odb_backend *_backend, const git_oid *oid)
{
    pgsql_odb_backend *backend = (pgsql_odb_backend*)_backend;
    PGresult *result;
    int found = 0;

    assert(backend && oid);

    result = exec_read_stmt(backend, "read", oid);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        goto cleanup;
    }

    if (PQntuples(result) > 0) {
        found = 1;
    }

cleanup:
    PQclear(result);
    return found;
}

static int pgsql_odb_backend__write(git_odb_backend *_backend,
    const git_oid *oid, const void *data, size_t len, git_otype type)
{
    pgsql_odb_backend *backend = (pgsql_odb_backend*)_backend;
    PGresult *result;
    uint32_t fmtd_type = htobe32(type);
    const char * const param_values[3] = {
        oid->id,
        (const char*)&fmtd_type,
        data};
    int param_lengths[3] = {GIT_OID_RAWSZ, sizeof(fmtd_type), len};
    int param_formats[3] = {1, 1, 1};     /* binary */

    assert(data && backend && oid);

    result = PQexecPrepared(backend->db, "write",
        3, param_values, param_lengths, param_formats,
        /* binary result */ 1);
    if (complete_pq_exec(result)) {
        set_giterr_from_pg(backend);
        return GIT_ERROR;
    }

    return GIT_OK;
}

static int init_db(PGconn *db)
{
    PGresult *result;

    result = PQexec(db,
        /* run as plpgsql so if statement works */
        "DO $BODY$ BEGIN "

        "CREATE TABLE IF NOT EXISTS \"" GIT2_TABLE_NAME "\" ("
        "  \"oid\" bytea NOT NULL DEFAULT '',"
        "  \"type\" int NOT NULL,"
        "  \"data\" bytea NOT NULL,"
        "  CONSTRAINT \"" GIT2_PK_NAME "\" PRIMARY KEY (\"oid\")"
        ");"

        "IF NOT EXISTS("
        "  select 1 from pg_index, pg_class"
        "  where pg_index.indexrelid = pg_class.oid"
        "    and pg_class.relname = '" GIT2_TYPE_IDX_NAME "'"
        ")"
        "THEN"
        "  CREATE INDEX \"" GIT2_TYPE_IDX_NAME "\""
        "    ON \"" GIT2_TABLE_NAME "\""
        "    (\"type\");"
        "END IF;"

        /* end plpgsql statement */
        "END; $BODY$");
    return complete_pq_exec(result);
}

static int prepare_stmts(PGconn *db)
{
    PGresult *result;

    result = PQprepare(db, "read",
        "SELECT \"type\", \"data\""
        "  FROM \"" GIT2_TABLE_NAME "\""
        "  WHERE \"oid\" = $1::bytea",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    result = PQprepare(db, "read_header",
        "SELECT \"type\", length(\"data\")"
        "  FROM \"" GIT2_TABLE_NAME "\""
        "  WHERE \"oid\" = $1::bytea",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    result = PQprepare(db, "exists",
        "SELECT 1"
        "  FROM \"" GIT2_TABLE_NAME "\""
        "  WHERE \"oid\" = $1::bytea",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    result = PQprepare(db, "write",
        "INSERT INTO \"" GIT2_TABLE_NAME "\""
        "  (\"oid\", \"type\", \"data\")"
        "  VALUES($1::bytea, $2::int, $3::bytea)",
        4, NULL);
    if (complete_pq_exec(result))
        return 1;

    return 0;
}

git_error_code git_odb_backend_pgsql(git_odb_backend **backend_out,
    const char *conninfo)
{
    pgsql_odb_backend *backend;
    int error;

    backend = calloc(1, sizeof(pgsql_odb_backend));
    if (NULL == backend) {
        giterr_set_oom();
        return GIT_ERROR;
    }

    backend->db = PQconnectdb(conninfo);
    if (PQstatus(backend->db) != CONNECTION_OK)
        goto cleanup;

    error = init_db(backend->db);
    if (error)
        goto cleanup;

    error = prepare_stmts(backend->db);
    if (error)
        goto cleanup;

    backend->parent.version = GIT_ODB_BACKEND_VERSION;
    backend->parent.read = &pgsql_odb_backend__read;
    backend->parent.read_header = &pgsql_odb_backend__read_header;
    backend->parent.write = &pgsql_odb_backend__write;
    backend->parent.exists = &pgsql_odb_backend__exists;
    backend->parent.free = &pgsql_odb_backend__free;

    *backend_out = (git_odb_backend*)backend;
    return GIT_OK;

cleanup:
    set_giterr_from_pg(backend);
    pgsql_odb_backend__free((git_odb_backend*)backend);
    return GIT_ERROR;
}
