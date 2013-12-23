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
#include <git2.h>
#include <git2/sys/odb_backend.h>


#define GIT2_TABLE_NAME "git2_odb"
#define GIT2_PK_NAME "git2_odb_pkey"
#define GIT2_TYPE_IDX_NAME "git2_odb_idx_type"
#define GIT2_SIZE_IDX_NAME "git2_odb_idx_size"


typedef struct {
    git_odb_backend parent;
    PGconn *db;
} pgsql_backend;


static void set_giterr_from_pg(pgsql_backend *backend)
{
    giterr_set_str(GITERR_ODB, PQerrorMessage(backend->db));
}

static void pgsql_backend__free(git_odb_backend *_backend)
{
    pgsql_backend *backend = (pgsql_backend*)_backend;
    assert(backend);

    PQfinish(backend->db);
}

static int pgsql_backend__read(void **data_p, size_t *len_p, git_otype *type_p,
    git_odb_backend *_backend, const git_oid *oid)
{
    pgsql_backend *backend = (pgsql_backend*)_backend;
    PGresult *result;
    const char * const param_values[1] = {oid->id};
    int param_lengths[1] = {20};
    int param_formats[1] = {1};     /* binary */
    int error = GIT_ERROR;
    int value_len;

    assert(len_p && type_p && backend && oid);

    result = PQexecPrepared(backend->db, "read",
        1, param_values, param_lengths, param_formats,
        /* binary result */ 1);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        error = GIT_ERROR;
        set_giterr_from_pg(backend);
        goto cleanup;
    }

    if (PQntuples(result) == 0) {
        error = GIT_ENOTFOUND;
        goto cleanup;
    }

    value_len = PQgetlength(result, 0, 0);
    if (value_len != sizeof(*type_p)) {
        error = GIT_ERROR;
        giterr_set_str(GITERR_ODB, "\"type\" column has bad size");
        goto cleanup;
    }

    value_len = PQgetlength(result, 0, 0);
    if (value_len != sizeof(*len_p)) {
        error = GIT_ERROR;
        giterr_set_str(GITERR_ODB, "\"size\" column has bad size");
        goto cleanup;
    }

    memcpy(type_p, PQgetvalue(result, 0, 0), sizeof(*type_p));
    memcpy(len_p, PQgetvalue(result, 0, 1), sizeof(*len_p));

    value_len = PQgetlength(result, 0, 2);
    if (value_len > 0) {
        *data_p = git_odb_backend_malloc(_backend, value_len);
        memcpy(data_p, PQgetvalue(result, 0, 2), value_len);
    }

    error = GIT_OK;

cleanup:
    PQclear(result);
    return error;
}

static int complete_pq_exec(PGresult *result)
{
    ExecStatusType exec_status = PQresultStatus(result);
    PQclear(result);
    return (PGRES_COMMAND_OK != exec_status);
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
        "  \"size\" bigint NOT NULL,"
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

        "IF NOT EXISTS("
        "  select 1 from pg_index, pg_class"
        "  where pg_index.indexrelid = pg_class.oid"
        "    and pg_class.relname = '" GIT2_SIZE_IDX_NAME "'"
        ")"
        "THEN"
        "  CREATE INDEX \"" GIT2_SIZE_IDX_NAME "\""
        "    ON \"" GIT2_TABLE_NAME "\""
        "    (\"size\");"
        "END IF;"

        /* end plpgsql statement */
        "END; $BODY$");
    return complete_pq_exec(result);
}

static int prepare_stmts(PGconn *db)
{
    PGresult *result;

    result = PQprepare(db, "read",
        "SELECT \"type\", \"size\", \"data\""
        "  FROM \"" GIT2_TABLE_NAME "\""
        "  WHERE \"oid\" = $1::bytea",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    return 0;
}

git_error_code git_odb_backend_pgsql(git_odb_backend **backend_out,
    const char *conninfo)
{
    pgsql_backend *backend;
    int error;

    backend = calloc(1, sizeof(pgsql_backend));
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

    backend->parent.read = &pgsql_backend__read;
    /*backend->parent.read_header = &pgsql_backend__read_header;
    backend->parent.write = &pgsql_backend__write;
    backend->parent.exists = &pgsql_backend__exists;*/
    backend->parent.free = &pgsql_backend__free;

    *backend_out = (git_odb_backend*)backend;
    return GIT_OK;

cleanup:
    set_giterr_from_pg(backend);
    pgsql_backend__free((git_odb_backend*)backend);
    return GIT_ERROR;
}
