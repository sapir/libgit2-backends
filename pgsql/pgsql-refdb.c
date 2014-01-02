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
#include <git2/sys/refdb_backend.h>
#include <git2/sys/refs.h>
#include "helpers.h"


#define GIT2_REFDB_TABLE_NAME "git2_refdb"
#define GIT2_REFDB_PK_NAME "git2_refdb_pkey"


typedef struct {
    git_refdb_backend parent;
    PGconn *db;
} pgsql_refdb_backend;


static void set_giterr_from_pg(pgsql_refdb_backend *backend)
{
    giterr_set_str(GITERR_REFERENCE, PQerrorMessage(backend->db));
}

static void pgsql_refdb_backend__free(git_refdb_backend *_backend)
{
    pgsql_refdb_backend *backend = (pgsql_refdb_backend*)_backend;
    assert(backend);

    PQfinish(backend->db);
}

static PGresult *exec_read_stmt(pgsql_refdb_backend *backend,
    const char *stmt_name, const char *str_param)
{
    const char * const param_values[1] = {str_param};
    int param_lengths[1] = {strlen(str_param)};
    int param_formats[1] = {0};     /* text */
    return PQexecPrepared(backend->db, stmt_name,
        1, param_values, param_lengths, param_formats,
        /* binary result */ 1);
}

static int pgsql_refdb_backend__exists(
    int *exists,
    git_refdb_backend *_backend,
    const char *ref_name)
{
    pgsql_refdb_backend *backend = (pgsql_refdb_backend*)_backend;
    PGresult *result;
    int error = GIT_ERROR;

    assert(exists && backend && ref_name);

    result = exec_read_stmt(backend, "exists", ref_name);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        error = GIT_ERROR;
        set_giterr_from_pg(backend);
        goto cleanup;
    }

    *exists = (PQntuples(result) > 0) ? 1 : 0;
    error = GIT_OK;

cleanup:
    PQclear(result);
    return error;
}

static char *glob_to_like_pattern(const char *glob)
{
    const int glob_len = strlen(glob);
    /* at most, each char of the glob will be translated into 2 chars */
    char *like_pattern = calloc(1, glob_len * 2 + 1);
    int i = 0;
    int j = 0;

    for (; glob[i] != '\0'; ++i) {
        switch (glob[i]) {
        /* escaping */
        case '%':
        case '_':
            like_pattern[j++] = '\\';
            like_pattern[j++] = glob[i];
            break;

        /* like equivalents for glob wildcards */
        case '*':
            like_pattern[j++] = '%';
            break;

        case '?':
            like_pattern[j++] = '_';
            break;

        default:
            like_pattern[j++] = glob[i];
            break;
        }
    }

    return like_pattern;
}

static int pgsql_refdb_backend__lookup(
    git_reference **out,
    git_refdb_backend *_backend,
    const char *ref_name)
{
    pgsql_refdb_backend *backend = (pgsql_refdb_backend*)_backend;
    PGresult *result;
    int error = GIT_ERROR;
    int ref_type;
    const char *ref_tgt;
    const char *ref_peel;

    assert(out && backend && ref_name);

    result = exec_read_stmt(backend, "lookup", ref_name);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        error = GIT_ERROR;
        set_giterr_from_pg(backend);
        goto cleanup;
    }

    if (PQntuples(result) == 0) {
        error = GIT_ENOTFOUND;
        goto cleanup;
    }

    if (get_int_from_result(result, &ref_type, 0)) {
        error = GIT_ERROR;
        goto cleanup;
    }

    ref_tgt = PQgetvalue(result, 0, 1);
    ref_peel = PQgetvalue(result, 0, 2);

    if (ref_type == GIT_REF_SYMBOLIC) {
        *out = git_reference__alloc_symbolic(ref_name, ref_tgt);
    } else if (ref_type == GIT_REF_OID) {
        *out = git_reference__alloc(ref_name, (git_oid*)ref_tgt,
            (git_oid*)ref_peel);
    } else {
        error = GIT_ERROR;
        goto cleanup;
    }

    error = GIT_OK;

cleanup:
    PQclear(result);
    return error;
}

static int pgsql_refdb_backend__iterator(
    git_reference_iterator **iter,
    struct git_refdb_backend *backend,
    const char *glob)
{
}

static int pgsql_refdb_backend__write(git_refdb_backend *_backend,
    const git_reference *ref, int force)
{
    pgsql_refdb_backend *backend = (pgsql_refdb_backend*)_backend;
    PGresult *result;

    const char *ref_name = git_reference_name(ref);
    int ref_type = git_reference_type(ref);
    uint32_t fmtd_ref_type = htobe32(ref_type);

    const char *ref_peel = (const char *)git_reference_target_peel(ref);
    int ref_peel_size = (ref_peel == NULL) ? 0 : GIT_OID_RAWSZ;

    const char *ref_tgt;
    int ref_tgt_size;
    switch (ref_type) {
    case GIT_REF_OID:
        ref_tgt = (const char*)git_reference_target(ref);
        ref_tgt_size = (ref_tgt == NULL) ? 0 : GIT_OID_RAWSZ;
        break;

    case GIT_REF_SYMBOLIC:
        ref_tgt = git_reference_symbolic_target(ref);
        ref_tgt_size = (ref_tgt == NULL) ? 0 : strlen(ref_tgt);
        break;

    default:
        return GIT_ERROR;
    }

    const char * const param_values[4] =
        { ref_name, (const char*)&fmtd_ref_type, ref_tgt, ref_peel };

    int param_lengths[4] =
        {strlen(ref_name), sizeof(fmtd_ref_type), ref_tgt_size, ref_peel_size};

    int param_formats[4] = {1, 1, 1, 1};     /* binary */

    if (force && _backend->del(_backend, ref_name)) {
        return GIT_ERROR;
    }

    result = PQexecPrepared(backend->db, "write",
        4, param_values, param_lengths, param_formats,
        /* binary result */ 1);
    if (complete_pq_exec(result)) {
        set_giterr_from_pg(backend);
        return GIT_ERROR;
    }

    return GIT_OK;
}

static int pgsql_refdb_backend__del(git_refdb_backend *_backend,
    const char *ref_name)
{
    pgsql_refdb_backend *backend = (pgsql_refdb_backend*)_backend;
    PGresult *result;
    const char * const param_values[1] = {ref_name};
    int param_lengths[1] = {strlen(ref_name)};
    int param_formats[1] = {0};     /* text */

    assert(data && backend && oid);

    result = PQexecPrepared(backend->db, "del",
        1, param_values, param_lengths, param_formats,
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

        "CREATE TABLE IF NOT EXISTS \"" GIT2_REFDB_TABLE_NAME "\" ("
        "  \"name\" text NOT NULL,"
        "  \"type\" int NOT NULL,"
        "  \"target\" bytea NOT NULL,"
        "  \"peel\" bytea NULL,"
        "  CONSTRAINT \"" GIT2_REFDB_PK_NAME "\" PRIMARY KEY (\"name\")"
        ");"

        /* end plpgsql statement */
        "END; $BODY$");
    return complete_pq_exec(result);
}

static int prepare_stmts(PGconn *db)
{
    PGresult *result;

    result = PQprepare(db, "lookup",
        "SELECT \"type\", \"target\", \"peel\""
        "  FROM \"" GIT2_REFDB_TABLE_NAME "\""
        "  WHERE \"name\" = $1::text",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    result = PQprepare(db, "iterator",
        "SELECT \"name\", \"type\", \"target\", \"peel\""
        "  FROM \"" GIT2_REFDB_TABLE_NAME "\""
        "  WHERE \"name\" LIKE $1::text ESCAPE '\\\\'",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    result = PQprepare(db, "exists",
        "SELECT 1"
        "  FROM \"" GIT2_REFDB_TABLE_NAME "\""
        "  WHERE \"name\" = $1::text",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    result = PQprepare(db, "write",
        "INSERT INTO \"" GIT2_REFDB_TABLE_NAME "\""
        "  (\"name\", \"type\", \"target\", \"peel\")"
        "  VALUES($1::text, $2::int, $3::bytea, $4::bytea)",
        4, NULL);
    if (complete_pq_exec(result))
        return 1;

    result = PQprepare(db, "del",
        "DELETE FROM \"" GIT2_REFDB_TABLE_NAME "\""
        "  WHERE \"name\" = $1::text",
        1, NULL);
    if (complete_pq_exec(result))
        return 1;

    return 0;
}

git_error_code git_refdb_backend_pgsql(git_refdb_backend **backend_out,
    const char *conninfo)
{
    pgsql_refdb_backend *backend;
    int error;

    backend = calloc(1, sizeof(pgsql_refdb_backend));
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

    backend->parent.exists = &pgsql_refdb_backend__exists;
    backend->parent.lookup = &pgsql_refdb_backend__lookup;
    backend->parent.iterator = &pgsql_refdb_backend__iterator;
    backend->parent.write = &pgsql_refdb_backend__write;
    backend->parent.del = &pgsql_refdb_backend__del;

    *backend_out = (git_refdb_backend*)backend;
    return GIT_OK;

cleanup:
    set_giterr_from_pg(backend);
    pgsql_refdb_backend__free((git_refdb_backend*)backend);
    return GIT_ERROR;
}
