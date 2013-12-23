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
#include <libpq-fe.h>
#include <git2.h>
#include <git2/sys/odb_backend.h>


#define GIT2_TABLE_NAME "git2_odb"
#define GIT2_TYPE_IDX_NAME "git2_odb_idx_type"
#define GIT2_SIZE_IDX_NAME "git2_odb_idx_size"


typedef struct {
  git_odb_backend parent;
  PGconn *db;
} pgsql_backend;


static void pgsql_backend__free(git_odb_backend *_backend)
{
  pgsql_backend *backend = (pgsql_backend*)_backend;
  assert(backend);

  PQfinish(backend->db);
}

static int init_db(PGconn *db)
{
  PGresult *result;
  ExecStatusType exec_status;

  result = PQexec(db,
    "CREATE TABLE IF NOT EXISTS \"" GIT2_TABLE_NAME "\" ("
    "  \"oid\" bytea NOT NULL DEFAULT '',"
    "  \"type\" int NOT NULL,"
    "  \"size\" bigint NOT NULL,"
    "  \"data\" bytea NOT NULL,"
    "  PRIMARY KEY (\"oid\")"
    ");"
    "CREATE INDEX IF NOT EXISTS \"" GIT2_TYPE_IDX_NAME "\""
    "  ON \"" GIT2_TABLE_NAME "\""
    "  (\"type\");"
    "CREATE INDEX IF NOT EXISTS \"" GIT2_SIZE_IDX_NAME "\""
    "  ON \"" GIT2_TABLE_NAME "\""
    "  (\"size\");"
    );

  exec_status = PQresultStatus(result);
  PQclear(result);
  if (PGRES_COMMAND_OK != exec_status)
    return 1;

  return 0;
}

static int prepare_stmts(PGconn *db)
{
  PGresult *result;
  ExecStatusType exec_status;

  result = PQprepare(db, "read",
    "SELECT \"type\", \"size\", \"data\""
    " FROM \"" GIT2_TABLE_NAME "\""
    " WHERE \"oid\" = $1::bytea",
    1, NULL);
  exec_status = PQresultStatus(result);
  PQclear(result);
  if (PGRES_COMMAND_OK != exec_status)
    return 1;

  return 0;
}

git_error_code git_odb_backend_pgsql(git_odb_backend **backend_out,
  const char *conninfo)
{
  pgsql_backend *backend;
  int error;

  backend = calloc(1, sizeof(pgsql_backend));
  if (NULL == backend)
    return GIT_ERROR;

  backend->db = PQconnectdb(conninfo);
  if (PQstatus(backend->db) != CONNECTION_OK)
    goto cleanup;

  error = init_db(backend->db);
  if (error)
    goto cleanup;

  error = prepare_stmts(backend->db);
  if (error)
    goto cleanup;

  /*backend->parent.read = &pgsql_backend__read;
  backend->parent.read_header = &pgsql_backend__read_header;
  backend->parent.write = &pgsql_backend__write;
  backend->parent.exists = &pgsql_backend__exists;*/
  backend->parent.free = &pgsql_backend__free;

  *backend_out = (git_odb_backend*)backend;
  return GIT_OK;

cleanup:
  pgsql_backend__free((git_odb_backend*)backend);
  return GIT_ERROR;
}
