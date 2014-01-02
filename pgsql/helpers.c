#include <libpq-fe.h>
#include <endian.h>
#include <string.h>
#include <git2/errors.h>
#include "helpers.h"

int get_int_from_result(PGresult *result, int *intp, int row, int col)
{
    int value_len;

    assert(result && intp);

    value_len = PQgetlength(result, row, col);
    if (value_len != sizeof(*intp)) {
        giterr_set_str(GITERR_ODB, "\"type\" column has bad size");
        return 1;
    }

    memcpy(intp, PQgetvalue(result, row, col), sizeof(*intp));
    *intp = be32toh(*intp);
    return 0;
}

int complete_pq_exec(PGresult *result)
{
    ExecStatusType exec_status = PQresultStatus(result);
    PQclear(result);
    return (PGRES_COMMAND_OK != exec_status);
}
