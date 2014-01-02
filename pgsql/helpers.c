#include <libpq-fe.h>
#include <endian.h>
#include "helpers.h"

int get_int_from_result(PGresult *result, int *intp, int col_idx)
{
    int value_len;

    assert(result && intp);

    value_len = PQgetlength(result, 0, col_idx);
    if (value_len != sizeof(*intp)) {
        giterr_set_str(GITERR_ODB, "\"type\" column has bad size");
        return 1;
    }

    memcpy(intp, PQgetvalue(result, 0, col_idx), sizeof(*intp));
    *intp = be32toh(*intp);
    return 0;
}

int complete_pq_exec(PGresult *result)
{
    ExecStatusType exec_status = PQresultStatus(result);
    PQclear(result);
    return (PGRES_COMMAND_OK != exec_status);
}
