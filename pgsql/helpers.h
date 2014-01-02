#include <libpq-fe.h>

int get_int_from_result(PGresult *result, int *intp, int col_idx);

int complete_pq_exec(PGresult *result);
