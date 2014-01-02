#include <libpq-fe.h>

int get_int_from_result(PGresult *result, int *intp, int row, int col);

int complete_pq_exec(PGresult *result);
