#include <libpq-fe.h>

int get_int_from_result(PGresult *result, int *intp, int col_idx);
