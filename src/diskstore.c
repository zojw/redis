#include "server.h"

int dsOpen(void)
{
}

int dsSet(redisDb *db, robj *key, robj *val, time_t expire)
{
}

robj *dsGet(redisDb *db, robj *key, time_t *expire)
{
}

int dsDel(redisDb *db, robj *key) {
}