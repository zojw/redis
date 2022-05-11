#ifndef __DISKCACHE_H
#define __DISKCACHE_H

#include "server.h"

/* Disk store threaded I/O request message */
#define REDIS_IOJOB_LOAD 0
#define REDIS_IOJOB_SAVE 1

/* IO operations scheduled */
typedef struct ioop {
    int type;
    redisDb *db;
    robj *key;
    time_t ctime; /* This is the creation time of the entry. */
} ioop;

typedef struct iojob
{
	int type;      /* Request type, REDIS_IOJOB_* */
	redisDb *db;   /* Redis database */
	robj *key;     /* This I/O request is about this key */
	robj *val;     /* the value to swap for REDIS_IOJOB_SAVE, otherwise this
			* field is populated by the I/O thread for REDIS_IOJOB_LOAD. */
	time_t expire; /* Expire time for this key on REDIS_IOJOB_LOAD */
} iojob;

void dsInit(void);
void cacheCron(void);

// Disk store negative caching
int cacheKeyMayExist(redisDb *db, robj *key);
void cacheSetKeyMayExist(redisDb *db, robj *key);
void cacheSetKeyDoesNotExist(redisDb *db, robj *key);

// IO Thread
void lockThreadedIO(void);
void unlockThreadedIO(void);
void waitEmptyIOJobsQueue(void);
void processAllPendingIOJobs(void);
void queueIOJob(iojob *j);
void cacheForcePointInTime(void);

// IO Scheduler
int cacheScheduleIOGetFlags(redisDb *db, robj *key);
void cacheScheduleIO(redisDb *db, robj *key, int type);

// Block client
int waitForSwappedKey(client *c, robj *key);
int blockClientOnSwappedKeys(client *c, struct redisCommand *cmd);
int dontWaitForSwappedKey(client *c, robj *key);
void handleClientsBlockedOnSwappedKey(client *db, robj *key);

#endif /* __DISKCACHE_H */