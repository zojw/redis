#include "diskcache.h"
#include "server.h"

int cacheFreeOneEntry(void);
int negativeCacheEvictOneEntry(void);

// IO Thread and enqueue job.
void vmThreadedIOCompletedJob(aeEventLoop *el, int fd, void *privdata, int mask);
void *IOThreadEntryPoint(void *arg);
void spawnIOThread(void);
int processActiveIOJobs(int max);
int processPendingIOJobs(int max);
void cacheCreateIOJob(int type, redisDb *db, robj *key, robj *val, time_t expire);

// IO Scheduler
void cacheScheduleIOAddFlag(redisDb *db, robj *key, long flag);
void cacheScheduleIODelFlag(redisDb *db, robj *key, long flag);
int cacheScheduleIOPushJobs(int flags);


void dsInit(void)
{
}

void cacheCron(void)
{
	while (zmalloc_used_memory() > server.cache_max_memory)
	{
	}
}

int cacheKeyMayExist(redisDb *db, robj *key)
{

}

void cacheSetKeyMayExist(redisDb *db, robj *key)
{

}

void cacheSetKeyDoesNotExist(redisDb *db, robj *key) {

}

void lockThreadedIO(void)
{

}

void unlockThreadedIO(void)
{

}

void waitEmptyIOJobsQueue(void) {

}

void processAllPendingIOJobs(void) {
}

void queueIOJob(iojob *j) {

}

void cacheForcePointInTime(void) {
}
