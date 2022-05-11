#include "diskcache.h"
#include "server.h"

#define DISK_NOTUSED(V) ((void)V)

//============== decl ==================

int cacheFreeOneEntry(void);
int negativeCacheEvictOneEntry(void);

// IO Thread and enqueue job.
void vmThreadedIOCompletedJob(aeEventLoop *el, int fd, void *privdata, int mask);
void *IOThreadEntryPoint(void *arg);
void spawnIOThread(void);
int processActiveIOJobs(int max);
int processPendingIOJobs(int max);
void cacheCreateIOJob(int type, redisDb *db, robj *key, robj *val, time_t expire);
void freeIOJob(iojob *j);

// IO Scheduler
void cacheScheduleIOAddFlag(redisDb *db, robj *key, long flag);
void cacheScheduleIODelFlag(redisDb *db, robj *key, long flag);
int cacheScheduleIOPushJobs(int flags);

//============== impl ==================

void dsInit(void)
{
	int pipefds[2];
	size_t stacksize;

	serverLog(LL_NOTICE, "Opening Disk Store: %s", server.ds_path);

	if (dsOpen() != C_OK)
	{
		serverLog(LL_WARNING, "Fatal error opening disk store. Exiting.");
		exit(1);
	};

	server.io_newjobs = listCreate();
	server.io_processing = listCreate();
	server.io_processed = listCreate();
	server.io_ready_clients = listCreate();
	pthread_mutex_init(&server.io_mutex, NULL);
	pthread_cond_init(&server.io_condvar, NULL);
	// pthread_mutex_init(&server.bgsavethread_mutex, NULL);
	server.io_active_threads = 0;
	if (pipe(pipefds) == -1)
	{
		serverLog(LL_WARNING, "Unable to intialized DS: pipe(2): %s. Exiting.", strerror(errno));
		exit(1);
	}
	server.io_ready_pipe_read = pipefds[0];
	server.io_ready_pipe_write = pipefds[1];
	assert(anetNonBlock(NULL, server.io_ready_pipe_read) != ANET_ERR);

	pthread_attr_init(&server.io_threads_attr);
	pthread_attr_getstacksize(&server.io_threads_attr, &stacksize);
	if (!stacksize)
		stacksize = 1;
	while (stacksize < REDIS_THREAD_STACK_SIZE)
		stacksize *= 2;
	pthread_attr_setstacksize(&server.io_threads_attr, stacksize);

	if (aeCreateFileEvent(server.el, server.io_ready_pipe_read, AE_READABLE,
			      vmThreadedIOCompletedJob, NULL) == AE_ERR)
		oom("creating file event");

	/* Spawn our I/O thread */
	spawnIOThread();
}

/* Redis generally does not try to recover from out of memory conditions
 * when allocating objects or strings, it is not clear if it will be possible
 * to report this condition to the client since the networking layer itself
 * is based on heap allocation for send buffers, so we simply abort.
 * At least the code will be simpler to read... */
void oom(const char *msg)
{
	serverLog(LL_WARNING, "%s: Out of memory\n", msg);
	sleep(1);
	abort();
}

void cacheCron(void)
{
	/* Push jobs */
	cacheScheduleIOPushJobs(0);

	/* Reclaim memory from the object cache */
	while (server.ds_enabled && zmalloc_used_memory() >
					server.cache_max_memory)
	{
		int done = 0;
		if (cacheFreeOneEntry() == C_OK)
			done++;
		if (negativeCacheEvictOneEntry() == C_OK)
			done++;
		if (done == 0)
			break; /* nothing more to free */
	}
}

/* Compute how good candidate the specified object is for eviction.
 * An higher number means a better candidate. */
double computeObjectSwappability(robj *o)
{
	/* actual age can be >= minage, but not < minage. As we use wrapping
	 * 21 bit clocks with minutes resolution for the LRU. */
	return (double)estimateObjectIdleTime(o);
}

int cacheFreeOneEntry(void)
{
	int j, i;
	struct dictEntry *best = NULL;
	double best_swappability = 0;
	redisDb *best_db = NULL;
	robj *val;
	sds key;

	for (j = 0; j < server.dbnum; j++)
	{
		redisDb *db = server.db + j;
		/* Why maxtries is set to 100?
		 * Because this way (usually) we'll find 1 object even if just 1% - 2%
		 * are swappable objects */
		int maxtries = 100;

		for (i = 0; i < 5 && dictSize(db->dict); i++)
		{
			dictEntry *de;
			double swappability;
			robj keyobj;
			sds keystr;

			if (maxtries)
				maxtries--;
			de = dictGetRandomKey(db->dict);
			keystr = dictGetKey(de);
			val = dictGetVal(de);
			initStaticStringObject(keyobj, keystr);

			/* Don't remove objects that are currently target of a
			 * read or write operation. */
			if (cacheScheduleIOGetFlags(db, &keyobj) != 0)
			{
				if (maxtries)
					i--; /* don't count this try */
				continue;
			}
			swappability = computeObjectSwappability(val);
			if (!best || swappability > best_swappability)
			{
				best = de;
				best_swappability = swappability;
				best_db = db;
			}
		}
	}
	if (best == NULL)
	{
		/* Not able to free a single object? we should check if our
		 * IO queues have stuff in queue, and try to consume the queue
		 * otherwise we'll use an infinite amount of memory if changes to
		 * the dataset are faster than I/O */
		if (listLength(server.cache_io_queue) > 0)
		{
			serverLog(LL_DEBUG, "--- Busy waiting IO to reclaim memory");
			cacheScheduleIOPushJobs(REDIS_IO_ASAP);
			processActiveIOJobs(1);
			return C_OK;
		}
		/* Nothing to free at all... */
		return C_ERR;
	}
	key = dictGetKey(best);
	val = dictGetVal(best);

	serverLog(LL_DEBUG, "Key selected for cache eviction: %s swappability:%f",
		  key, best_swappability);

	/* Delete this key from memory */
	{
		robj *kobj = createStringObject(key, sdslen(key));
		dbDelete(best_db, kobj);
		decrRefCount(kobj);
	}
	return C_OK;
}

int negativeCacheEvictOneEntry(void)
{
	struct dictEntry *de;
	robj *best = NULL;
	redisDb *best_db = NULL;
	time_t time, best_time = 0;
	int j;

	for (j = 0; j < server.dbnum; j++)
	{
		redisDb *db = server.db + j;
		int i;

		if (dictSize(db->io_negcache) == 0)
			continue;
		for (i = 0; i < 3; i++)
		{
			de = dictGetRandomKey(db->io_negcache);
			time = (time_t)dictGetVal(de);

			if (best == NULL || time < best_time)
			{
				best = dictGetKey(de);
				best_db = db;
				best_time = time;
			}
		}
	}
	if (best)
	{
		dictDelete(best_db->io_negcache, best);
		return C_OK;
	}
	else
	{
		return C_ERR;
	}
}

int cacheKeyMayExist(redisDb *db, robj *key)
{
}

void cacheSetKeyMayExist(redisDb *db, robj *key)
{
}

void cacheSetKeyDoesNotExist(redisDb *db, robj *key)
{
}

void waitEmptyIOJobsQueue(void)
{
}

void processAllPendingIOJobs(void)
{
}

void cacheForcePointInTime(void)
{
}

// IO Thread

void lockThreadedIO(void)
{
	pthread_mutex_lock(&server.io_mutex);
}

void unlockThreadedIO(void)
{
	pthread_mutex_unlock(&server.io_mutex);
}

void vmThreadedIOCompletedJob(aeEventLoop *el, int fd, void *privdata, int mask)
{
	char buf[1];
	int retval, processed = 0, toprocess = -1;
	DISK_NOTUSED(el);
	DISK_NOTUSED(mask);

	/* For every byte we read in the read side of the pipe, there is one
	 * I/O job completed to process. */
	while ((retval = read(fd, buf, 1)) == 1)
	{
		iojob *j;
		listNode *ln;

		serverLog(LL_DEBUG, "Processing I/O completed job");

		/* Get the processed element (the oldest one) */
		lockThreadedIO();
		assert(listLength(server.io_processed) != 0);
		if (toprocess == -1)
		{
			toprocess = (listLength(server.io_processed) * REDIS_MAX_COMPLETED_JOBS_PROCESSED) / 100;
			if (toprocess <= 0)
				toprocess = 1;
		}
		ln = listFirst(server.io_processed);
		j = ln->value;
		listDelNode(server.io_processed, ln);
		unlockThreadedIO();

		/* Post process it in the main thread, as there are things we
		 * can do just here to avoid race conditions and/or invasive locks */
		serverLog(LL_DEBUG, "COMPLETED Job type %s, key: %s",
			  (j->type == REDIS_IOJOB_LOAD) ? "load" : "save",
			  (unsigned char *)j->key->ptr);
		if (j->type == REDIS_IOJOB_LOAD)
		{
			/* Create the key-value pair in the in-memory database */
			if (j->val != NULL)
			{
				/* Note: it's possible that the key is already in memory
				 * due to a blocking load operation. */
				if (dictFind(j->db->dict, j->key->ptr) == NULL)
				{
					dbAdd(j->db, j->key, j->val);
					incrRefCount(j->val);
					if (j->expire != -1)
						setExpire(NULL, j->db, j->key, j->expire);
				}
			}
			else
			{
				/* Key not found on disk. If it is also not in memory
				 * as a cached object, nor there is a job writing it
				 * in background, we are sure the key does not exist
				 * currently.
				 *
				 * So we set a negative cache entry avoiding that the
				 * resumed client will block load what does not exist... */
				if (dictFind(j->db->dict, j->key->ptr) == NULL &&
				    (cacheScheduleIOGetFlags(j->db, j->key) &
				     (REDIS_IO_SAVE | REDIS_IO_SAVEINPROG)) == 0)
				{
					cacheSetKeyDoesNotExist(j->db, j->key);
				}
			}
			cacheScheduleIODelFlag(j->db, j->key, REDIS_IO_LOADINPROG);
			handleClientsBlockedOnSwappedKey(j->db, j->key);
		}
		else if (j->type == REDIS_IOJOB_SAVE)
		{
			cacheScheduleIODelFlag(j->db, j->key, REDIS_IO_SAVEINPROG);
		}
		freeIOJob(j);
		processed++;
		if (privdata == NULL)
			cacheScheduleIOPushJobs(0);
		if (processed == toprocess)
			return;
	}
	if (retval < 0 && errno != EAGAIN)
	{
		serverLog(LL_WARNING,
			  "WARNING: read(2) error in vmThreadedIOCompletedJob() %s",
			  strerror(errno));
	}
}

void spawnIOThread(void)
{
	pthread_t thread;
	sigset_t mask, omask;
	int err;

	sigemptyset(&mask);
	sigaddset(&mask, SIGCHLD);
	sigaddset(&mask, SIGHUP);
	sigaddset(&mask, SIGPIPE);
	pthread_sigmask(SIG_SETMASK, &mask, &omask);
	while ((err = pthread_create(&thread, &server.io_threads_attr, IOThreadEntryPoint, NULL)) != 0)
	{
		serverLog(LL_WARNING, "Unable to spawn an I/O thread: %s",
			  strerror(err));
		usleep(1000000);
	}
	pthread_sigmask(SIG_SETMASK, &omask, NULL);
	server.io_active_threads++;
}

void *IOThreadEntryPoint(void *arg)
{
	iojob *j;
	listNode *ln;
	DISK_NOTUSED(arg);
	long long start;

	pthread_detach(pthread_self());
	lockThreadedIO();
	while (1)
	{
		/* Get a new job to process */
		if (listLength(server.io_newjobs) == 0)
		{
			/* Wait for more work to do */
			serverLog(LL_DEBUG, "[T] wait for signal");
			pthread_cond_wait(&server.io_condvar, &server.io_mutex);
			serverLog(LL_DEBUG, "[T] signal received");
			continue;
		}
		start = ustime();
		serverLog(LL_DEBUG, "[T] %ld IO jobs to process",
			  listLength(server.io_newjobs));
		ln = listFirst(server.io_newjobs);
		j = ln->value;
		listDelNode(server.io_newjobs, ln);
		/* Add the job in the processing queue */
		listAddNodeTail(server.io_processing, j);
		ln = listLast(server.io_processing); /* We use ln later to remove it */
		unlockThreadedIO();

		serverLog(LL_DEBUG, "[T] %ld: new job type %s: %p about key '%s'",
			  (long)pthread_self(),
			  (j->type == REDIS_IOJOB_LOAD) ? "load" : "save",
			  (void *)j, (char *)j->key->ptr);

		/* Process the Job */
		if (j->type == REDIS_IOJOB_LOAD)
		{
			time_t expire;

			j->val = dsGet(j->db, j->key, &expire);
			if (j->val)
				j->expire = expire;
		}
		else if (j->type == REDIS_IOJOB_SAVE)
		{
			if (j->val)
			{
				dsSet(j->db, j->key, j->val, j->expire);
			}
			else
			{
				dsDel(j->db, j->key);
			}
		}

		/* Done: insert the job into the processed queue */
		serverLog(LL_DEBUG, "[T] %ld completed the job: %p (key %s)",
			  (long)pthread_self(), (void *)j, (char *)j->key->ptr);

		serverLog(LL_DEBUG, "[T] lock IO");
		lockThreadedIO();
		serverLog(LL_DEBUG, "[T] IO locked");
		listDelNode(server.io_processing, ln);
		listAddNodeTail(server.io_processed, j);

		/* Signal the main thread there is new stuff to process */
		assert(write(server.io_ready_pipe_write, "x", 1) == 1);
		serverLog(LL_DEBUG, "TIME (%c): %lld\n", j->type == REDIS_IOJOB_LOAD ? 'L' : 'S', ustime() - start);
	}
	/* never reached, but that's the full pattern... */
	unlockThreadedIO();
	return NULL;
}

void freeIOJob(iojob *j)
{
	decrRefCount(j->key);
	/* j->val can be NULL if the job is about deleting the key from disk. */
	if (j->val)
		decrRefCount(j->val);
	zfree(j);
}

int processActiveIOJobs(int max)
{
	int processed = 0;

	while (max == -1 || max > 0)
	{
		int io_processed_len;

		serverLog(LL_DEBUG, "[P] lock IO");
		lockThreadedIO();
		serverLog(LL_DEBUG, "Waiting IO jobs processing: new:%d proessing:%d processed:%d", listLength(server.io_newjobs), listLength(server.io_processing), listLength(server.io_processed));

		if (listLength(server.io_newjobs) == 0 &&
		    listLength(server.io_processing) == 0)
		{
			/* There is nothing more to process */
			serverLog(LL_DEBUG, "[P] Nothing to process, unlock IO, return");
			unlockThreadedIO();
			break;
		}

#if 1
		/* If there are new jobs we need to signal the thread to
		 * process the next one. FIXME: drop this if useless. */
		serverLog(LL_DEBUG, "[P] waitEmptyIOJobsQueue: new %d, processing %d, processed %d",
			  listLength(server.io_newjobs),
			  listLength(server.io_processing),
			  listLength(server.io_processed));

		if (listLength(server.io_newjobs))
		{
			serverLog(LL_DEBUG, "[P] There are new jobs, signal");
			pthread_cond_signal(&server.io_condvar);
		}
#endif

		/* Check if we can process some finished job */
		io_processed_len = listLength(server.io_processed);
		serverLog(LL_DEBUG, "[P] Unblock IO");
		unlockThreadedIO();
		serverLog(LL_DEBUG, "[P] Wait");
		usleep(10000);
		if (io_processed_len)
		{
			vmThreadedIOCompletedJob(NULL, server.io_ready_pipe_read,
						 (void *)0xdeadbeef, 0);
			processed++;
			if (max != -1)
				max--;
		}
	}
	return processed;
}

// IO scheduler

void cacheScheduleIO(redisDb *db, robj *key, int type)
{
}

int cacheScheduleIOGetFlags(redisDb *db, robj *key)
{
	struct dictEntry *de = dictFind(db->io_queued, key);
	return (de == NULL) ? 0 : ((long)dictGetVal(de));
}

void cacheScheduleIOAddFlag(redisDb *db, robj *key, long flag)
{
	struct dictEntry *de = dictFind(db->io_queued, key);
	if (!de)
	{
		dictAdd(db->io_queued, key, (void *)flag);
		incrRefCount(key);
		return;
	}
	else
	{
		long flags = (long)dictGetVal(de);

		if (flags & flag)
		{
			serverLog(LL_WARNING, "Adding the same flag again: was: %ld, addede: %ld", flags, flag);
			assert(!(flags & flag));
		}
		flags |= flag;
		dictGetVal(de) = (void *)flags;
	}
}

void cacheScheduleIODelFlag(redisDb *db, robj *key, long flag)
{
	struct dictEntry *de = dictFind(db->io_queued, key);
	long flags;
	assert(de != NULL);
	flags = (long)dictGetVal(de);
	assert(flags & flag);
	flags &= ~flag;
	if (flags == 0)
	{
		dictDelete(db->io_queued, key);
	}
	else
	{
		dictGetVal(de) = (void *)flags;
	}
}

void queueIOJob(iojob *j)
{
	serverLog(LL_DEBUG, "Queued IO Job %p type %d about key '%s'\n",
		  (void *)j, j->type, (char *)j->key->ptr);
	listAddNodeTail(server.io_newjobs, j);
}

void cacheCreateIOJob(int type, redisDb *db, robj *key, robj *val, time_t expire)
{
	iojob *j;

	j = zmalloc(sizeof(*j));
	j->type = type;
	j->db = db;
	j->key = key;
	incrRefCount(key);
	j->val = val;
	if (val)
		incrRefCount(val);
	j->expire = expire;

	lockThreadedIO();
	queueIOJob(j);
	pthread_cond_signal(&server.io_condvar);
	unlockThreadedIO();
}

#define MAX_IO_JOBS_QUEUE 10

int cacheScheduleIOPushJobs(int flags)
{
	time_t now = time(NULL);
	listNode *ln;
	int jobs, topush = 0, pushed = 0;

	// if (server.bgsavethread != (pthread_t)-1)
	// return 0;

	lockThreadedIO();
	jobs = listLength(server.io_newjobs);
	unlockThreadedIO();

	topush = MAX_IO_JOBS_QUEUE - jobs;
	if (topush < 0)
		topush = 0;
	if (topush > (signed)listLength(server.cache_io_queue))
		topush = listLength(server.cache_io_queue);

	while ((ln = listFirst(server.cache_io_queue)) != NULL)
	{
		ioop *op = ln->value;
		struct dictEntry *de;
		robj *val;

		if (!topush)
			break;
		topush--;

		if (op->type != REDIS_IO_LOAD && flags & REDIS_IO_ONLYLOADS)
			break;

		/* Don't execute SAVE before the scheduled time for completion */
		if (op->type == REDIS_IO_SAVE && !(flags & REDIS_IO_ASAP) &&
		    (now - op->ctime) < server.cache_flush_delay)
			break;

		/* Don't add a SAVE job in the IO thread queue if there is already
		 * a save in progress for the same key. */
		if (op->type == REDIS_IO_SAVE &&
		    cacheScheduleIOGetFlags(op->db, op->key) & REDIS_IO_SAVEINPROG)
		{
			/* Move the operation at the end of the list if there
			 * are other operations, so we can try to process the next one.
			 * Otherwise break, nothing to do here. */
			if (listLength(server.cache_io_queue) > 1)
			{
				listDelNode(server.cache_io_queue, ln);
				listAddNodeTail(server.cache_io_queue, op);
				continue;
			}
			else
			{
				break;
			}
		}

		serverLog(LL_DEBUG, "Creating IO %s Job for key %s",
			  op->type == REDIS_IO_LOAD ? "load" : "save", op->key->ptr);

		if (op->type == REDIS_IO_LOAD)
		{
			cacheCreateIOJob(REDIS_IOJOB_LOAD, op->db, op->key, NULL, 0);
		}
		else
		{
			time_t expire = -1;

			/* Lookup the key, in order to put the current value in the IO
			 * Job. Otherwise if the key does not exists we schedule a disk
			 * store delete operation, setting the value to NULL. */
			de = dictFind(op->db->dict, op->key->ptr);
			if (de)
			{
				val = dictGetVal(de);
				expire = getExpire(op->db, op->key);
			}
			else
			{
				/* Setting the value to NULL tells the IO thread to delete
				 * the key on disk. */
				val = NULL;
			}
			cacheCreateIOJob(REDIS_IOJOB_SAVE, op->db, op->key, val, expire);
		}
		/* Mark the operation as in progress. */
		cacheScheduleIODelFlag(op->db, op->key, op->type);
		cacheScheduleIOAddFlag(op->db, op->key,
				       (op->type == REDIS_IO_LOAD) ? REDIS_IO_LOADINPROG : REDIS_IO_SAVEINPROG);
		/* Finally remove the operation from the queue.
		 * But we'll have trace of it in the hash table. */
		listDelNode(server.cache_io_queue, ln);
		decrRefCount(op->key);
		zfree(op);
		pushed++;
	}
	return pushed;
}

// Block client

void handleClientsBlockedOnSwappedKey(client *db, robj *key)
{
}