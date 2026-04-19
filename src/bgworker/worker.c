/*
 * worker.c — pg_accumulator background maintenance worker
 *
 * Performs three periodic tasks:
 *   1. Delta merge  — flush accumulated deltas into balance_cache
 *   2. Partition maintenance — create partitions ahead of current date
 *   3. Statistics collection — log register stats periodically
 *
 * Each task has its own interval controlled by GUCs. The worker sleeps
 * on a latch and wakes up at the shortest interval, executing whichever
 * tasks are due. Advisory locks prevent multiple workers from conflicting.
 *
 * Graceful shutdown on SIGTERM via the standard latch mechanism.
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/xact.h"
#include "executor/spi.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "tcop/utility.h"
#include "utils/builtins.h"
#include "utils/snapmgr.h"

#include "../pg_accumulator.h"

/* --------------------------------------------------------
 * Signal handling
 * -------------------------------------------------------- */
static volatile sig_atomic_t got_sigterm = false;
static volatile sig_atomic_t got_sighup = false;

static void
pgacc_sigterm_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

static void
pgacc_sighup_handler(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sighup = true;
	SetLatch(MyLatch);

	errno = save_errno;
}

/* --------------------------------------------------------
 * Execute a SQL query inside a transaction via SPI.
 * Returns true on success, false on error (logged, not thrown).
 * -------------------------------------------------------- */
static bool
pgacc_execute_sql(const char *sql, int *rows_affected)
{
	int  ret;
	bool success = true;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	pgstat_report_activity(STATE_RUNNING, sql);

	ret = SPI_execute(sql, false, 0);
	if (ret != SPI_OK_SELECT && ret != SPI_OK_UPDATE &&
		ret != SPI_OK_DELETE && ret != SPI_OK_INSERT &&
		ret != SPI_OK_UTILITY && ret != SPI_OK_UPDATE_RETURNING &&
		ret != SPI_OK_DELETE_RETURNING)
	{
		elog(WARNING, "pg_accumulator worker: SPI_execute failed: %s (code %d)",
			 sql, ret);
		success = false;
	}

	if (rows_affected)
		*rows_affected = (int) SPI_processed;

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();

	pgstat_report_activity(STATE_IDLE, NULL);

	return success;
}

/* --------------------------------------------------------
 * Try to acquire a session-level advisory lock.
 * Returns true if lock acquired, false if already held.
 * The lock is released explicitly via pgacc_advisory_unlock.
 * -------------------------------------------------------- */
static bool
pgacc_advisory_trylock(int64 key1, int64 key2)
{
	int  ret;
	bool acquired = false;

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(
		psprintf("SELECT pg_try_advisory_lock(%lld, %lld)",
				 (long long) key1, (long long) key2),
		true, 1);

	if (ret == SPI_OK_SELECT && SPI_processed == 1)
	{
		bool isnull;
		Datum val = SPI_getbinval(SPI_tuptable->vals[0],
								 SPI_tuptable->tupdesc, 1, &isnull);
		if (!isnull)
			acquired = DatumGetBool(val);
	}

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();

	return acquired;
}

static void
pgacc_advisory_unlock(int64 key1, int64 key2)
{
	char sql[128];

	snprintf(sql, sizeof(sql),
			 "SELECT pg_advisory_unlock(%lld, %lld)",
			 (long long) key1, (long long) key2);
	pgacc_execute_sql(sql, NULL);
}

/* --------------------------------------------------------
 * Advisory lock keys (namespaced to avoid collisions)
 * key1 = 0x70676163 ('pgac'), key2 = task id + worker id
 * -------------------------------------------------------- */
#define PGACC_LOCK_NAMESPACE  0x70676163
#define PGACC_LOCK_DELTA      1
#define PGACC_LOCK_PARTITION  2
#define PGACC_LOCK_STATS      3

/* --------------------------------------------------------
 * Task: Delta merge
 * Calls accum._delta_merge(interval, batch_size). Requires
 * an advisory lock so only one worker merges at a time.
 * -------------------------------------------------------- */
static void
pgacc_do_delta_merge(void)
{
	char sql[512];
	int  merged = 0;

	/* Try to lock — skip if another worker is already merging */
	if (!pgacc_advisory_trylock(PGACC_LOCK_NAMESPACE, PGACC_LOCK_DELTA))
	{
		elog(DEBUG1, "pg_accumulator worker: delta merge lock busy, skipping");
		return;
	}

	snprintf(sql, sizeof(sql),
			 "SELECT %s._delta_merge("
			 "  p_max_age := make_interval(secs := %d / 1000.0),"
			 "  p_batch_size := %d"
			 ")",
			 pgacc_schema ? pgacc_schema : "accum",
			 pgacc_delta_merge_delay_ms,
			 pgacc_delta_merge_batch_size);

	if (pgacc_execute_sql(sql, &merged) && merged > 0)
		elog(DEBUG1, "pg_accumulator worker: delta merge processed %d rows", merged);

	pgacc_advisory_unlock(PGACC_LOCK_NAMESPACE, PGACC_LOCK_DELTA);
}

/* --------------------------------------------------------
 * Task: Partition maintenance
 * For each register, ensure partitions exist ahead of now.
 * -------------------------------------------------------- */
static void
pgacc_do_partition_maintenance(void)
{
	char sql[1024];
	int  created = 0;

	if (!pgacc_advisory_trylock(PGACC_LOCK_NAMESPACE, PGACC_LOCK_PARTITION))
	{
		elog(DEBUG1, "pg_accumulator worker: partition lock busy, skipping");
		return;
	}

	/*
	 * Iterate over all registers and create partitions ahead.
	 * We use a DO block to iterate in a single transaction.
	 */
	snprintf(sql, sizeof(sql),
			 "DO $pgacc$ "
			 "DECLARE "
			 "  reg record; "
			 "  cnt int; "
			 "  total int := 0; "
			 "BEGIN "
			 "  FOR reg IN "
			 "    SELECT r.name, r.partition_by "
			 "    FROM %s._registers r ORDER BY r.name "
			 "  LOOP "
			 "    SELECT %s._create_initial_partitions("
			 "      reg.name, reg.partition_by, %d) INTO cnt; "
			 "    total := total + cnt; "
			 "  END LOOP; "
			 "  IF total > 0 THEN "
			 "    RAISE LOG 'pg_accumulator worker: created %% partitions', total; "
			 "  END IF; "
			 "END $pgacc$;",
			 pgacc_schema ? pgacc_schema : "accum",
			 pgacc_schema ? pgacc_schema : "accum",
			 pgacc_partitions_ahead);

	pgacc_execute_sql(sql, &created);

	pgacc_advisory_unlock(PGACC_LOCK_NAMESPACE, PGACC_LOCK_PARTITION);
}

/* --------------------------------------------------------
 * Task: Statistics collection
 * Log summary stats for each register.
 * -------------------------------------------------------- */
static void
pgacc_do_stats_collection(void)
{
	char sql[1024];

	if (!pgacc_advisory_trylock(PGACC_LOCK_NAMESPACE, PGACC_LOCK_STATS))
		return;

	snprintf(sql, sizeof(sql),
			 "DO $pgacc$ "
			 "DECLARE "
			 "  reg record; "
			 "  st  jsonb; "
			 "BEGIN "
			 "  FOR reg IN "
			 "    SELECT r.name FROM %s._registers r ORDER BY r.name "
			 "  LOOP "
			 "    BEGIN "
			 "      st := %s.register_stats(reg.name); "
			 "      RAISE DEBUG1 'pg_accumulator stats [%%]: %%', reg.name, st; "
			 "    EXCEPTION WHEN OTHERS THEN "
			 "      RAISE WARNING 'pg_accumulator stats [%%]: error — %%', "
			 "        reg.name, SQLERRM; "
			 "    END; "
			 "  END LOOP; "
			 "END $pgacc$;",
			 pgacc_schema ? pgacc_schema : "accum",
			 pgacc_schema ? pgacc_schema : "accum");

	pgacc_execute_sql(sql, NULL);

	pgacc_advisory_unlock(PGACC_LOCK_NAMESPACE, PGACC_LOCK_STATS);
}

/* --------------------------------------------------------
 * Check if _registers table exists (for pure-SQL test mode
 * where the extension may not be formally installed but
 * the schema/tables exist).
 * -------------------------------------------------------- */
static bool
pgacc_schema_ready(void)
{
	int  ret;
	bool ready = false;
	char sql[256];

	snprintf(sql, sizeof(sql),
			 "SELECT 1 FROM information_schema.tables "
			 "WHERE table_schema = '%s' AND table_name = '_registers'",
			 pgacc_schema ? pgacc_schema : "accum");

	SetCurrentStatementStartTimestamp();
	StartTransactionCommand();
	SPI_connect();
	PushActiveSnapshot(GetTransactionSnapshot());

	ret = SPI_execute(sql, true, 1);
	if (ret == SPI_OK_SELECT && SPI_processed > 0)
		ready = true;

	PopActiveSnapshot();
	SPI_finish();
	CommitTransactionCommand();

	return ready;
}

/* --------------------------------------------------------
 * Worker main entry point
 * -------------------------------------------------------- */
void
pg_accumulator_worker_main(Datum main_arg)
{
	int       worker_id = DatumGetInt32(main_arg);
	TimestampTz last_delta_merge  = 0;
	TimestampTz last_partition    = 0;
	TimestampTz last_stats        = 0;
	TimestampTz now_ts;
	long      sleep_ms;
	int       rc;

	/* Set up signal handlers */
	pqsignal(SIGTERM, pgacc_sigterm_handler);
	pqsignal(SIGHUP, pgacc_sighup_handler);
	BackgroundWorkerUnblockSignals();

	/* Connect to the demo database. If dbname is NULL, only shared catalogs
	 * are available and queries against information_schema fail. */
	BackgroundWorkerInitializeConnection("accumulator_dev", NULL, 0);

	elog(DEBUG1, "pg_accumulator worker %d: started", worker_id);

	/*
	 * Wait for the schema to become available. The extension or setup
	 * SQL may not have run yet when the worker starts.
	 */
	while (!got_sigterm)
	{
		if (pgacc_schema_ready())
			break;

		elog(DEBUG1, "pg_accumulator worker %d: schema not ready, waiting...",
			 worker_id);

		(void) WaitLatch(MyLatch,
						 WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
						 5000,  /* check every 5 seconds */
						 PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
	}

	if (got_sigterm)
	{
		elog(LOG, "pg_accumulator worker %d: shutdown before schema ready",
			 worker_id);
		proc_exit(0);
	}

	elog(DEBUG1, "pg_accumulator worker %d: schema ready, entering maintenance loop",
		 worker_id);

	/* ---- Post-crash recovery check ---- */
	PG_TRY();
	{
		char recovery_sql[256];
		int  recovered = 0;

		snprintf(recovery_sql, sizeof(recovery_sql),
				 "SELECT %s._recovery_check()",
				 pgacc_schema ? pgacc_schema : "accum");

		if (pgacc_execute_sql(recovery_sql, &recovered) && recovered > 0)
			elog(LOG, "pg_accumulator worker %d: recovery rebuilt %d register(s)",
				 worker_id, recovered);
	}
	PG_CATCH();
	{
		EmitErrorReport();
		FlushErrorState();
		if (IsTransactionState())
			AbortCurrentTransaction();
		elog(WARNING, "pg_accumulator worker %d: recovery check failed, continuing",
			 worker_id);
	}
	PG_END_TRY();

	/* ---- Main maintenance loop ---- */
	while (!got_sigterm)
	{
		/* Reload config on SIGHUP */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
			elog(LOG, "pg_accumulator worker %d: configuration reloaded", worker_id);
		}

		now_ts = GetCurrentTimestamp();

		/* ---- Delta merge ---- */
		if (pgacc_delta_merge_interval_ms > 0 &&
			TimestampDifferenceExceeds(last_delta_merge, now_ts,
									   pgacc_delta_merge_interval_ms))
		{
			PG_TRY();
			{
				pgacc_do_delta_merge();
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();
				/* Abort any pending transaction */
				if (IsTransactionState())
					AbortCurrentTransaction();
				elog(WARNING, "pg_accumulator worker %d: delta merge failed, will retry",
					 worker_id);
			}
			PG_END_TRY();

			last_delta_merge = GetCurrentTimestamp();
		}

		if (got_sigterm)
			break;

		/* ---- Partition maintenance ---- */
		if (pgacc_maintenance_interval_ms > 0 &&
			TimestampDifferenceExceeds(last_partition, now_ts,
									   pgacc_maintenance_interval_ms))
		{
			PG_TRY();
			{
				pgacc_do_partition_maintenance();
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();
				if (IsTransactionState())
					AbortCurrentTransaction();
				elog(WARNING, "pg_accumulator worker %d: partition maintenance failed, will retry",
					 worker_id);
			}
			PG_END_TRY();

			last_partition = GetCurrentTimestamp();
		}

		if (got_sigterm)
			break;

		/* ---- Stats collection (runs at maintenance_interval) ---- */
		if (pgacc_maintenance_interval_ms > 0 &&
			TimestampDifferenceExceeds(last_stats, now_ts,
									   pgacc_maintenance_interval_ms))
		{
			PG_TRY();
			{
				pgacc_do_stats_collection();
			}
			PG_CATCH();
			{
				EmitErrorReport();
				FlushErrorState();
				if (IsTransactionState())
					AbortCurrentTransaction();
				elog(WARNING, "pg_accumulator worker %d: stats collection failed, will retry",
					 worker_id);
			}
			PG_END_TRY();

			last_stats = GetCurrentTimestamp();
		}

		if (got_sigterm)
			break;

		/*
		 * Sleep until the next scheduled task. Use the shortest interval
		 * to ensure timely wakeups.
		 */
		sleep_ms = pgacc_delta_merge_interval_ms;
		if (pgacc_maintenance_interval_ms > 0 &&
			pgacc_maintenance_interval_ms < sleep_ms)
			sleep_ms = pgacc_maintenance_interval_ms;
		if (sleep_ms < 100)
			sleep_ms = 100;  /* floor to avoid busy-looping */

		rc = WaitLatch(MyLatch,
					   WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
					   sleep_ms,
					   PG_WAIT_EXTENSION);
		ResetLatch(MyLatch);

		/* Suppress unused-variable warning */
		(void) rc;
	}

	elog(LOG, "pg_accumulator worker %d: shutting down", worker_id);
	proc_exit(0);
}
