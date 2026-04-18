/*
 * pg_accumulator.c — Extension entry point
 *
 * Registers GUC parameters and launches background workers
 * for periodic maintenance (delta merge, partition creation, stats).
 */

#include "postgres.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "utils/guc.h"

#include "../pg_accumulator.h"

PG_MODULE_MAGIC;

/* --------------------------------------------------------
 * GUC variables
 * -------------------------------------------------------- */
int   pgacc_background_workers      = 1;
int   pgacc_maintenance_interval_ms = 3600000;  /* 1 hour  */
int   pgacc_delta_merge_interval_ms = 5000;     /* 5 sec   */
int   pgacc_delta_merge_delay_ms    = 2000;     /* 2 sec   */
int   pgacc_delta_merge_batch_size  = 10000;
int   pgacc_partitions_ahead        = 3;
char *pgacc_schema                  = NULL;

/* --------------------------------------------------------
 * _PG_init — called when the shared library is loaded
 * -------------------------------------------------------- */
void
_PG_init(void)
{
	BackgroundWorker worker;
	int              i;

	if (!process_shared_preload_libraries_in_progress)
		return;

	/* --- GUC: pg_accumulator.background_workers --- */
	DefineCustomIntVariable(
		"pg_accumulator.background_workers",
		"Number of background maintenance workers.",
		NULL,
		&pgacc_background_workers,
		1,                /* default */
		0,                /* min — 0 disables workers */
		8,                /* max */
		PGC_POSTMASTER,
		0,
		NULL, NULL, NULL);

	/* --- GUC: pg_accumulator.maintenance_interval --- */
	DefineCustomIntVariable(
		"pg_accumulator.maintenance_interval",
		"Interval between partition maintenance runs (ms).",
		NULL,
		&pgacc_maintenance_interval_ms,
		3600000,          /* 1 hour */
		1000,             /* 1 sec min */
		86400000,         /* 24 hours max */
		PGC_SIGHUP,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	/* --- GUC: pg_accumulator.delta_merge_interval --- */
	DefineCustomIntVariable(
		"pg_accumulator.delta_merge_interval",
		"Interval between delta buffer merge cycles (ms).",
		NULL,
		&pgacc_delta_merge_interval_ms,
		5000,             /* 5 sec */
		100,              /* 100 ms min */
		3600000,          /* 1 hour max */
		PGC_SIGHUP,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	/* --- GUC: pg_accumulator.delta_merge_delay --- */
	DefineCustomIntVariable(
		"pg_accumulator.delta_merge_delay",
		"Minimum age of a delta row before merge (ms).",
		NULL,
		&pgacc_delta_merge_delay_ms,
		2000,             /* 2 sec */
		0,
		3600000,
		PGC_SIGHUP,
		GUC_UNIT_MS,
		NULL, NULL, NULL);

	/* --- GUC: pg_accumulator.delta_merge_batch_size --- */
	DefineCustomIntVariable(
		"pg_accumulator.delta_merge_batch_size",
		"Maximum number of delta rows processed per merge cycle.",
		NULL,
		&pgacc_delta_merge_batch_size,
		10000,
		100,
		1000000,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	/* --- GUC: pg_accumulator.partitions_ahead --- */
	DefineCustomIntVariable(
		"pg_accumulator.partitions_ahead",
		"Number of partitions to create ahead of current date.",
		NULL,
		&pgacc_partitions_ahead,
		3,
		0,
		24,
		PGC_SIGHUP,
		0,
		NULL, NULL, NULL);

	/* --- GUC: pg_accumulator.schema --- */
	DefineCustomStringVariable(
		"pg_accumulator.schema",
		"Schema name used by the extension.",
		NULL,
		&pgacc_schema,
		"accum",
		PGC_SUSET,
		0,
		NULL, NULL, NULL);

	/* --- Register background workers --- */
	for (i = 0; i < pgacc_background_workers; i++)
	{
		memset(&worker, 0, sizeof(BackgroundWorker));

		snprintf(worker.bgw_name, BGW_MAXLEN,
				 "pg_accumulator maintenance worker %d", i);
		snprintf(worker.bgw_type, BGW_MAXLEN,
				 "pg_accumulator maintenance");
		snprintf(worker.bgw_library_name, BGW_MAXLEN,
				 "pg_accumulator");
		snprintf(worker.bgw_function_name, BGW_MAXLEN,
				 "pg_accumulator_worker_main");

		worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
						   BGWORKER_BACKEND_DATABASE_CONNECTION;
		worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
		worker.bgw_restart_time = 10;   /* restart after 10s on crash */
		worker.bgw_main_arg = Int32GetDatum(i);
		worker.bgw_notify_pid = 0;

		RegisterBackgroundWorker(&worker);
	}

	elog(LOG, "pg_accumulator: registered %d background worker(s)",
		 pgacc_background_workers);
}
