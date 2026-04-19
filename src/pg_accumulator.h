/*
 * pg_accumulator.h — Shared header for pg_accumulator extension
 *
 * Declares GUC variables, shared state, and bgworker entry points.
 */

#ifndef PG_ACCUMULATOR_H
#define PG_ACCUMULATOR_H

#include "postgres.h"

/* GUC variables — defined in pg_accumulator.c, used by worker.c */
extern int  pgacc_background_workers;
extern int  pgacc_maintenance_interval_ms;
extern int  pgacc_delta_merge_interval_ms;
extern int  pgacc_delta_merge_delay_ms;
extern int  pgacc_delta_merge_batch_size;
extern int  pgacc_partitions_ahead;
extern char *pgacc_schema;

/* Background worker entry point */
PGDLLEXPORT void pg_accumulator_worker_main(Datum main_arg);

#endif /* PG_ACCUMULATOR_H */
