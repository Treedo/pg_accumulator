# pg_accumulator Makefile
# PostgreSQL Extension Build System

EXTENSION    = pg_accumulator
EXTVERSION   = 0.1.0
MODULE_big   = pg_accumulator

OBJS = src/core/pg_accumulator.o \
       src/core/schema.o \
       src/core/registry.o \
       src/hash/hash.o \
       src/ddl/ddl_generator.o \
       src/ddl/ddl_tables.o \
       src/ddl/ddl_indexes.o \
       src/ddl/ddl_functions.o \
       src/triggers/trigger_engine.o \
       src/triggers/trigger_totals.o \
       src/triggers/trigger_cache.o \
       src/write_api/post.o \
       src/write_api/unpost.o \
       src/write_api/repost.o \
       src/read_api/balance.o \
       src/read_api/turnover.o \
       src/read_api/movements.o \
       src/registry_api/create.o \
       src/registry_api/alter.o \
       src/registry_api/drop.o \
       src/registry_api/list.o \
       src/registry_api/info.o \
       src/delta_buffer/delta.o \
       src/delta_buffer/merge.o \
       src/partitioning/partition_manager.o \
       src/partitioning/auto_create.o \
       src/maintenance/verify.o \
       src/maintenance/rebuild.o \
       src/maintenance/stats.o \
       src/bgworker/worker.o

DATA = sql/pg_accumulator--$(EXTVERSION).sql
EXTRA_CLEAN = sql/pg_accumulator--$(EXTVERSION).sql

# SQL source files (concatenated into final extension SQL)
SQL_SRC = sql/00_schema.sql \
          sql/01_registry.sql \
          sql/02_hash.sql \
          sql/03_ddl.sql \
          sql/04_triggers.sql \
          sql/05_write_api.sql \
          sql/06_read_api.sql \
          sql/07_registry_api.sql \
          sql/08_delta_buffer.sql \
          sql/09_partitioning.sql \
          sql/10_maintenance.sql \
          sql/11_config.sql

# Build concatenated SQL
sql/pg_accumulator--$(EXTVERSION).sql: $(SQL_SRC)
	cat $^ > $@

# pgTAP tests
TESTS = $(wildcard test/sql/*.sql)
REGRESS = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

# Custom targets
.PHONY: test-docker test-tap test-all

test-docker:
	docker compose -f docker/docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test-runner

test-tap:
	docker compose -f docker/docker-compose.test.yml run --rm test-runner

test-all: test-docker
