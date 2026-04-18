# pg_accumulator — Розробка

## Архітектура проєкту

```
pg_accumulator/
├── README.MD                          # Документація API
├── Makefile                           # Система збірки (PGXS)
├── pg_accumulator.control             # Метадані розширення
│
├── docker/                            # Docker-інфраструктура
│   ├── Dockerfile                     # Dev-образ з PostgreSQL 17
│   ├── Dockerfile.test                # Тестовий образ з pgTAP
│   ├── docker-compose.yml             # Dev-середовище
│   ├── docker-compose.test.yml        # Тестове середовище (CI)
│   └── init-scripts/
│       └── 01-init-extension.sh       # Ініціалізація при старті
│
├── sql/                               # SQL-файли розширення
│   ├── 00_schema.sql                  # Створення схеми accum
│   ├── 01_registry.sql                # Таблиця метаданих _registers
│   ├── 02_hash.sql                    # Хеш-функції
│   ├── 03_ddl.sql                     # DDL-генерація
│   ├── 04_triggers.sql                # Тригери
│   ├── 05_write_api.sql               # register_post/unpost/repost
│   ├── 06_read_api.sql                # balance/turnover/movements
│   ├── 07_registry_api.sql            # register_create/alter/drop/list/info
│   ├── 08_delta_buffer.sql            # High-write mode
│   ├── 09_partitioning.sql            # Управління партиціями
│   ├── 10_maintenance.sql             # verify/rebuild/stats
│   └── 11_config.sql                  # Конфігурація
│
├── src/                               # Модулі (опис у MODULE.md)
│   ├── core/MODULE.md                 # Ядро, схема, реєстр метаданих
│   ├── hash/MODULE.md                 # Хешування вимірів (xxhash64/murmur3)
│   ├── ddl/MODULE.md                  # Генерація таблиць, індексів, функцій
│   ├── triggers/MODULE.md             # Тригерний ланцюг movements→totals→cache
│   ├── write_api/MODULE.md            # register_post/unpost/repost
│   ├── read_api/MODULE.md             # balance/turnover/movements
│   ├── registry_api/MODULE.md         # register_create/alter/drop/list/info
│   ├── delta_buffer/MODULE.md         # High-write delta buffer + merge
│   ├── partitioning/MODULE.md         # Авто-партиціювання
│   ├── maintenance/MODULE.md          # Діагностика та перебудова
│   └── bgworker/MODULE.md             # Background worker
│
└── test/                              # Тести (pgTAP)
    ├── run_tests.sh                   # Скрипт запуску тестів
    ├── setup/                         # Підготовка тестового середовища
    │   ├── 00-test-schema.sql         # Створення схеми + pgTAP
    │   ├── 01-registry-table.sql      # Таблиця _registers
    │   └── 02-core-functions.sql      # PL/pgSQL-прототипи всього API
    │
    └── sql/                           # pgTAP тест-файли
        ├── 01_core_registry.sql       # Тести: схема, реєстр, валідація
        ├── 02_register_create.sql     # Тести: створення регістрів, DDL
        ├── 03_register_post.sql       # Тести: запис рухів
        ├── 04_register_unpost.sql     # Тести: скасування рухів
        ├── 05_register_repost.sql     # Тести: перепроведення
        ├── 06_register_drop.sql       # Тести: видалення регістрів
        ├── 07_register_list_info.sql  # Тести: register_list/info
        ├── 08_triggers_totals.sql     # Тести: тригерний ланцюг
        ├── 09_balance_cache.sql       # Тести: balance_cache
        ├── 10_correction_retroactive  # Тести: корекція заднім числом
        ├── 11_turnover_register.sql   # Тести: turnover-регістри
        ├── 12_direct_insert.sql       # Тести: прямий INSERT
        ├── 13_multiple_dimensions.sql # Тести: багато вимірів/ресурсів
        ├── 14_end_to_end_warehouse    # E2E: складський облік
        ├── 15_end_to_end_finance.sql  # E2E: фінансові транзакції
        ├── 16_high_write_mode.sql     # Тести: delta buffer
        └── 17_recorder_pattern.sql    # Тести: recorder-паттерн
```

## Порядок розробки (рекомендований)

### Фаза 1: Фундамент
1. **Docker + тести** — розгорнути PostgreSQL, запустити pgTAP ✅
2. **core** — схема `accum`, таблиця `_registers`
3. **hash** — функція `_hash_<register>()`

### Фаза 2: Інфраструктура регістру
4. **ddl** — генерація таблиць movements/totals/cache
5. **triggers** — тригерний ланцюг (BEFORE/AFTER INSERT/DELETE)
6. **registry_api** — `register_create()`, `register_drop()`

### Фаза 3: Операції з даними
7. **write_api** — `register_post()`, `register_unpost()`, `register_repost()`
8. **read_api** — `_balance()`, `_turnover()`, `_movements()`

### Фаза 4: Розширені можливості
9. **delta_buffer** — high-write mode
10. **partitioning** — автоматичне керування партиціями
11. **maintenance** — verify, rebuild, stats

### Фаза 5: Продакшн
12. **bgworker** — background worker (delta merge, partition maintenance)
13. **registry_api** — `register_alter()`, `register_list()`, `register_info()`
14. Оптимізація, бенчмарки, документація

## Запуск тестів

```bash
# Через Docker (рекомендовано)
make test-docker

# Або скриптом
./test/run_tests.sh

# Локально (потрібен PostgreSQL + pgTAP)
./test/run_tests.sh --local
```
