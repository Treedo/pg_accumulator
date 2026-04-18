# Модуль: Background Worker (Фоновий процес)

## Призначення
Фоновий процес PostgreSQL, який виконує періодичні обслуговуючі завдання: злиття delta buffer, автоматичне створення партицій, збір статистики.

## Файли
- `worker.c` — Реалізація background worker

## Відповідальність

### 1. Реєстрація worker (`_PG_init`)
```c
BackgroundWorker worker;
snprintf(worker.bgw_name, BGW_MAXLEN, "pg_accumulator maintenance");
worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
worker.bgw_restart_time = 10; // restart через 10 сек при crash
worker.bgw_main_arg = 0;
RegisterBackgroundWorker(&worker);
```

### 2. Цикл обслуговування

```
LOOP:
  1. Перевірити та виконати delta merge для всіх high_write регістрів
     → Кожні pg_accumulator.delta_merge_interval
     → Для кожного регістру з high_write=true:
        DELETE дельт старших за delta_merge_delay
        Агрегувати та UPDATE cache

  2. Перевірити та створити партиції наперед
     → Кожні pg_accumulator.maintenance_interval
     → Для кожного регістру:
        Перевірити чи є партиції на partitions_ahead вперед
        Створити відсутні

  3. Спати до наступного інтервалу
     → WaitLatch з мінімальним інтервалом
     → Реагити на SIGTERM для graceful shutdown
```

### 3. Delta Merge
```
Для кожного регістру з high_write=true:
  1. Advisory lock (pg_try_advisory_lock) → пропустити якщо зайнятий
  2. BEGIN
  3. DELETE FROM delta WHERE created_at < now() - delay
     RETURNING dim_hash, resources
  4. Агрегувати по dim_hash
  5. UPDATE cache SET resources += aggregated
  6. COMMIT
  7. Advisory unlock
```

### 4. Partition Maintenance
```
Для кожного регістру:
  1. Знайти останню існуючу партицію
  2. Якщо < partitions_ahead від now():
     CREATE PARTITION для відсутніх періодів
  3. Advisory lock для уникнення конкурентного створення
```

### 5. Graceful Shutdown
```
HandleSIGTERM:
  - Встановити прапор shutdown
  - Завершити поточну транзакцію (COMMIT або ROLLBACK)
  - Вийти з циклу
```

### 6. Конфігурація
| GUC | Опис | Default |
|-----|------|---------|
| `background_workers` | Кількість worker-процесів | 2 |
| `maintenance_interval` | Інтервал обслуговування | 1 hour |
| `delta_merge_interval` | Інтервал merge дельт | 5s |
| `delta_merge_delay` | Мін. вік дельти | 2s |
| `delta_merge_batch_size` | Макс. дельт за merge | 10000 |

## Залежності
- `core/registry` — список регістрів
- `delta_buffer/merge` — логіка злиття
- `partitioning/auto_create` — створення партицій

## SQL-файли
- Немає (реалізований повністю на C)

## Тести
- Worker запускається при завантаженні розширення
- Delta merge виконується за розкладом
- Партиції створюються автоматично
- Graceful shutdown працює
- Множинні workers не конфліктують (advisory locks)
- Worker переживає помилки (restart)
