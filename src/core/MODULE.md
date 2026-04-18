# Модуль: Core (Ядро розширення)

## Призначення
Фундаментальний модуль, який ініціалізує розширення, створює базову схему та підтримує внутрішній реєстр метаданих усіх створених регістрів накопичення.

## Файли
- `pg_accumulator.c` — Точка входу: `_PG_init()`, реєстрація GUC-параметрів, запуск background worker
- `schema.c` — Створення/валідація службової схеми `accum` (або вказаної користувачем)
- `registry.c` — CRUD-операції над внутрішньою таблицею `_registers` (метадані регістрів)

## Відповідальність

### 1. Ініціалізація розширення (`_PG_init`)
- Реєстрація всіх GUC-параметрів (`pg_accumulator.schema`, `pg_accumulator.hash_function`, тощо)
- Запуск background worker для обслуговування (delta merge, partition maintenance)
- Реєстрація hook'ів при необхідності

### 2. Управління схемою (`schema.c`)
- Створення схеми `accum` при `CREATE EXTENSION`
- Створення службових таблиць:
  ```sql
  CREATE TABLE accum._registers (
      name           text PRIMARY KEY,
      kind           text NOT NULL CHECK (kind IN ('balance', 'turnover')),
      dimensions     jsonb NOT NULL,
      resources      jsonb NOT NULL,
      totals_period  text NOT NULL DEFAULT 'day',
      partition_by   text NOT NULL DEFAULT 'month',
      high_write     boolean NOT NULL DEFAULT false,
      recorder_type  text NOT NULL DEFAULT 'text',
      created_at     timestamptz NOT NULL DEFAULT now(),
      updated_at     timestamptz NOT NULL DEFAULT now()
  );
  ```
- Валідація існування схеми перед операціями

### 3. Реєстр метаданих (`registry.c`)
- `_register_get(name)` — Отримати метадані регістру
- `_register_put(name, ...)` — Зберегти/оновити метадані
- `_register_delete(name)` — Видалити метадані
- `_register_exists(name)` — Перевірити існування
- `_register_list()` — Список усіх зареєстрованих регістрів
- Валідація імен регістрів (латинські літери, цифри, `_`)
- Валідація типів вимірів і ресурсів

## Залежності
- Жодних внутрішніх залежностей (базовий модуль)

## SQL-файли
- `sql/00_schema.sql` — DDL для службових таблиць
- `sql/01_registry.sql` — Функції роботи з реєстром

## Тести
- Створення/видалення схеми
- CRUD операцій реєстру
- Валідація некоректних імен/типів
- Конкурентний доступ до реєстру
