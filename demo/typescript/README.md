# pg_accumulator — TypeScript Demo

Демонстрація розширення `pg_accumulator` у вигляді простого **фінансового трекера**.

## Що демонструє

| Функція | Опис |
|---|---|
| Поточний баланс | Карточки з сумою по кожному рахунку (`balance_cache`) |
| Нова транзакція | `accum.register_post()` — провести запис |
| Скасування | `accum.register_unpost()` — скасувати/видалити запис |
| Залишок на дату | `accum.finance_balance()` — точний стан на будь-яку дату |
| Список рухів | Таблиця останніх 50 транзакцій |

## Стек

- **Node.js 20** + **Express 4** (TypeScript)
- **pg** — підключення до PostgreSQL
- Статичний `index.html` із Vanilla JS (без фреймворків)

## Запуск

```bash
cd demo/typescript
docker compose up --build
```

Відкрити у браузері: **http://localhost:3001**

> Дані БД зберігаються у volume `pgdata_ts`. Для чистого старту:
> ```bash
> docker compose down -v && docker compose up --build
> ```

## Порти

| Сервіс | Порт |
|---|---|
| Веб-додаток | `3001` |
| PostgreSQL | `5435` |
