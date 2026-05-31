import os
import json
from datetime import date

import psycopg2
import psycopg2.extras
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "demo-secret-key")

DB_DSN = os.environ.get(
    "DATABASE_URL",
    "host=postgres dbname=accumulator_dev user=dev password=dev_password",
)

SUPPORTED_LOCALES = ["en", "uk"]

TRANSLATIONS = {
    "en": {
        "page_title": "pg_accumulator — Accounting & Inventory Demo",
        "subtitle": "PostgreSQL extension for high-performance aggregation and financial bookkeeping",
        "registered_registers": "Registered registers:",
        "audit_sound": "✓ Debit ≡ Credit audit passed",
        "audit_warning": "⚠ Warning! Debit/Credit mismatch",
        "tab_ledger": "General Ledger",
        "tab_inventory": "Inventory Balance",
        "ledger_form_heading": "Create a ledger entry",
        "ledger_doc_label": "Document (recorder)",
        "ledger_date_label": "Posting date",
        "ledger_currency_label": "Currency",
        "ledger_dr_account_label": "Debit account (Dr)",
        "ledger_dr_subconto_label": "Debit subconto (JSON / text)",
        "ledger_cr_account_label": "Credit account (Cr)",
        "ledger_cr_subconto_label": "Credit subconto (JSON / text)",
        "ledger_amount_label": "Transaction amount",
        "ledger_post_button": "Post ledger entry",
        "ledger_cancel_heading": "Cancel a ledger posting",
        "ledger_cancel_description": "Reverses a posted ledger document and restores account balances.",
        "ledger_cancel_doc_label": "Document (recorder)",
        "ledger_cancel_button": "Reverse posting",
        "account_types_heading": "Account type guide:",
        "account_type_active": "Active (1xx/2xx/9xx)",
        "account_type_passive": "Passive (4xx/5xx/7xx)",
        "account_type_active_desc": "Increases on Debit, decreases on Credit.",
        "account_type_passive_desc": "Increases on Credit, decreases on Debit.",
        "trial_balance_heading": "Trial Balance",
        "col_account": "Account",
        "col_type": "Type",
        "col_subconto": "Subconto details",
        "col_amount_dr": "Debit (DR)",
        "col_amount_cr": "Credit (CR)",
        "col_balance": "Balance",
        "col_currency": "Currency",
        "totals_label": "Total transaction sums:",
        "totals_match": "✓ Totals match",
        "totals_mismatch": "⚠ Mismatch detected!",
        "total_assets": "Total assets:",
        "total_passives": "Total liabilities & equity:",
        "no_balances": "No ledger balances yet. Post the first entries.",
        "journal_heading": "General Journal",
        "journal_doc": "Document",
        "journal_date": "Date",
        "journal_dr_account": "Debit account",
        "journal_dr_subconto": "Debit subconto",
        "journal_cr_account": "Credit account",
        "journal_cr_subconto": "Credit subconto",
        "journal_amount": "Amount",
        "journal_recorded_at": "Recorded at",
        "journal_empty": "No journal entries yet.",
        "inventory_form_heading": "Post inventory movement",
        "inventory_doc_label": "Document (recorder)",
        "inventory_date_label": "Date",
        "inventory_warehouse_label": "Warehouse",
        "inventory_product_label": "Product",
        "inventory_quantity_label": "Quantity (negative to remove)",
        "inventory_amount_label": "Value",
        "inventory_post_button": "Post inventory movement",
        "inventory_cancel_heading": "Cancel inventory posting",
        "inventory_cancel_doc_label": "Document (recorder)",
        "inventory_cancel_button": "Reverse inventory movement",
        "inventory_balance_heading": "Historical inventory balance",
        "inventory_balance_warehouse_label": "Warehouse",
        "inventory_balance_product_label": "Product",
        "inventory_balance_date_label": "As of date",
        "inventory_balance_button": "Fetch balance",
        "current_balances_heading": "Current inventory balances (O(1) cache)",
        "current_balances_empty": "No inventory data yet. Post the first movement.",
        "movements_heading": "Recent inventory movements",
        "movement_doc": "Document",
        "movement_date": "Date",
        "movement_warehouse": "Warehouse",
        "movement_product": "Product",
        "movement_quantity": "Quantity",
        "movement_amount": "Amount",
        "movement_recorded_at": "Recorded at",
        "movement_empty": "No movements yet.",
        "flash_post_success": "Movement posted successfully to {register}: {recorder}",
        "flash_post_error": "Post error: {error}",
        "flash_unpost_success": "Posting reversed: {recorder}",
        "flash_unpost_error": "Reverse error: {error}",
        "flash_js_error": "Error: ",
        "account_10": "10 Cash & Accounts (Asset)",
        "account_28": "28 Inventory (Asset)",
        "account_90": "90 Rent Expense (Asset/Expense)",
        "account_40": "40 Share Capital (Liability)",
        "account_50": "50 Bank Loans (Liability)",
        "language_english": "English",
        "language_ukrainian": "Українська",
    },
    "uk": {
        "page_title": "pg_accumulator — Бухгалтерський та Складський Демо-Облік",
        "subtitle": "Розширення PostgreSQL для високопродуктивного агрегування та фінансового обліку",
        "registered_registers": "Зареєстровані регістри:",
        "audit_sound": "✓ Баланс Дебет ≡ Кредит Збігається",
        "audit_warning": "⚠ Помилка! Розходження Дебет/Кредит",
        "tab_ledger": "Бухгалтерія",
        "tab_inventory": "Складський облік",
        "ledger_form_heading": "Зробити бухгалтерську проводку",
        "ledger_doc_label": "Документ (recorder)",
        "ledger_date_label": "Дата проведення",
        "ledger_currency_label": "Валюта",
        "ledger_dr_account_label": "Рахунок ДЕБЕТУ (Dr Account)",
        "ledger_dr_subconto_label": "Субконто Дебету (Dr Subconto JSON / text)",
        "ledger_cr_account_label": "Рахунок КРЕДИТУ (Cr Account)",
        "ledger_cr_subconto_label": "Субконто Кредиту (Cr Subconto JSON / text)",
        "ledger_amount_label": "Сума транзакції (Amount)",
        "ledger_post_button": "Провести проводку (Подвійний запис)",
        "ledger_cancel_heading": "Скасувати бухгалтерську операцію",
        "ledger_cancel_description": "Анулює проводку, повертаючи задіяні баланси активів та пасивів у попередній стан за методом сторно.",
        "ledger_cancel_doc_label": "Документ (recorder)",
        "ledger_cancel_button": "Сторнувати документ",
        "account_types_heading": "Довідник типів рахунків:",
        "account_type_active": "Активні (1xx/2xx/9xx)",
        "account_type_passive": "Пасивні (4xx/5xx/7xx)",
        "account_type_active_desc": "Зростають по Дебету, зменшуються по Кредиту.",
        "account_type_passive_desc": "Зростають по Кредиту, зменшуються по Дебету.",
        "trial_balance_heading": "Оборотно-сальдова відомість (Trial Balance)",
        "col_account": "Рахунок",
        "col_type": "Тип",
        "col_subconto": "Детальний аналітичний аналіз (Субконто)",
        "col_amount_dr": "Обороти Дебет (DR)",
        "col_amount_cr": "Обороти Кредит (CR)",
        "col_balance": "Сальдо / Кінцевий баланс",
        "col_currency": "Валюта",
        "totals_label": "Разом оборотів (Транзакційні суми):",
        "totals_match": "✓ Обороти збігаються",
        "totals_mismatch": "⚠ Помилка розбіжності!",
        "total_assets": "Усього Активів:",
        "total_passives": "Усього Пасивів:",
        "no_balances": "Немає бухгалтерських балансів. Проведіть перші проводки.",
        "journal_heading": "Журнал бухгалтерських проводок (General Journal)",
        "journal_doc": "Документ",
        "journal_date": "Дата",
        "journal_dr_account": "Дебет рахунок",
        "journal_dr_subconto": "Субконто дебету",
        "journal_cr_account": "Кредит рахунок",
        "journal_cr_subconto": "Субконто кредиту",
        "journal_amount": "Сума",
        "journal_recorded_at": "Час запису",
        "journal_empty": "Журнал проводок порожній.",
        "inventory_form_heading": "Записати надходження / вибуття на склад",
        "inventory_doc_label": "Документ (recorder)",
        "inventory_date_label": "Дата",
        "inventory_warehouse_label": "Склад",
        "inventory_product_label": "Товар",
        "inventory_quantity_label": "Кількість (для списання вказуйте мінус)",
        "inventory_amount_label": "Сума",
        "inventory_post_button": "Записати на Склад",
        "inventory_cancel_heading": "Скасувати складський документ",
        "inventory_cancel_doc_label": "Документ (recorder)",
        "inventory_cancel_button": "Анулювати складський рух",
        "inventory_balance_heading": "Запит історичного складського залишку",
        "inventory_balance_warehouse_label": "Склад",
        "inventory_balance_product_label": "Товар",
        "inventory_balance_date_label": "На дату",
        "inventory_balance_button": "Отримати залишок",
        "current_balances_heading": "Поточні складські залишки в реальному часі (O(1) cache)",
        "current_balances_empty": "Немає складських даних. Запишіть перший рух вище.",
        "movements_heading": "Останні складські операції (inventory movements)",
        "movement_doc": "Документ",
        "movement_date": "Дата",
        "movement_warehouse": "Склад",
        "movement_product": "Товар",
        "movement_quantity": "Кількість",
        "movement_amount": "Сума",
        "movement_recorded_at": "Записано",
        "movement_empty": "Немає рухів.",
        "flash_post_success": "Рух успішно записано у регістр {register}: {recorder}",
        "flash_post_error": "Помилка проведення операції: {error}",
        "flash_unpost_success": "Проведення документа скасовано: {recorder}",
        "flash_unpost_error": "Помилка скасування документа: {error}",
        "flash_js_error": "Помилка: ",
        "language_english": "English",
        "language_ukrainian": "Українська",
    },
}


def get_conn():
    return psycopg2.connect(DB_DSN)


def parse_locale(request):
    lang = request.args.get("lang") or request.cookies.get("lang")
    if lang and lang in SUPPORTED_LOCALES:
        return lang
    accept = request.headers.get("Accept-Language", "")
    for part in accept.split(","):
        code = part.split(";")[0].strip().lower()
        if code.startswith("uk"):
            return "uk"
        if code.startswith("en"):
            return "en"
    return "en"


def get_strings(lang):
    return TRANSLATIONS.get(lang, TRANSLATIONS["en"])


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    lang = parse_locale(request)
    t = get_strings(lang)

    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # 1. Current warehouse balances (inventory register)
        balances = []
        try:
            cur.execute("SELECT * FROM accum.inventory_balance_cache ORDER BY warehouse, product")
            balances = cur.fetchall()
        except Exception:
            conn.rollback()

        # 2. Recent warehouse movements (last 50)
        movements = []
        try:
            cur.execute("""
                SELECT id, recorder, period, warehouse, product, quantity, amount, recorded_at
                FROM accum.inventory_movements
                ORDER BY recorded_at DESC
                LIMIT 50
            """)
            movements = cur.fetchall()
        except Exception:
            conn.rollback()

        # 3. Bookkeeping Balances / Trial Balance (general_ledger register)
        ledger_balances = []
        try:
            cur.execute("""
                SELECT 
                    account, 
                    subconto, 
                    amount_dr, 
                    amount_cr, 
                    currency,
                    CASE 
                        WHEN account ~ '^(1|2|9)' THEN amount_dr - amount_cr
                        WHEN account ~ '^(4|5|7)' THEN amount_cr - amount_dr
                        ELSE amount_dr - amount_cr
                    END as balance,
                    CASE
                        WHEN account ~ '^(1|2|9)' THEN 'A' -- Active (Asset / Expense)
                        WHEN account ~ '^(4|5|7)' THEN 'P' -- Passive (Liabilities / Equity / Sales)
                        ELSE 'AP'
                    END as acc_type
                FROM accum.general_ledger_balance_cache
                ORDER BY account, subconto
            """)
            ledger_balances = cur.fetchall()
        except Exception:
            conn.rollback()

        # 4. Recent Bookkeeping Movements (last 50)
        ledger_movements = []
        try:
            cur.execute("""
                SELECT id, recorder, period, account_dr, subconto_dr, account_cr, subconto_cr, currency, amount, recorded_at
                FROM accum.general_ledger_movements
                ORDER BY recorded_at DESC
                LIMIT 50
            """)
            ledger_movements = cur.fetchall()
        except Exception:
            conn.rollback()

        # 5. Global General Ledger Soundness Audit (Debit == Credit checking)
        ledger_sound = True
        try:
            cur.execute("SELECT accum.register_ledger_verify('general_ledger')")
            row = cur.fetchone()
            if row:
                ledger_sound = list(row.values())[0]
        except Exception:
            conn.rollback()

        # Registers list
        registers = []
        try:
            cur.execute("SELECT * FROM accum.registers ORDER BY name")
            registers = cur.fetchall()
        except Exception:
            conn.rollback()

    return render_template(
        "index_i18n.html",
        balances=balances,
        movements=movements,
        registers=registers,
        ledger_balances=ledger_balances,
        ledger_movements=ledger_movements,
        ledger_sound=ledger_sound,
        today=str(date.today()),
        lang=lang,
        t=t,
    )


@app.route("/post", methods=["POST"])
def post_movement():
    lang = request.form.get("lang") or request.args.get("lang")
    register = request.form.get("register", "inventory")
    recorder = request.form["recorder"]
    period = request.form.get("period") or str(date.today())

    if register == "general_ledger":
        account_dr = request.form["account_dr"]
        account_cr = request.form["account_cr"]
        currency = request.form.get("currency", "USD")
        amount = float(request.form["amount"])

        # Safely parse subconto
        s_dr = request.form.get("subconto_dr", "{}").strip()
        if not s_dr:
            subconto_dr = {}
        elif s_dr.startswith("{"):
            try:
                subconto_dr = json.loads(s_dr)
            except Exception:
                subconto_dr = {"note": s_dr}
        else:
            subconto_dr = {"name": s_dr}

        s_cr = request.form.get("subconto_cr", "{}").strip()
        if not s_cr:
            subconto_cr = {}
        elif s_cr.startswith("{"):
            try:
                subconto_cr = json.loads(s_cr)
            except Exception:
                subconto_cr = {"note": s_cr}
        else:
            subconto_cr = {"name": s_cr}

        movement = {
            "recorder": recorder,
            "period": period,
            "account_dr": account_dr,
            "subconto_dr": subconto_dr,
            "account_cr": account_cr,
            "subconto_cr": subconto_cr,
            "currency": currency,
            "amount": amount,
        }
    else:
        warehouse = int(request.form["warehouse"])
        product = int(request.form["product"])
        quantity = float(request.form["quantity"])
        amount = float(request.form["amount"])

        movement = {
            "recorder": recorder,
            "period": period,
            "warehouse": warehouse,
            "product": product,
            "quantity": quantity,
            "amount": amount,
        }

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT accum.register_post(%s, %s::jsonb)",
                (register, json.dumps(movement)),
            )
            conn.commit()
            lang = lang if lang in SUPPORTED_LOCALES else parse_locale(request)
            t = get_strings(lang)
            flash(t["flash_post_success"].format(register=register, recorder=recorder), "success")
    except Exception as e:
        lang = lang if lang in SUPPORTED_LOCALES else parse_locale(request)
        t = get_strings(lang)
        flash(t["flash_post_error"].format(error=e), "error")

    return redirect(url_for("index", lang=lang))


@app.route("/unpost", methods=["POST"])
def unpost_movement():
    lang = request.form.get("lang") or request.args.get("lang")
    register = request.form.get("register", "inventory")
    recorder = request.form["recorder"]

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT accum.register_unpost(%s, %s)", (register, recorder))
            conn.commit()
            lang = lang if lang in SUPPORTED_LOCALES else parse_locale(request)
            t = get_strings(lang)
            flash(t["flash_unpost_success"].format(recorder=recorder), "success")
    except Exception as e:
        lang = lang if lang in SUPPORTED_LOCALES else parse_locale(request)
        t = get_strings(lang)
        flash(t["flash_unpost_error"].format(error=e), "error")

    return redirect(url_for("index", lang=lang))


@app.route("/balance")
def balance():
    warehouse = request.args.get("warehouse", type=int)
    product = request.args.get("product", type=int)
    at_date = request.args.get("at_date")

    dims = {}
    if warehouse:
        dims["warehouse"] = warehouse
    if product:
        dims["product"] = product

    try:
        with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if at_date:
                cur.execute(
                    "SELECT * FROM accum.inventory_balance(dimensions := %s::jsonb, at_date := %s::timestamptz)",
                    (json.dumps(dims), at_date),
                )
            else:
                cur.execute(
                    "SELECT * FROM accum.inventory_balance(dimensions := %s::jsonb)",
                    (json.dumps(dims),),
                )
            result = cur.fetchall()
            return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/turnover")
def turnover():
    warehouse = request.args.get("warehouse", type=int)
    product = request.args.get("product", type=int)
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")

    dims = {}
    if warehouse:
        dims["warehouse"] = warehouse
    if product:
        dims["product"] = product

    try:
        with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            sql = "SELECT * FROM accum.inventory_turnover(dimensions := %s::jsonb"
            params = [json.dumps(dims)]
            if date_from:
                sql += ", date_from := %s::timestamptz"
                params.append(date_from)
            if date_to:
                sql += ", date_to := %s::timestamptz"
                params.append(date_to)
            sql += ")"
            cur.execute(sql, params)
            result = cur.fetchall()
            return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/registers")
def api_registers():
    try:
        with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM accum.registers ORDER BY name")
            return jsonify(cur.fetchall())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3301, debug=True)
