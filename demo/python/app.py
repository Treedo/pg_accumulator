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


def get_conn():
    return psycopg2.connect(DB_DSN)


# ---------------------------------------------------------------------------
# Pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    with get_conn() as conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        # Current balances
        balances = []
        try:
            cur.execute("SELECT * FROM accum.inventory_balance_cache ORDER BY warehouse, product")
            balances = cur.fetchall()
        except Exception:
            conn.rollback()

        # Recent movements (last 50)
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

        # Registers list
        registers = []
        try:
            cur.execute("SELECT * FROM accum.registers ORDER BY name")
            registers = cur.fetchall()
        except Exception:
            conn.rollback()

    return render_template("index.html", balances=balances, movements=movements, registers=registers)


@app.route("/post", methods=["POST"])
def post_movement():
    register = request.form.get("register", "inventory")
    recorder = request.form["recorder"]
    period = request.form.get("period") or str(date.today())
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
            flash(f"Рух записано: {recorder}", "success")
    except Exception as e:
        flash(f"Помилка: {e}", "error")

    return redirect(url_for("index"))


@app.route("/unpost", methods=["POST"])
def unpost_movement():
    register = request.form.get("register", "inventory")
    recorder = request.form["recorder"]

    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute("SELECT accum.register_unpost(%s, %s)", (register, recorder))
            conn.commit()
            flash(f"Документ скасовано: {recorder}", "success")
    except Exception as e:
        flash(f"Помилка: {e}", "error")

    return redirect(url_for("index"))


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
    app.run(host="0.0.0.0", port=5000, debug=True)
