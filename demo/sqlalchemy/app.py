"""
pg_accumulator + sqlalchemy-accumulator Demo Application
========================================================

A Flask app demonstrating sqlalchemy-accumulator alongside standard
SQLAlchemy ORM models.  Shows how accumulation registers and ORM
entities (Products, Warehouses, Clients, Orders) coexist in one
transaction — the key selling point for real-world applications.
"""

import os
from datetime import date, datetime
from decimal import Decimal

from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from sqlalchemy import create_engine, Column, Integer, String, Numeric, Boolean, ForeignKey, DateTime, func
from sqlalchemy.orm import DeclarativeBase, Session, relationship, sessionmaker

from sqlalchemy_accumulator import (
    AccumulatorClient,
    define_register,
    AccumulatorError,
)

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "demo-secret-key")

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://dev:dev_password@postgres:5432/accumulator_dev",
)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine)
accum = AccumulatorClient(engine)


# ---------------------------------------------------------------------------
# ORM Models — standard SQLAlchemy entities
# ---------------------------------------------------------------------------

class Base(DeclarativeBase):
    pass


class Warehouse(Base):
    __tablename__ = "warehouses"
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False)
    address = Column(String(255))
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Product(Base):
    __tablename__ = "products"
    id = Column(Integer, primary_key=True)
    sku = Column(String(50), nullable=False, unique=True)
    name = Column(String(200), nullable=False)
    unit_price = Column(Numeric(18, 2), default=0)
    category = Column(String(100))
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class Client(Base):
    __tablename__ = "clients"
    id = Column(Integer, primary_key=True)
    name = Column(String(200), nullable=False)
    email = Column(String(200))
    phone = Column(String(50))
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    orders = relationship("Order", back_populates="client")


class Order(Base):
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey("clients.id"))
    warehouse_id = Column(Integer, ForeignKey("warehouses.id"))
    status = Column(String(20), default="draft")
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    client = relationship("Client", back_populates="orders")
    warehouse = relationship("Warehouse")
    lines = relationship("OrderLine", back_populates="order", cascade="all, delete-orphan")


class OrderLine(Base):
    __tablename__ = "order_lines"
    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("orders.id", ondelete="CASCADE"))
    product_id = Column(Integer, ForeignKey("products.id"))
    quantity = Column(Numeric(18, 4), nullable=False)
    unit_price = Column(Numeric(18, 2), nullable=False)
    amount = Column(Numeric(18, 2), nullable=False)
    order = relationship("Order", back_populates="lines")
    product = relationship("Product")

# ---------------------------------------------------------------------------
# Register definition — define once, use everywhere
# ---------------------------------------------------------------------------

inventory = define_register(
    name="inventory",
    kind="balance",
    dimensions={"warehouse": "int", "product": "int"},
    resources={"quantity": "numeric(18,4)", "amount": "numeric(18,2)"},
    totals_period="day",
)


# ---------------------------------------------------------------------------
# JSON serializer helper
# ---------------------------------------------------------------------------

def json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (date, datetime)):
        return obj.isoformat()
    return str(obj)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    error = None
    registers = []
    balances = []
    movements = []
    warehouses = []
    products = []
    clients = []
    orders = []

    try:
        registers = accum.list_registers()
    except Exception as e:
        error = str(e)

    # Fetch movements using the read API
    try:
        handle = accum.use(inventory)
        movements = handle.movements(limit=50)
    except Exception:
        pass

    # Get balances via raw SQL (balance_cache is the fastest)
    balance_rows = []
    try:
        with engine.connect() as conn:
            from sqlalchemy import text
            result = conn.execute(
                text("SELECT * FROM accum.inventory_balance_cache ORDER BY warehouse, product")
            )
            balance_rows = [dict(r._mapping) for r in result]
    except Exception:
        pass

    # ORM entities
    with SessionLocal() as session:
        warehouses = session.query(Warehouse).order_by(Warehouse.id).all()
        products = session.query(Product).order_by(Product.id).all()
        clients = session.query(Client).order_by(Client.id).all()
        orders = (
            session.query(Order)
            .order_by(Order.created_at.desc())
            .limit(20)
            .all()
        )

        # Build lookup dicts for template
        wh_map = {w.id: w.name for w in warehouses}
        pr_map = {p.id: p.name for p in products}

        # Enrich balance rows with names
        for b in balance_rows:
            b["warehouse_name"] = wh_map.get(b.get("warehouse"), "?")
            b["product_name"] = pr_map.get(b.get("product"), "?")

        # Enrich movements with names
        for m in movements:
            m["warehouse_name"] = wh_map.get(m.get("warehouse"), "?")
            m["product_name"] = pr_map.get(m.get("product"), "?")

        return render_template(
            "index.html",
            registers=registers,
            balances=balance_rows,
            movements=movements,
            warehouses=warehouses,
            products=products,
            clients=clients,
            orders=orders,
            error=error,
        )


@app.route("/post", methods=["POST"])
def post_movement():
    try:
        recorder = request.form["recorder"]
        period = request.form.get("period") or str(date.today())
        warehouse = int(request.form["warehouse"])
        product = int(request.form["product"])
        quantity = float(request.form["quantity"])
        amount = float(request.form["amount"])

        handle = accum.use(inventory)
        count = handle.post({
            "recorder": recorder,
            "period": period,
            "warehouse": warehouse,
            "product": product,
            "quantity": quantity,
            "amount": amount,
        })
        flash(f"Posted successfully: {recorder} ({count} movement)", "success")
    except AccumulatorError as e:
        flash(f"Accumulator error: {e}", "error")
    except Exception as e:
        flash(f"Error: {e}", "error")

    return redirect(url_for("index"))


@app.route("/unpost", methods=["POST"])
def unpost_movement():
    try:
        recorder = request.form["recorder"]
        handle = accum.use(inventory)
        count = handle.unpost(recorder)
        flash(f"Unposted: {recorder} ({count} movement(s) removed)", "success")
    except AccumulatorError as e:
        flash(f"Accumulator error: {e}", "error")
    except Exception as e:
        flash(f"Error: {e}", "error")

    return redirect(url_for("index"))


@app.route("/repost", methods=["POST"])
def repost_movement():
    try:
        recorder = request.form["recorder"]
        period = request.form.get("period") or str(date.today())
        warehouse = int(request.form["warehouse"])
        product = int(request.form["product"])
        quantity = float(request.form["quantity"])
        amount = float(request.form["amount"])

        handle = accum.use(inventory)
        count = handle.repost(recorder, {
            "recorder": recorder,
            "period": period,
            "warehouse": warehouse,
            "product": product,
            "quantity": quantity,
            "amount": amount,
        })
        flash(f"Reposted: {recorder} ({count} movement)", "success")
    except AccumulatorError as e:
        flash(f"Accumulator error: {e}", "error")
    except Exception as e:
        flash(f"Error: {e}", "error")

    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# ORM entity routes
# ---------------------------------------------------------------------------

@app.route("/warehouses/add", methods=["POST"])
def add_warehouse():
    try:
        with SessionLocal() as session:
            wh = Warehouse(
                name=request.form["name"],
                address=request.form.get("address", ""),
            )
            session.add(wh)
            session.commit()
            flash(f"Warehouse created: {wh.name} (id={wh.id})", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for("index"))


@app.route("/products/add", methods=["POST"])
def add_product():
    try:
        with SessionLocal() as session:
            p = Product(
                sku=request.form["sku"],
                name=request.form["name"],
                unit_price=Decimal(request.form.get("unit_price", "0")),
                category=request.form.get("category", ""),
            )
            session.add(p)
            session.commit()
            flash(f"Product created: {p.name} (id={p.id})", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for("index"))


@app.route("/clients/add", methods=["POST"])
def add_client():
    try:
        with SessionLocal() as session:
            c = Client(
                name=request.form["name"],
                email=request.form.get("email", ""),
                phone=request.form.get("phone", ""),
            )
            session.add(c)
            session.commit()
            flash(f"Client created: {c.name} (id={c.id})", "success")
    except Exception as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for("index"))


@app.route("/orders/create", methods=["POST"])
def create_order():
    """
    Create an order AND post inventory movements — in ONE transaction.
    This is the key demo: ORM + pg_accumulator in the same session.
    """
    try:
        with SessionLocal() as session:
            client_id = int(request.form["client_id"])
            warehouse_id = int(request.form["warehouse_id"])
            product_id = int(request.form["product_id"])
            quantity = Decimal(request.form["quantity"])

            # Look up product price
            product = session.query(Product).get(product_id)
            if not product:
                flash("Product not found", "error")
                return redirect(url_for("index"))

            amount = quantity * product.unit_price

            # 1) Create ORM order
            order = Order(
                client_id=client_id,
                warehouse_id=warehouse_id,
                status="posted",
            )
            session.add(order)
            session.flush()  # get order.id

            line = OrderLine(
                order_id=order.id,
                product_id=product_id,
                quantity=quantity,
                unit_price=product.unit_price,
                amount=amount,
            )
            session.add(line)

            # 2) Post accumulator movement — SAME transaction
            accum_session = AccumulatorClient(session)
            handle = accum_session.use(inventory)
            handle.post({
                "recorder": f"order:{order.id}",
                "period": str(date.today()),
                "warehouse": warehouse_id,
                "product": product_id,
                "quantity": float(-quantity),   # shipment = negative
                "amount": float(-amount),
            })

            session.commit()
            flash(
                f"Order #{order.id} created & posted — "
                f"{quantity} x {product.name} shipped from warehouse {warehouse_id}",
                "success",
            )
    except AccumulatorError as e:
        flash(f"Accumulator error: {e}", "error")
    except Exception as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for("index"))


@app.route("/orders/<int:order_id>/cancel", methods=["POST"])
def cancel_order(order_id):
    """Cancel an order: update ORM status + unpost accumulator movements."""
    try:
        with SessionLocal() as session:
            order = session.query(Order).get(order_id)
            if not order:
                flash("Order not found", "error")
                return redirect(url_for("index"))

            # 1) Unpost accumulator movement
            accum_session = AccumulatorClient(session)
            handle = accum_session.use(inventory)
            handle.unpost(f"order:{order.id}")

            # 2) Update ORM status
            order.status = "cancelled"
            session.commit()
            flash(f"Order #{order.id} cancelled — inventory restored", "success")
    except AccumulatorError as e:
        flash(f"Accumulator error: {e}", "error")
    except Exception as e:
        flash(f"Error: {e}", "error")
    return redirect(url_for("index"))


@app.route("/api/balance")
def api_balance():
    warehouse = request.args.get("warehouse", type=int)
    product = request.args.get("product", type=int)
    at_date = request.args.get("at_date")

    dims = {}
    if warehouse is not None:
        dims["warehouse"] = warehouse
    if product is not None:
        dims["product"] = product

    try:
        handle = accum.use(inventory)
        result = handle.balance(at_date=at_date, **dims)
        return jsonify({"balance": result, "filters": dims, "at_date": at_date})
    except AccumulatorError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/turnover")
def api_turnover():
    warehouse = request.args.get("warehouse", type=int)
    product = request.args.get("product", type=int)
    date_from = request.args.get("date_from")
    date_to = request.args.get("date_to")

    dims = {}
    if warehouse is not None:
        dims["warehouse"] = warehouse
    if product is not None:
        dims["product"] = product

    try:
        handle = accum.use(inventory)
        result = handle.turnover(
            date_from=date_from,
            date_to=date_to,
            **dims,
        )
        return jsonify({"turnover": result, "filters": dims})
    except AccumulatorError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/movements")
def api_movements():
    recorder = request.args.get("recorder")
    limit = request.args.get("limit", 50, type=int)
    offset = request.args.get("offset", 0, type=int)

    try:
        handle = accum.use(inventory)
        result = handle.movements(recorder=recorder, limit=limit, offset=offset)
        return jsonify({"movements": result, "limit": limit, "offset": offset})
    except AccumulatorError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/registers")
def api_registers():
    try:
        registers = accum.list_registers()
        return jsonify([
            {
                "name": r.name,
                "kind": r.kind,
                "dimensions": r.dimensions,
                "resources": r.resources,
                "movements_count": r.movements_count,
                "created_at": r.created_at,
            }
            for r in registers
        ])
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/register-info/<name>")
def api_register_info(name):
    try:
        info = accum.register_info(name)
        return jsonify({
            "name": info.name,
            "kind": info.kind,
            "dimensions": info.dimensions,
            "resources": info.resources,
            "totals_period": info.totals_period,
            "partition_by": info.partition_by,
            "high_write": info.high_write,
            "recorder_type": info.recorder_type,
            "movements_count": info.movements_count,
            "created_at": info.created_at,
        })
    except AccumulatorError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=3304, debug=True)
