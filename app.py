from flask import Flask, request, jsonify
import psycopg2
import os
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

# Supabase connection settings from Railway variables
DB_HOST     = os.environ.get("DB_HOST")
DB_PORT     = int(os.environ.get("DB_PORT", 5432))
DB_NAME     = os.environ.get("DB_NAME")
DB_USER     = os.environ.get("DB_USER")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "")

def get_db():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )

def init_db():
    try:
        conn = get_db()
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id TEXT PRIMARY KEY,
                order_time TIMESTAMP,
                property_name TEXT,
                gross_sales REAL,
                tax REAL,
                tips REAL,
                delivery_charges REAL,
                service_charges REAL,
                additional_charges REAL,
                charges REAL,
                revenue REAL,
                refunds REAL,
                discounts REAL,
                dispatch_type TEXT,
                payment_method TEXT,
                sales_channel_type TEXT,
                sales_channel_name TEXT,
                is_preorder TEXT,
                status TEXT,
                raw_json TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS order_items (
                id SERIAL PRIMARY KEY,
                order_id TEXT,
                order_time TIMESTAMP,
                item_name TEXT,
                category TEXT,
                price REAL,
                quantity INTEGER,
                revenue REAL
            )
        """)
        conn.commit()
        conn.close()
        log.info("Database initialized successfully!")
    except Exception as e:
        log.error(f"Database init error: {e}")

def save_order(order):
    try:
        conn = get_db()
        cursor = conn.cursor()

        order_id = str(order.get("OrderId", ""))
        if not order_id:
            return False

        # Check if already exists
        cursor.execute(
            "SELECT 1 FROM orders WHERE order_id = %s",
            (order_id,)
        )
        if cursor.fetchone():
            conn.close()
            return False

        dispatch_map = {
            "Pickup": "Collection",
            "DineIn": "Dine In",
            "TableService": "Dine In"
        }
        raw_dispatch = order.get("DeliveryType", "Unknown")

        cursor.execute("""
            INSERT INTO orders (
                order_id, order_time, property_name, gross_sales, tax, tips,
                delivery_charges, service_charges, additional_charges, charges,
                revenue, refunds, discounts, dispatch_type, payment_method,
                sales_channel_type, sales_channel_name, is_preorder, status, raw_json
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO NOTHING
        """, (
            order_id,
            order.get("PlacedTime"),
            order.get("Store", {}).get("Name", "Chocoberry Cardiff"),
            float(order.get("OrderItemsAmount", 0) or 0),
            float(order.get("TotalTax", 0) or 0),
            float(order.get("TipAmount", 0) or 0),
            float(order.get("DeliveryAmount", 0) or 0),
            float(order.get("ServiceChargeAmount", 0) or 0),
            float(order.get("ProcessingFee", 0) or 0),
            0.0,
            float(order.get("Amount", 0) or 0),
            float(order.get("RefundedAmount", 0) or 0),
            float((order.get("Voucher") or {}).get("Amount", 0) or 0),
            dispatch_map.get(raw_dispatch, raw_dispatch),
            order.get("PaymentAccountType", "Unknown"),
            order.get("AppType", "Unknown"),
            (order.get("Channel") or {}).get("Source", "Unknown"),
            "Yes" if order.get("IsPreOrder") else "No",
            order.get("OrderState", "Unknown"),
            str(order)
        ))

        # Save order items
        for item in order.get("OrderItems", []):
            cursor.execute("""
                INSERT INTO order_items (
                    order_id, order_time, item_name, category,
                    price, quantity, revenue
                ) VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                order_id,
                order.get("PlacedTime"),
                item.get("Name", "Unknown"),
                item.get("MenuSectionName", "Unknown"),
                float(item.get("Price", 0) or 0),
                1,
                float(item.get("PriceIncludingOptionSetItems", 0) or 0)
            ))

        conn.commit()
        conn.close()
        log.info(f"New order saved to Supabase: {order_id}")
        return True

    except Exception as e:
        log.error(f"Error saving order: {e}")
        return False

# Initialize database on startup
init_db()

@app.route("/webhook", methods=["POST"])
def webhook():
    token = request.headers.get("X-Verify-Token", "")
    if token != VERIFY_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401

    data = request.json
    log.info(f"Webhook received: {data}")

    order = data.get("Body", data)

    if order:
        saved = save_order(order)
        if saved:
            return jsonify({"status": "saved"}), 200
        else:
            return jsonify({"status": "already exists"}), 200

    return jsonify({"status": "no order found"}), 200

@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "running"}), 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
```

---

