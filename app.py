from flask import Flask, request, jsonify
import sqlite3
import os
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

DB_PATH = os.environ.get("DB_PATH", "restaurant_data.db")
VERIFY_TOKEN = os.environ.get("VERIFY_TOKEN", "chocoberry123")

def save_order(order):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        order_id = str(order.get("OrderId", ""))
        
        # Skip if exists
        cursor.execute("SELECT 1 FROM orders WHERE order_id = ?", (order_id,))
        if cursor.fetchone():
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    order_id, order_time, item_name, category, price, quantity, revenue
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
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
        log.info(f"New order saved: {order_id}")
        return True
        
    except Exception as e:
        log.error(f"Error saving order: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()

@app.route("/webhook", methods=["POST"])
def webhook():
    # Verify token
    token = request.headers.get("X-Verify-Token", "")
    if token != VERIFY_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    
    data = request.json
    log.info(f"Webhook received: {data}")
    
    # Flipdish sends order inside "Body" field
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
