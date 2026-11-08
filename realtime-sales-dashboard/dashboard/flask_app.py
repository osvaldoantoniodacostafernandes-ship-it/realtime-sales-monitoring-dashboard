# ==============================================================================
# dashboard/flask_app.py
# Real-Time Sales Monitoring Web Dashboard (Flask + Chart.js)
#
# Memenuhi rekomendasi manual kurikulum DIT 2026:
# - Tabel 6: Rekomendasi Dashboard Web (Streamlit / Flask)
# - Bab 5.6: Visualisasi Real-Time, KPI, Charts, Alerts
#
# Jalankan:
#   python dashboard/flask_app.py
# Buka di browser:
#   http://localhost:5000
# ==============================================================================

import os
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from flask import Flask, render_template, jsonify
import mysql.connector

from config.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

app = Flask(__name__, template_folder="templates")


def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE,
    )


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/stats")
def api_stats():
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        # 1. KPIs
        cur.execute("""
            SELECT 
                COUNT(*) as total_txns,
                COALESCE(SUM(total_amount), 0) as total_revenue,
                COALESCE(SUM(quantity), 0) as total_units,
                COALESCE(AVG(total_amount), 0) as avg_order_val,
                COALESCE(SUM(estimated_profit), 0) as total_profit
            FROM transactions;
        """)
        kpi = cur.fetchone()

        # 2. Anomalies count
        cur.execute("SELECT COUNT(*) as anomaly_count FROM ml_anomaly_alerts;")
        anom_res = cur.fetchone()
        kpi["total_anomalies"] = anom_res["anomaly_count"] if anom_res else 0

        # 3. Category Breakdown
        cur.execute("""
            SELECT category, SUM(quantity) as units, ROUND(SUM(total_amount), 2) as revenue
            FROM transactions
            GROUP BY category
            ORDER BY revenue DESC;
        """)
        categories = cur.fetchall()

        # 4. Payment Method
        cur.execute("""
            SELECT payment_method, COUNT(*) as orders, ROUND(SUM(total_amount), 2) as revenue
            FROM transactions
            GROUP BY payment_method;
        """)
        payments = cur.fetchall()

        # 5. Latest 12 Transactions
        cur.execute("""
            SELECT 
                transaction_id, DATE_FORMAT(transaction_date, '%Y-%m-%d %H:%i:%s') as t_date,
                product_name, category, quantity, price, total_amount, payment_method, 
                supplier, estimated_profit, is_high_value
            FROM transactions
            ORDER BY transaction_date DESC
            LIMIT 12;
        """)
        transactions = cur.fetchall()

        # 6. ML Forecast
        cur.execute("""
            SELECT 
                DATE_FORMAT(forecast_time, '%H:%i') as f_time,
                predicted_revenue, lower_bound, upper_bound
            FROM ml_sales_forecast
            ORDER BY forecast_time ASC;
        """)
        forecast = cur.fetchall()

        # 7. Recent Anomaly Alerts
        cur.execute("""
            SELECT 
                transaction_id, DATE_FORMAT(transaction_date, '%H:%i:%s') as a_time,
                product_name, total_amount, anomaly_score, anomaly_type
            FROM ml_anomaly_alerts
            ORDER BY id DESC
            LIMIT 5;
        """)
        anomalies = cur.fetchall()

        # 8. Top 5 Products
        cur.execute("""
            SELECT product_name, category, SUM(quantity) as units, ROUND(SUM(total_amount), 2) as revenue
            FROM transactions
            GROUP BY product_name, category
            ORDER BY revenue DESC
            LIMIT 5;
        """)
        top_products = cur.fetchall()

        cur.close()
        conn.close()

        # Format floats
        for k in ["total_revenue", "avg_order_val", "total_profit"]:
            kpi[k] = float(kpi[k])
        for c in categories:
            c["revenue"] = float(c["revenue"])
            c["units"] = int(c["units"])
        for p in payments:
            p["revenue"] = float(p["revenue"])
        for t in transactions:
            t["price"] = float(t["price"])
            t["total_amount"] = float(t["total_amount"])
            t["estimated_profit"] = float(t["estimated_profit"])
        for f in forecast:
            f["predicted_revenue"] = float(f["predicted_revenue"])
            f["lower_bound"] = float(f["lower_bound"])
            f["upper_bound"] = float(f["upper_bound"])
        for a in anomalies:
            a["total_amount"] = float(a["total_amount"])
            a["anomaly_score"] = float(a["anomaly_score"])
        for tp in top_products:
            tp["revenue"] = float(tp["revenue"])
            tp["units"] = int(tp["units"])

        return jsonify({
            "status": "success",
            "kpi": kpi,
            "categories": categories,
            "payments": payments,
            "transactions": transactions,
            "forecast": forecast,
            "anomalies": anomalies,
            "top_products": top_products,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    import threading
    import webbrowser

    print("=" * 60)
    print("FLASK REAL-TIME SALES DASHBOARD (DIT 2026)")
    print("Server running at: http://localhost:5000")
    print("Opening browser automatically in 1.5 seconds...")
    print("=" * 60)

    # Automatically open browser
    def open_browser():
        import time
        time.sleep(1.5)
        webbrowser.open("http://localhost:5000")

    threading.Thread(target=open_browser, daemon=True).start()

    app.run(host="0.0.0.0", port=5000, debug=False)
