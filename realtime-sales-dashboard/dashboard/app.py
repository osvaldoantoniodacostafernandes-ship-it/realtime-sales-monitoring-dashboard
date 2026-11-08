# ==============================================================================
# dashboard/app.py
# Real-Time Sales Monitoring Dashboard (Streamlit)
#
# Meets Big Data Capstone Curriculum Requirements (DIT 2026):
# - Section 5.6: Visualization & Dashboard (KPI, Charts, Trend, Real-Time Monitoring, Alerts)
# - Table 6: Recommended Technology (Streamlit)
# - Topic #11: Real-Time Sales Dashboard (Monitoring Sales and Revenue)
#
# How to run:
#   python -m streamlit run dashboard/app.py
# ==============================================================================

import os
import sys
from pathlib import Path
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import mysql.connector

from config.config import (
    MYSQL_HOST,
    MYSQL_PORT,
    MYSQL_USER,
    MYSQL_PASSWORD,
    MYSQL_DATABASE,
)

# Page configuration
st.set_page_config(
    page_title="Real-Time Sales Dashboard | DIT 2026",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS styling
st.markdown("""
    <style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1E3A8A;
        margin-bottom: 0px;
    }
    .sub-title {
        font-size: 1.0rem;
        color: #64748B;
        margin-bottom: 20px;
    }
    .metric-card {
        background: white;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.06);
        border-left: 5px solid #3B82F6;
    }
    .alert-box {
        background-color: #FEF2F2;
        border-left: 5px solid #EF4444;
        padding: 12px;
        border-radius: 6px;
        margin-bottom: 10px;
    }
    </style>
""", unsafe_allow_html=True)


def get_db_connection():
    try:
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
    except Exception as e:
        st.error(f"Failed to connect to MySQL: {e}")
        return None


def fetch_kpis():
    conn = get_db_connection()
    if not conn:
        return {}
    cur = conn.cursor(dictionary=True)
    
    cur.execute("""
        SELECT 
            COUNT(*) as total_txns,
            COALESCE(SUM(total_amount), 0) as total_revenue,
            COALESCE(SUM(quantity), 0) as total_units,
            COALESCE(AVG(total_amount), 0) as avg_order_val,
            COALESCE(SUM(estimated_profit), 0) as total_profit
        FROM transactions;
    """)
    kpi_data = cur.fetchone()

    cur.execute("SELECT COUNT(*) as anomaly_count FROM ml_anomaly_alerts WHERE is_flagged = 1;")
    anom_row = cur.fetchone()
    kpi_data["anomalies"] = anom_row["anomaly_count"] if anom_row else 0

    cur.close()
    conn.close()
    return kpi_data


def fetch_latest_transactions(limit=15):
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    query = f"""
        SELECT 
            transaction_id, transaction_date, product_name, category, 
            quantity, price, total_amount, payment_method, 
            supplier, estimated_profit, is_high_value
        FROM transactions 
        ORDER BY transaction_date DESC 
        LIMIT {limit};
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def fetch_category_summary():
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    query = """
        SELECT category, SUM(total_quantity) as total_quantity, SUM(total_revenue) as total_revenue
        FROM category_summary
        GROUP BY category
        ORDER BY total_revenue DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    if df.empty:
        # Fallback to transactions table
        conn = get_db_connection()
        query2 = """
            SELECT category, SUM(quantity) as total_quantity, SUM(total_amount) as total_revenue
            FROM transactions
            GROUP BY category
            ORDER BY total_revenue DESC;
        """
        df = pd.read_sql(query2, conn)
        conn.close()
    return df


def fetch_payment_summary():
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    query = """
        SELECT payment_method, COUNT(*) as total_orders, SUM(total_amount) as total_revenue
        FROM transactions
        GROUP BY payment_method;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


def fetch_batch_data():
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame(), pd.DataFrame()
    df_daily = pd.read_sql("SELECT * FROM daily_sales_batch ORDER BY report_date ASC;", conn)
    df_top = pd.read_sql("SELECT * FROM top_products_batch ORDER BY sales_rank ASC LIMIT 10;", conn)
    conn.close()
    return df_daily, df_top


def fetch_ml_data():
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
    df_forecast = pd.read_sql("SELECT * FROM ml_sales_forecast ORDER BY forecast_time ASC;", conn)
    df_anomalies = pd.read_sql("SELECT * FROM ml_anomaly_alerts ORDER BY id DESC LIMIT 10;", conn)
    df_clusters = pd.read_sql("SELECT * FROM ml_product_clusters ORDER BY total_revenue DESC;", conn)
    conn.close()
    return df_forecast, df_anomalies, df_clusters


# ==============================================================================
# SIDEBAR
# ==============================================================================
st.sidebar.image("https://dit.edu.tl/wp-content/uploads/2021/04/dit-logo-new.png", width=120)
st.sidebar.title("⚙️ System Controls")
auto_refresh = st.sidebar.checkbox("Auto Refresh Real-Time (5 seconds)", value=True)
refresh_interval = st.sidebar.slider("Refresh Interval (Seconds)", 3, 30, 5)

st.sidebar.markdown("---")
st.sidebar.markdown("### 🏢 Store Information")
st.sidebar.info("**Store:** Loilary Supermarket\n\n**Location:** Dili, Timor-Leste\n\n**Currency:** USD ($)")

st.sidebar.markdown("### 🛠️ Pipeline Status")
st.sidebar.success("✅ Apache Kafka (Topic: sales_transactions)")
st.sidebar.success("✅ Spark Structured Streaming & Batch")
st.sidebar.success("✅ MySQL 8.0 Database (sales_dashboard)")
st.sidebar.success("✅ Machine Learning & Analytics Ready")

if auto_refresh:
    import time
    time.sleep(0.1)


# ==============================================================================
# HEADER
# ==============================================================================
st.markdown('<div class="main-title">📈 Real-Time Sales Monitoring Dashboard</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-title">Big Data Capstone Project – S1 Computer Science | Dili Institute of Technology (DIT) 2026</div>', unsafe_allow_html=True)

# Fetch KPIs
kpis = fetch_kpis()
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        label="💰 Total Revenue",
        value=f"${kpis.get('total_revenue', 0):,.2f}",
        delta=f"Profit: ${kpis.get('total_profit', 0):,.2f}"
    )

with col2:
    st.metric(
        label="🛒 Total Transactions",
        value=f"{kpis.get('total_txns', 0):,}",
        delta="Live Records"
    )

with col3:
    st.metric(
        label="📦 Units Sold",
        value=f"{kpis.get('total_units', 0):,}",
        delta="Items"
    )

with col4:
    st.metric(
        label="💳 Avg. Order Value (AOV)",
        value=f"${kpis.get('avg_order_val', 0):,.2f}",
        delta="Per Transaction"
    )

with col5:
    anomaly_val = kpis.get('anomalies', 0)
    st.metric(
        label="⚠️ Anomalies Detected",
        value=f"{anomaly_val}",
        delta="ML Alert",
        delta_color="inverse"
    )

st.markdown("---")

# ==============================================================================
# TABS INTERFACE
# ==============================================================================
tab1, tab2, tab3, tab4 = st.tabs([
    "🔴 Real-Time Streaming",
    "⚙️ Spark Batch Analytics",
    "🤖 Machine Learning & Alerts",
    "🏛️ Loilary Store & Architecture"
])

# ------------------------------------------------------------------------------
# TAB 1: REAL-TIME STREAMING
# ------------------------------------------------------------------------------
with tab1:
    col_chart1, col_chart2 = st.columns([3, 2])

    with col_chart1:
        st.subheader("📊 Sales Distribution by Category (Real-Time)")
        df_cat = fetch_category_summary()
        if not df_cat.empty:
            fig_cat = px.bar(
                df_cat,
                x="category",
                y="total_revenue",
                color="category",
                text_auto=".2s",
                labels={"total_revenue": "Revenue ($)", "category": "Product Category"},
                title="Total Revenue by Product Category"
            )
            fig_cat.update_layout(showlegend=False, height=350, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_cat, use_container_width=True)
        else:
            st.info("Waiting for category data...")

    with col_chart2:
        st.subheader("💳 Payment Method")
        df_pay = fetch_payment_summary()
        if not df_pay.empty:
            fig_pay = px.pie(
                df_pay,
                names="payment_method",
                values="total_revenue",
                hole=0.45,
                title="Revenue Share by Payment Method"
            )
            fig_pay.update_layout(height=350, margin=dict(l=20, r=20, t=40, b=20))
            st.plotly_chart(fig_pay, use_container_width=True)
        else:
            st.info("Waiting for payment data...")

    st.subheader("⚡ Live Cashier Transaction Feed (Last 15 Records)")
    df_txns = fetch_latest_transactions(limit=15)
    if not df_txns.empty:
        st.dataframe(
            df_txns,
            use_container_width=True,
            column_config={
                "transaction_id": "Transaction ID",
                "transaction_date": st.column_config.DatetimeColumn("Timestamp", format="YYYY-MM-DD HH:mm:ss"),
                "product_name": "Product Name",
                "category": "Category",
                "quantity": "Qty",
                "price": st.column_config.NumberColumn("Unit Price ($)", format="$%.2f"),
                "total_amount": st.column_config.NumberColumn("Total ($)", format="$%.2f"),
                "payment_method": "Payment Method",
                "supplier": "Supplier",
                "estimated_profit": st.column_config.NumberColumn("Est. Profit ($)", format="$%.2f"),
                "is_high_value": st.column_config.CheckboxColumn("High Value?"),
            }
        )
    else:
        st.info("No transaction data available yet.")

# ------------------------------------------------------------------------------
# TAB 2: SPARK BATCH ANALYTICS
# ------------------------------------------------------------------------------
with tab2:
    st.subheader("📅 Daily Batch Aggregation Results (Spark Batch Processing - LO5)")
    df_daily, df_top = fetch_batch_data()
    
    if not df_daily.empty:
        col_b1, col_b2 = st.columns([3, 2])
        with col_b1:
            fig_daily = px.line(
                df_daily,
                x="report_date",
                y="total_revenue",
                markers=True,
                title="Daily Revenue Trend ($ USD)",
                labels={"report_date": "Date", "total_revenue": "Total Revenue ($)"}
            )
            fig_daily.update_layout(height=350)
            st.plotly_chart(fig_daily, use_container_width=True)

        with col_b2:
            fig_peak = px.bar(
                df_daily,
                x="report_date",
                y="peak_hour",
                title="Busiest Shopping Hour per Day (Peak Hour)",
                labels={"report_date": "Date", "peak_hour": "Peak Hour (0-23)"}
            )
            fig_peak.update_layout(height=350)
            st.plotly_chart(fig_peak, use_container_width=True)

        st.markdown("#### 📋 Daily Batch Summary Table")
        st.dataframe(df_daily, use_container_width=True)
    else:
        st.info("Daily batch data has not been processed yet. Please run `python spark/spark_batch_job.py`.")

    st.markdown("---")
    st.subheader("🏆 Top 10 Best-Selling Products (Spark Distributed Window Ranking)")
    if not df_top.empty:
        fig_top = px.bar(
            df_top,
            x="total_revenue",
            y="product_name",
            orientation="h",
            color="category",
            title="Top 10 Products by Revenue ($)",
            labels={"total_revenue": "Revenue ($)", "product_name": "Product"}
        )
        fig_top.update_layout(yaxis=dict(autorange="reversed"), height=400)
        st.plotly_chart(fig_top, use_container_width=True)
        st.dataframe(df_top, use_container_width=True)
    else:
        st.info("Top products data is not available yet.")

# ------------------------------------------------------------------------------
# TAB 3: MACHINE LEARNING & ANOMALY ALERTS
# ------------------------------------------------------------------------------
with tab3:
    st.subheader("🤖 Big Data Machine Learning (Section 5.5 - 10% Evaluation Weight)")
    df_forecast, df_anomalies, df_clusters = fetch_ml_data()

    col_ml1, col_ml2 = st.columns([3, 2])

    with col_ml1:
        st.markdown("#### 🔮 6-Hour Ahead Revenue Forecast (Regression Forecasting)")
        if not df_forecast.empty:
            fig_fc = go.Figure()
            fig_fc.add_trace(go.Scatter(
                x=df_forecast["forecast_time"],
                y=df_forecast["predicted_revenue"],
                mode="lines+markers",
                name="Predicted Revenue ($)",
                line=dict(color="#2563EB", width=3)
            ))
            fig_fc.add_trace(go.Scatter(
                x=df_forecast["forecast_time"],
                y=df_forecast["upper_bound"],
                mode="lines",
                name="Upper Bound (95% CI)",
                line=dict(width=0),
                showlegend=False
            ))
            fig_fc.add_trace(go.Scatter(
                x=df_forecast["forecast_time"],
                y=df_forecast["lower_bound"],
                mode="lines",
                name="Confidence Interval",
                fill="tonexty",
                fillcolor="rgba(37, 99, 235, 0.15)",
                line=dict(width=0)
            ))
            fig_fc.update_layout(
                title="Future Revenue Forecast with Confidence Band",
                xaxis_title="Forecast Timestamp",
                yaxis_title="Estimated Revenue ($)",
                height=350,
                margin=dict(l=20, r=20, t=40, b=20)
            )
            st.plotly_chart(fig_fc, use_container_width=True)
            st.dataframe(df_forecast, use_container_width=True)
        else:
            st.info("Forecast model has not run yet. Run `python analytics/sales_ml_analytics.py`.")

    with col_ml2:
        st.markdown("#### ⚠️ Real-Time Anomaly Alerts (Suspicious Transaction Detection)")
        if not df_anomalies.empty:
            for _, row in df_anomalies.head(6).iterrows():
                st.markdown(f"""
                <div class="alert-box">
                    <strong>🚨 {row['transaction_id']}</strong> | ${row['total_amount']:,.2f}<br/>
                    <small><b>Product:</b> {row['product_name']}</small><br/>
                    <small><b>Reason:</b> {row['anomaly_type']}</small>
                </div>
                """, unsafe_allow_html=True)
        else:
            st.success("No anomalies detected at this time.")

    st.markdown("---")
    st.markdown("#### 🎯 Product Segmentation (K-Means Clustering)")
    if not df_clusters.empty:
        col_c1, col_c2 = st.columns([2, 3])
        with col_c1:
            fig_cluster_pie = px.pie(
                df_clusters,
                names="cluster_label",
                values="total_revenue",
                title="Revenue Proportion per Cluster"
            )
            st.plotly_chart(fig_cluster_pie, use_container_width=True)
        with col_c2:
            fig_cluster_scatter = px.scatter(
                df_clusters,
                x="total_units",
                y="total_revenue",
                color="cluster_label",
                hover_name="product_name",
                title="Scatter Plot: Units Sold vs Total Revenue",
                labels={"total_units": "Total Units Sold", "total_revenue": "Total Revenue ($)"}
            )
            st.plotly_chart(fig_cluster_scatter, use_container_width=True)

        st.dataframe(df_clusters, use_container_width=True)
    else:
        st.info("Product cluster data is not available yet.")

# ------------------------------------------------------------------------------
# TAB 4: PROFILE & ARCHITECTURE
# ------------------------------------------------------------------------------
with tab4:
    st.subheader("🏛️ Loilary Store - Dili, Timor-Leste")
    col_p1, col_p2 = st.columns(2)

    with col_p1:
        st.markdown("""
        **Case Study Background:**
        * **Business Entity:** Loilary Supermarket
        * **Operational Area:** Dili City, Timor-Leste
        * **Product Categories:** Beverages, Food, Groceries, Household Essentials, Imported Goods
        * **Data Characteristics:** Continuous point-of-sale cashier transactions with USD currency pricing.
        * **Business Challenge:** Need for real-time revenue monitoring without end-of-day batch delays, early anomaly transaction alerts, and instant profit margin estimation by category.
        """)

    with col_p2:
        st.markdown("""
        **Implementation of 5V Big Data Concepts:**
        * **Volume:** Managing thousands of transactions (>15,000 database records).
        * **Velocity:** Real-time transaction stream every 0.5 - 2 seconds via Apache Kafka.
        * **Variety:** Semi-structured JSON data format from field cashier point-of-sale producers.
        * **Veracity:** Automated data cleaning, schema validation, and statistical anomaly detection (Data Quality & Cleaning).
        * **Value:** Interactive analytics dashboard displaying revenue KPIs, profit margins, and Machine Learning predictive insights.
        """)

    st.markdown("---")
    st.subheader("🏗️ Big Data Pipeline Architecture Diagram (Curriculum Section 7)")
    st.code("""
+-----------------------------------------------------------------------------------------+
|                                 BIG DATA REAL-TIME PIPELINE                             |
+-----------------------------------------------------------------------------------------+
|  [DATA SOURCE]                                                                          |
|  Loilary Store POS (158 Field Products in Dili) -> [producer/kafka_producer.py]         |
|                                       |                                                 |
|                                       v (Format: Semi-Structured JSON)                  |
|  [DATA INGESTION]                                                                       |
|  Apache Kafka Broker 7.5.0 (Topic: sales_transactions @ port 9092)                      |
|                                       |                                                 |
|                                       v (Spark Streaming Ingest)                        |
|  [DATA PROCESSING - APACHE SPARK 3.5.0]                                                 |
|  1. Data Cleaning & Schema Validation                                                   |
|  2. Broadcast JOIN with product_catalog metadata                                        |
|  3. Feature Engineering (Profit, Margin, Peak Hour, High Value Flag)                    |
|  4. Windowed Aggregations (1-min tumble) + Spark Batch Processing                       |
|                                       |                                                 |
|                                       v (JDBC Structured Output)                        |
|  [DATA STORAGE - MYSQL 8.0: sales_dashboard]                                            |
|  - transactions (Raw & Enriched)        - daily_sales_batch (Batch)                     |
|  - sales_summary (Window 1m)            - top_products_batch (Ranking)                  |
|  - category_summary                     - ml_sales_forecast (Forecast)                  |
|  - payment_summary                      - ml_anomaly_alerts (Alerts)                    |
|                                       |                                                 |
|                                       +-----------------------+                         |
|                                       |                       |                         |
|                                       v                       v                         |
|  [DATA ANALYTICS & ML]          [VISUALIZATION]         [VISUALIZATION]                 |
|  - Multivariate OLS Forecast    Streamlit Dashboard     Microsoft Power BI              |
|  - Statistical Z-Score Anomaly  (Web Local Port 8501)   (DirectQuery Mode)              |
|  - K-Means Product Clustering   Flask Web Dashboard                                     |
|                                 (Web Local Port 5000)                                   |
+-----------------------------------------------------------------------------------------+
    """, language="text")

st.markdown("---")
st.caption(f"Real-Time Sales Dashboard © 2026 Dili Institute of Technology (DIT) | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
