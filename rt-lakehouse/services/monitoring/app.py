import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import time
import json
from datetime import datetime, timedelta

# Page config
st.set_page_config(
    page_title="RT-Lakehouse Monitoring",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Constants
API_BASE = "http://assistant-api:8000"
REFRESH_INTERVAL = 30  # seconds

def check_service_health():
    """Check health of all services"""
    services = {
        "Assistant API": f"{API_BASE}/health",
        "DuckDB": "Connected via API",
        "Qdrant": "Connected via API", 
        "Kafka": "Connected via Spark"
    }
    
    health_status = {}
    
    for service, url in services.items():
        if url.startswith("http"):
            try:
                response = requests.get(url, timeout=5)
                health_status[service] = {
                    "status": "✅ Healthy" if response.status_code == 200 else "❌ Error",
                    "response_time": response.elapsed.total_seconds()
                }
            except Exception as e:
                health_status[service] = {
                    "status": "❌ Error",
                    "error": str(e)
                }
        else:
            health_status[service] = {"status": "📊 Via API"}
    
    return health_status

def get_metrics():
    """Fetch current metrics from API"""
    try:
        response = requests.get(f"{API_BASE}/metrics", timeout=10)
        if response.status_code == 200:
            return response.json()
        else:
            return None
    except Exception as e:
        st.error(f"Failed to fetch metrics: {e}")
        return None

def query_data(sql_query):
    """Execute SQL query via API"""
    try:
        response = requests.post(
            f"{API_BASE}/query",
            json={"question": sql_query, "include_chart": False},
            timeout=30
        )
        if response.status_code == 200:
            data = response.json()
            return pd.DataFrame(data["results"]) if data["results"] else pd.DataFrame()
        else:
            st.error(f"Query failed: {response.status_code}")
            return pd.DataFrame()
    except Exception as e:
        st.error(f"Query error: {e}")
        return pd.DataFrame()

def main():
    st.title("🏠 RT-Lakehouse Monitoring Dashboard")
    st.markdown("Real-time monitoring of your streaming data lakehouse")
    
    # Auto-refresh toggle
    auto_refresh = st.sidebar.checkbox("Auto-refresh (30s)", value=True)
    
    if auto_refresh:
        # Auto-refresh placeholder
        placeholder = st.empty()
        time.sleep(0.1)  # Small delay to show loading
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now"):
        st.rerun()
    
    # Service Health Section
    st.header("🔧 Service Health")
    health_status = check_service_health()
    
    cols = st.columns(len(health_status))
    for i, (service, status) in enumerate(health_status.items()):
        with cols[i]:
            st.metric(
                label=service,
                value=status["status"],
                delta=f"{status.get('response_time', 0):.2f}s" if 'response_time' in status else None
            )
    
    # System Metrics Section
    st.header("📊 System Metrics")
    metrics = get_metrics()
    
    if metrics:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Latest KPIs")
            if metrics.get("latest_kpis"):
                kpi_df = pd.DataFrame([metrics["latest_kpis"]])
                st.dataframe(kpi_df, use_container_width=True)
            else:
                st.info("No KPI data available")
        
        with col2:
            st.subheader("Recent Events (Last Hour)")
            if metrics.get("recent_events"):
                events_df = pd.DataFrame(
                    list(metrics["recent_events"].items()),
                    columns=["Event Type", "Count"]
                )
                
                fig = px.bar(
                    events_df, 
                    x="Event Type", 
                    y="Count",
                    title="Event Distribution"
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No event data available")
    else:
        st.warning("Unable to fetch system metrics")
    
    # Pipeline Performance Section
    st.header("⚡ Pipeline Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Conversion Rate Trend")
        conversion_df = query_data("Show conversion rate trend for the last 2 hours")
        
        if not conversion_df.empty and 'conversion_rate' in conversion_df.columns:
            fig = px.line(
                conversion_df,
                x=conversion_df.columns[0] if len(conversion_df.columns) > 0 else None,
                y='conversion_rate',
                title="Conversion Rate Over Time"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No conversion rate data available")
    
    with col2:
        st.subheader("Revenue Trend")
        revenue_df = query_data("Show GMV trend for the last 2 hours")
        
        if not revenue_df.empty and 'gmv' in revenue_df.columns:
            fig = px.line(
                revenue_df,
                x=revenue_df.columns[0] if len(revenue_df.columns) > 0 else None,
                y='gmv',
                title="Gross Merchandise Value Over Time"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No revenue data available")
    
    # Data Quality Section
    st.header("🔍 Data Quality")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Event count by type
        event_counts = query_data("Count events by type in the last hour")
        if not event_counts.empty:
            st.metric("Total Events", event_counts.sum().iloc[0] if len(event_counts.columns) > 1 else "N/A")
        else:
            st.metric("Total Events", "N/A")
    
    with col2:
        # Unique users
        unique_users = query_data("Count unique users in the last hour")
        if not unique_users.empty:
            st.metric("Active Users", unique_users.iloc[0, 0] if len(unique_users) > 0 else "N/A")
        else:
            st.metric("Active Users", "N/A")
    
    with col3:
        # Data freshness
        latest_event = query_data("Show latest event timestamp")
        if not latest_event.empty:
            try:
                latest_time = pd.to_datetime(latest_event.iloc[0, 0])
                freshness = datetime.now() - latest_time
                st.metric("Data Freshness", f"{freshness.seconds}s ago")
            except:
                st.metric("Data Freshness", "N/A")
        else:
            st.metric("Data Freshness", "N/A")
    
    # Raw Data Explorer
    with st.expander("🔍 Raw Data Explorer"):
        st.subheader("Custom SQL Query")
        
        query_options = [
            "SELECT * FROM silver_events ORDER BY ts DESC LIMIT 10",
            "SELECT * FROM gold_kpis ORDER BY window_start DESC LIMIT 10",
            "SELECT event_type, COUNT(*) as count FROM silver_events GROUP BY event_type",
            "SELECT country, COUNT(*) as users FROM silver_events GROUP BY country ORDER BY users DESC LIMIT 10"
        ]
        
        selected_query = st.selectbox("Choose a query:", query_options)
        custom_query = st.text_area("Or write your own:", value=selected_query, height=100)
        
        if st.button("Execute Query"):
            df = query_data(custom_query)
            if not df.empty:
                st.dataframe(df, use_container_width=True)
                st.download_button(
                    "Download CSV",
                    df.to_csv(index=False),
                    file_name=f"query_result_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.info("No data returned")
    
    # Footer
    st.markdown("---")
    st.markdown(
        f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | "
        f"Auto-refresh: {'Enabled' if auto_refresh else 'Disabled'}"
    )
    
    # Auto-refresh logic
    if auto_refresh:
        time.sleep(REFRESH_INTERVAL)
        st.rerun()

if __name__ == "__main__":
    main()
