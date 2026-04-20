# streamlit_app.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import timedelta

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="US Hourly Electric Grid Monitor", page_icon=":material/electrical_services:")

# --- CUSTOM CSS ---
st.markdown("""
    <style>
    .main { background-color: #f0f2f6; }
    .stMetric { 
        background-color: #ffffff; 
        padding: 20px; 
        border-radius: 12px; 
        box-shadow: 0 4px 6px rgba(0,0,0,0.1); 
        border: 2px solid #d1d5db;
    }
    .chart-container {
        background-color: white;
        padding: 20px;
        border-radius: 15px;
        margin-bottom: 30px;
        border: 1px solid #e1e4e8;
    }
    </style>
    """, unsafe_allow_html=True)

# --- BIGQUERY CLIENT ---
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials)
dataset = "eia-project-490020.eia_raw_data"

# --- DATA FETCHING FUNCTIONS ---

@st.cache_data(ttl=600)
def get_map_data(date_obj, hour_val):
    query = f"""
        SELECT 
            ba.ba_id, ba.ba_code, ba.ba_name, ba.latitude, ba.longitude, 
            f.related_ba_id, rba.latitude as rel_lat, rba.longitude as rel_lon,
            COALESCE(f.value, 0) as value, 
            COALESCE(t.type_code, 'FLOW') as type_code,
            COALESCE(t.type_description, 'Interchange') as type_description
        FROM `eia-project-490020.eia_raw_data.dim_ba` ba
        LEFT JOIN `eia-project-490020.eia_raw_data.fct_grid_operation` f ON ba.ba_id = f.ba_id
        LEFT JOIN `eia-project-490020.eia_raw_data.dim_ba` rba ON f.related_ba_id = rba.ba_id
        LEFT JOIN `eia-project-490020.eia_raw_data.dim_type` t ON f.type_id = t.type_id
        LEFT JOIN `eia-project-490020.eia_raw_data.dim_date` d ON f.date_id = d.date_id
        LEFT JOIN `eia-project-490020.eia_raw_data.dim_time_of_day` tod ON f.time_id = tod.time_id
        WHERE (t.type_code in ('FLOW', 'D') and d.full_date = '{date_obj}' AND tod.hour_24 = {hour_val})
            OR f.ba_id IS NULL ;
    """
    return client.query(query).to_dataframe()

@st.cache_data(ttl=600)
def get_weekly_gen_mix(start_date, end_date):
    query = f"""
    SELECT d.full_date, tod.hour_24, t.type_description as source, SUM(f.value) as value
    FROM `{dataset}.fct_grid_operation` f
    JOIN `{dataset}.dim_type` t ON f.type_id = t.type_id
    JOIN `{dataset}.dim_date` d ON f.date_id = d.date_id
    JOIN `{dataset}.dim_time_of_day` tod ON f.time_id = tod.time_id
    WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
      AND t.type_code IN ('COL', 'NG', 'NUC', 'SUN', 'WND', 'WAT', 'GEO', 'OIL', 'OTH', 'BAT')
    GROUP BY 1, 2, 3
    ORDER BY d.full_date, tod.hour_24 ASC
    """
    return client.query(query).to_dataframe()

# --- MAIN DASHBOARD ---

st.title("US Hourly Electric Grid Monitor")
st.markdown("---")

# SECTION 1: MAP WITH LOCAL PARAMETERS
with st.container():
    st.subheader("📍 Real-Time Grid Topology & Interchanges")
    
    # Local Parameters for Map
    c1, c2 = st.columns([1, 2])
    with c1:
        map_date = st.date_input("Map Date", value=pd.to_datetime("2026-04-19"), key="map_date")
    with c2:
        map_hour = st.slider("Map Hour", 0, 23, 13, key="map_hour")
    
    df_map = get_map_data(map_date, map_hour)
    
    if not df_map.empty:

        nodes = df_map[df_map['type_code'] == 'D'].drop_duplicates(subset=['ba_id'])
        edges = df_map[df_map['type_code'] == 'FLOW'].copy()

        fig_map = go.Figure()

        # Edges
        for _, row in edges.iterrows():
            if pd.notnull(row['rel_lat']):
                color = '#00C853' if row['value'] > 0 else '#FF3D00' if row['value'] < 0 else '#4A4A4A'
                opacity = 1.0 if row['value'] != 0 else 0.2
                fig_map.add_trace(go.Scattergeo(
                    lon=[row['longitude'], row['rel_lon']], lat=[row['latitude'], row['rel_lat']],
                    mode='lines', line=dict(width=2, color=color), opacity=opacity, hoverinfo='none'
                ))

        # Nodes
        max_d = nodes['value'].max() if not nodes.empty else 1
        fig_map.add_trace(go.Scattergeo(
            lon=nodes['longitude'], lat=nodes['latitude'], mode='markers+text',
            text=nodes['ba_code'], textposition="top center",
            marker=dict(size=10 + (nodes['value']/max_d)*35, color=nodes['value'], colorscale='Turbo', showscale=True, line=dict(width=1.5, color='white')),
            textfont=dict(size=11, family="Arial Black")
        ))

        fig_map.update_layout(geo=dict(scope='usa', showland=True, landcolor="#e5e7eb"), height=700, margin={"r":0,"t":0,"l":0,"b":0})
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("No map data available for this selection.")

st.markdown("---")

# SECTION 2: WEEKLY ANALYSIS WITH LOCAL PARAMETERS
with st.container():
    st.subheader("📈 Energy Generation Mix (Weekly Interval)")
    
    # Local Parameters for Weekly Chart
    wc1, wc2 = st.columns(2)
    with wc1:
        start_date = st.date_input("Start Date", value=pd.to_datetime("2026-04-12"), key="gen_start")
    with wc2:
        end_date = st.date_input("End Date", value=start_date + timedelta(days=7), key="gen_end")
    
    gen_df = get_weekly_gen_mix(start_date, end_date)
    
    if not gen_df.empty:
        # Create a datetime column for a continuous X-axis
        gen_df['timestamp'] = pd.to_datetime(gen_df['full_date']) + pd.to_timedelta(gen_df['hour_24'], unit='h')
        
        fig_gen = px.area(
            gen_df, x="timestamp", y="value", color="source",
            color_discrete_sequence=px.colors.qualitative.Dark24
        )
        fig_gen.update_layout(hovermode="x unified", height=500, xaxis_title="Time", yaxis_title="MW Generated")
        st.plotly_chart(fig_gen, use_container_width=True)
    else:
        st.warning("No generation data for this period.")

st.markdown("---")

# SECTION 3: PERFORMANCE BAR CHART (WEEKLY)
with st.container():
    st.subheader("📊 Top Balancing Authorities (Weekly Total)")
    
    # Re-using the same date range from Section 2 or custom
    st.info(f"Showing performance from {start_date} to {end_date}")
    
    # Simplified query for weekly sum per BA
    query_top = f"""
        SELECT ba.ba_code, SUM(f.value) as total_mw
        FROM `{dataset}.fct_grid_operation` f
        JOIN `{dataset}.dim_ba` ba ON f.ba_id = ba.ba_id
        JOIN `{dataset}.dim_date` d ON f.date_id = d.date_id
        WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY 1 ORDER BY 2 DESC LIMIT 15
    """
    top_df = client.query(query_top).to_dataframe()
    
    if not top_df.empty:
        fig_bar = px.bar(top_df, x='total_mw', y='ba_code', orientation='h', color='total_mw', color_continuous_scale='Turbo')
        fig_bar.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig_bar, use_container_width=True)

# RAW DATA EXPANDER AT THE VERY BOTTOM
with st.expander("🔍 System Logs / Raw Data"):
    st.write("Current Map Snapshot Data:")
    st.dataframe(df_map)