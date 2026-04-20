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

# --- STEP 1: FETCH STATIC SKELETON (Runs once) ---
@st.cache_data
def get_static_skeleton():
    # This query only gets the BAs and the fixed connections from your "Good Day"
    query = f"""
    WITH topology AS (
        SELECT DISTINCT ba_id, related_ba_id 
        FROM `{dataset}.fct_grid_operation`
        WHERE date_id = (SELECT date_id FROM `{dataset}.dim_date` WHERE full_date = '2026-04-07')
          AND related_ba_id != -1
    )
    SELECT 
        ba.ba_id, ba.ba_code, ba.ba_name, ba.latitude, ba.longitude,
        topo.related_ba_id, rba.latitude as rel_lat, rba.longitude as rel_lon
    FROM `{dataset}.dim_ba` ba
    LEFT JOIN topology topo ON ba.ba_id = topo.ba_id
    LEFT JOIN `{dataset}.dim_ba` rba ON topo.related_ba_id = rba.ba_id
    WHERE ba.is_active = 'Yes' AND ba.ba_id != -1
    """
    return client.query(query).to_dataframe()

# --- STEP 2: FETCH HOURLY TELEMETRY (Lightweight Query) ---
@st.cache_data(ttl=600)
def get_hourly_telemetry(date_obj, hour_val):
    # This query hits the Fact table directly with a simple filter (Cheap!)
    query = f"""
    SELECT 
        f.ba_id, 
        f.related_ba_id, 
        f.value, 
        t.type_code
    FROM `{dataset}.fct_grid_operation` f
    JOIN `{dataset}.dim_type` t ON f.type_id = t.type_id
    WHERE f.date_id = (SELECT date_id FROM `{dataset}.dim_date` WHERE full_date = '{date_obj}')
      AND f.time_id = (SELECT time_id FROM `{dataset}.dim_time_of_day` WHERE hour_24 = {hour_val})
      AND t.type_code IN ('D', 'FLOW')
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
    
        # --- STEP 1: LOAD DATA ---
    # This skeleton is cached and doesn't hit BigQuery every time
    skeleton_df = get_static_skeleton()
    # This telemetry is cheap and only pulls rows for the specific hour
    telemetry_df = get_hourly_telemetry(map_date, map_hour)

    # --- STEP 2: MERGE LOGIC ---
    # First merge: Flow data (matches on both ba_id and related_ba_id)
    df_combined = pd.merge(
        skeleton_df, 
        telemetry_df[telemetry_df['type_code'] == 'FLOW'], 
        on=['ba_id', 'related_ba_id'], 
        how='left'
    )

    # Second merge: Demand data (matches only on ba_id)
    demand_data = telemetry_df[telemetry_df['type_code'] == 'D'][['ba_id', 'value']]
    df_map = pd.merge(
        df_combined, 
        demand_data, 
        on='ba_id', 
        how='left', 
        suffixes=('_flow', '_demand')
    )

    # --- STEP 3: RENDER MAP ---
    if not df_map.empty:
        fig_map = go.Figure()

        # 1. SKELETON LAYER: Grey Edges (Physical Grid)
        # We use skeleton_df directly to ensure the skeleton NEVER disappears
        for _, row in skeleton_df[skeleton_df['rel_lat'].notnull()].iterrows():
            fig_map.add_trace(go.Scattergeo(
                lon=[row['longitude'], row['rel_lon']],
                lat=[row['latitude'], row['rel_lat']],
                mode='lines',
                line=dict(width=1, color='#d1d1d1'), 
                opacity=0.3,
                hoverinfo='none', showlegend=False
            ))

        # 2. SKELETON LAYER: Black Nodes
        fig_map.add_trace(go.Scattergeo(
            lon=skeleton_df['longitude'],
            lat=skeleton_df['latitude'],
            mode='markers',
            marker=dict(size=4, color='#2d3436'),
            hoverinfo='text',
            text=skeleton_df['ba_code'],
            showlegend=False
        ))

        # 3. LIVE DATA: Active Flows (Thick Colored Lines)
        # We filter df_map for rows where 'value_flow' is not null and not 0
        active_flows = df_map[(df_map['value_flow'].notnull()) & (df_map['value_flow'] != 0)]
        for _, row in active_flows.iterrows():
            flow_color = '#00C853' if row['value_flow'] > 0 else '#FF3D00'
            fig_map.add_trace(go.Scattergeo(
                lon=[row['longitude'], row['rel_lon']],
                lat=[row['latitude'], row['rel_lat']],
                mode='lines',
                line=dict(width=3, color=flow_color),
                opacity=0.8,
                hoverinfo='text',
                text=f"Flow: {row['value_flow']:,} MW"
            ))

        # 4. LIVE DATA: Demand (Dynamic Bubbles)
        # We use 'value_demand' which was merged in Step 2
        nodes_live = df_map[df_map['value_demand'].notnull()]
        if not nodes_live.empty:
            v_max = nodes_live['value_demand'].max() or 1
            fig_map.add_trace(go.Scattergeo(
                lon=nodes_live['longitude'],
                lat=nodes_live['latitude'],
                mode='markers+text',
                text=nodes_live['ba_code'],
                textposition="top center",
                marker=dict(
                    size=10 + (nodes_live['value_demand'] / v_max) * 35,
                    color=nodes_live['value_demand'],
                    colorscale='Turbo',
                    showscale=True,
                    colorbar=dict(title="<b>Demand (MW)</b>", thickness=15),
                    line=dict(width=1.5, color='white')
                )
            ))

        # 5. LAYOUT CONFIGURATION
        fig_map.update_layout(
            geo=dict(
                scope='usa', projection_type='albers usa',
                showland=True, landcolor="#f1f2f6", subunitcolor="#dfe4ea"
            ),
            margin={"r":0,"t":40,"l":0,"b":0}, height=750
        )

        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.warning("⚠️ No grid data found for the selected time.")

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