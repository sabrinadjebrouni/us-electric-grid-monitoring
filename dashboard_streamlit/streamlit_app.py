# streamlit_app.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery

# --- PAGE CONFIG ---
st.set_page_config(layout="wide", page_title="EIA Grid Dashboard", page_icon="⚡")

# --- CUSTOM CSS FOR VISIBILITY ---
st.markdown("""
    <style>
    .main { background-color: #f5f7f9; }
    .stMetric { background-color: #ffffff; padding: 15px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
    </style>
    """, unsafe_allow_html=True)

# --- BIGQUERY CLIENT ---
credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials=credentials)
dataset = "eia-project-490020.eia_raw_data"

# --- DATA FETCHING ---
@st.cache_data(ttl=600)
def run_query(date_obj, hour_val):
    query = f"""
    SELECT 
        f.ba_id, ba.ba_code, ba.ba_name, ba.latitude, ba.longitude, 
        f.related_ba_id, rba.latitude as rel_lat, rba.longitude as rel_lon,
        f.value, t.type_code, t.type_description
    FROM `{dataset}.fct_grid_operation` f
    JOIN `{dataset}.dim_ba` ba ON f.ba_id = ba.ba_id
    LEFT JOIN `{dataset}.dim_ba` rba ON f.related_ba_id = rba.ba_id
    JOIN `{dataset}.dim_type` t ON f.type_id = t.type_id
    JOIN `{dataset}.dim_date` d ON f.date_id = d.date_id
    JOIN `{dataset}.dim_time_of_day` tod ON f.time_id = tod.time_id
    WHERE d.full_date = '{date_obj}' AND tod.hour_24 = {hour_val}
    """
    return client.query(query).to_dataframe()

# --- SIDEBAR ---
st.sidebar.title("Dashboard Controls")
selected_date = st.sidebar.date_input("Select Date", value=pd.to_datetime("2026-04-19"))
selected_hour = st.sidebar.slider("Hour of Day (24h)", 0, 23, 13)
st.sidebar.markdown("---")
st.sidebar.info("This dashboard visualizes real-time U.S. Grid operations including Demand and Inter-BA Flows.")

# --- MAIN RENDER ---
df = run_query(selected_date, selected_hour)

if not df.empty:
    # 1. TOP METRICS
    total_demand = df[df['type_code'] == 'D']['value'].sum()
    active_flows = len(df[df['type_code'] == 'FLOW'])
    
    m1, m2, m3 = st.columns(3)
    m1.metric("Total System Demand", f"{total_demand:,} MW")
    m2.metric("Active Interconnections", active_flows)
    m3.metric("Reporting BAs", df['ba_code'].nunique())

    st.markdown("---")

    # 2. MAP AND SIDE CHARTS
    col_map, col_stats = st.columns([2, 1])

    with col_map:
        st.subheader(f"Grid Topology at {selected_hour}:00")
        
        nodes = df[df['type_code'] != 'FLOW'].drop_duplicates(subset=['ba_id'])
        edges = df[df['type_code'] == 'FLOW']

        fig_map = go.Figure()

        # Lines (Edges)
        for _, row in edges.iterrows():
            if pd.notnull(row['rel_lat']):
                fig_map.add_trace(go.Scattergeo(
                    locationmode='USA-states',
                    lon=[row['longitude'], row['rel_lon']],
                    lat=[row['latitude'], row['rel_lat']],
                    mode='lines',
                    line=dict(width=1.5, color='#7f8c8d'),
                    opacity=0.4,
                    hoverinfo='none'
                ))

        # Circles (Nodes) + Text Labels
        fig_map.add_trace(go.Scattergeo(
            locationmode='USA-states',
            lon=nodes['longitude'],
            lat=nodes['latitude'],
            mode='markers+text',
            text=nodes['ba_code'],
            textposition="top center",
            hoverinfo='text',
            hovertext=nodes['ba_name'] + "<br>Value: " + nodes['value'].astype(str) + " MW",
            marker=dict(
                size=14,
                color=nodes['value'],
                colorscale='Portland',
                showscale=True,
                colorbar=dict(title="MW", thickness=15, len=0.5),
                line=dict(width=2, color='white')
            ),
            textfont=dict(size=9, color='black')
        ))

        fig_map.update_layout(
            geo=dict(scope='usa', projection_type='albers usa', showland=True, landcolor="#f9f9f9"),
            margin={"r":0,"t":0,"l":0,"b":0},
            height=600
        )
        st.plotly_chart(fig_map, use_container_width=True)

    with col_stats:
        st.subheader("Breakdown")
        
        # Chart 1: Top 10 BAs by Value
        top_bas = nodes.nlargest(10, 'value')
        fig_bar = px.bar(top_bas, x='value', y='ba_code', orientation='h',
                         title="Top 10 BAs by MW",
                         color='value', color_continuous_scale='Portland')
        fig_bar.update_layout(showlegend=False, height=300, margin={"t":30,"b":0})
        st.plotly_chart(fig_bar, use_container_width=True)

        # Chart 2: Data Type Distribution
        fig_pie = px.pie(df, names='type_description', values='value', 
                         title="Demand vs Generation vs Flow",
                         hole=0.4)
        fig_pie.update_layout(height=300, margin={"t":30,"b":0})
        st.plotly_chart(fig_pie, use_container_width=True)

    # 3. DATA EXPLORER
    with st.expander("Inspect Full Grid Data"):
        st.dataframe(df.sort_values(by='value', ascending=False), use_container_width=True)

else:
    st.error("No data returned for this selection. Try changing the date or hour.")