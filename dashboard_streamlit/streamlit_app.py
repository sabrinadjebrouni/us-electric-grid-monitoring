# streamlit_app.py

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import timedelta
import plotly.express as px
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


# Data for Combined Grid Operations
@st.cache_data(ttl=3600)
def get_timeseries_data(start_date, end_date):
    query = f"""
    SELECT 
        d.full_date,
        t.hour_24,
        -- Combined Timestamp for Plotly
        DATETIME(d.full_date, TIME(t.hour_24, 0, 0)) as timestamp,
        
        -- Aggregating specific types
        SUM(CASE WHEN tp.type_code = 'D' THEN f.value ELSE 0 END) as demand,
        SUM(CASE WHEN tp.type_code = 'DF' THEN f.value ELSE 0 END) as demand_forecast,
        SUM(CASE WHEN tp.type_code = 'FLOW' THEN f.value ELSE 0 END) as total_interchange,
        
        -- Summing all generation sources (type_ids 4 through 19)
        SUM(CASE WHEN f.type_id BETWEEN 4 AND 19 THEN f.value ELSE 0 END) as net_generation
        
    FROM `{dataset}.fct_grid_operation` f
    JOIN `{dataset}.dim_date` d ON f.date_id = d.date_id
    JOIN `{dataset}.dim_time_of_day` t ON f.time_id = t.time_id
    JOIN `{dataset}.dim_type` tp ON f.type_id = tp.type_id
    WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
    GROUP BY 1, 2, 3
    ORDER BY d.full_date, t.hour_24
    """
    return client.query(query).to_dataframe()


#Data for operation grouped by region
@st.cache_data(ttl=3600)
def get_regional_summary_data(start_date, end_date):
    query = f"""
    SELECT 
        b.region_country_name,
        CASE 
            WHEN tp.type_code = 'D' THEN 'Demand'
            WHEN tp.type_code = 'DF' THEN 'Demand Forecast'
            WHEN tp.type_code = 'FLOW' THEN 'Total Interchange'
            WHEN f.type_id BETWEEN 4 AND 19 THEN 'Net Generation'
            ELSE 'Other'
        END as category,
        SUM(f.value) as total_value
    FROM `{dataset}.fct_grid_operation` f
    JOIN `{dataset}.dim_date` d ON f.date_id = d.date_id
    JOIN `{dataset}.dim_ba` b ON f.ba_id = b.ba_id
    JOIN `{dataset}.dim_type` tp ON f.type_id = tp.type_id
    WHERE d.full_date BETWEEN '{start_date}' AND '{end_date}'
      AND f.type_id != -1
    GROUP BY 1, 2
    HAVING category != 'Other'
    ORDER BY total_value DESC
    """
    return client.query(query).to_dataframe()

# DATA FOR NET GENERATION BY ENERGY TYPE 
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
    st.subheader("Hourly Electricity Demand and Interchange per Balancing Authority")
    
    c1, c2 = st.columns([1, 2])
    
    # Logic: Set a specific 'Launch' date or use the current date
    # To use today: default_date = datetime.date.today()
    default_date = pd.to_datetime("2026-04-07") 
    default_hour = 1 # 1:00 PM
    
    with c1:
        map_date = st.date_input(
            "Map Date", 
            value=default_date, 
            key="map_date"
        )
    with c2:
        map_hour = st.slider(
            "Map Hour", 
            min_value=0, 
            max_value=23, 
            value=default_hour, 
            key="map_hour"
        )
    
    # --- STEP 1: LOAD DATA ---
    skeleton_df = get_static_skeleton()
    telemetry_df = get_hourly_telemetry(map_date, map_hour)

    # --- STEP 2: MERGE LOGIC ---
    # Merge Flow data
    df_combined = pd.merge(
        skeleton_df, 
        telemetry_df[telemetry_df['type_code'] == 'FLOW'], 
        on=['ba_id', 'related_ba_id'], 
        how='left'
    )

    # Merge Demand data
    demand_data = telemetry_df[telemetry_df['type_code'] == 'D'][['ba_id', 'value']]
    df_map = pd.merge(
        df_combined, 
        demand_data, 
        on='ba_id', 
        how='left', 
        suffixes=('_flow', '_demand')
    )

    # --- STEP 2.5: PREPARE HOVER TEXT FOR NODES ---
    # We group by BA to consolidate Demand and the Net Interchange for the tooltip
    node_hover_df = df_map.groupby(['ba_id', 'ba_name', 'ba_code', 'latitude', 'longitude']).agg({
        'value_demand': 'first',
        'value_flow': 'sum'
    }).reset_index()

    def build_hover_text(row):
        d_val = row['value_demand']
        d_text = f"{d_val:,.0f} MW" if (pd.notnull(d_val) and d_val != 0) else "data unavailable"
        
        f_val = row['value_flow']
        f_text = f"{f_val:,.0f} MW" if (pd.notnull(f_val) and f_val != 0) else "data unavailable"
        
        return f"<b>{row['ba_name']} ({row['ba_code']})</b><br>Demand: {d_text}<br>Net Interchange: {f_text}"

    node_hover_df['custom_hover'] = node_hover_df.apply(build_hover_text, axis=1)

    # --- STEP 3: RENDER MAP ---
    if not df_map.empty:
        fig_map = go.Figure()

        # 1. SKELETON LAYER: Grey Edges (Physical Grid)
        for _, row in skeleton_df[skeleton_df['rel_lat'].notnull()].iterrows():
            fig_map.add_trace(go.Scattergeo(
                lon=[row['longitude'], row['rel_lon']],
                lat=[row['latitude'], row['rel_lat']],
                mode='lines',
                line=dict(width=1, color='#d1d1d1'), 
                opacity=0.3,
                showlegend=False,
                hoverinfo='none'
            ))

        # 2. SKELETON LAYER: Black Nodes (Now with Tooltips)
        fig_map.add_trace(go.Scattergeo(
            lon=node_hover_df['longitude'],
            lat=node_hover_df['latitude'],
            mode='markers',
            marker=dict(size=4, color='#2d3436'),
            hoverinfo='text',
            text=node_hover_df['custom_hover'],
            showlegend=False
        ))

        # 3. LIVE DATA: Active Flows (Thick Colored Lines)
        active_flows = df_map[(df_map['value_flow'].notnull()) & (df_map['value_flow'] != 0)]
        for _, row in active_flows.iterrows():
            flow_color = '#00C853' if row['value_flow'] > 0 else '#FF3D00'
            fig_map.add_trace(go.Scattergeo(
                lon=[row['longitude'], row['rel_lon']],
                lat=[row['latitude'], row['rel_lat']],
                mode='lines',
                line=dict(width=3, color=flow_color),
                opacity=0.8,
                showlegend=False,
                hoverinfo='text',
                text=f"Flow: {row['ba_code']} ➔ {row['related_ba_id']}<br>Value: {row['value_flow']:,} MW"
            ))
        
        # 4. LIVE DATA: Demand (Dynamic Bubbles)
        nodes_live = node_hover_df[node_hover_df['value_demand'].notnull()]
        if not nodes_live.empty:
            v_max = nodes_live['value_demand'].max() or 1
            fig_map.add_trace(go.Scattergeo(
                lon=nodes_live['longitude'],
                lat=nodes_live['latitude'],
                mode='markers',
                hoverinfo='text',
                text=nodes_live['custom_hover'],
                showlegend=False,
                marker=dict(
                    size=10 + (nodes_live['value_demand'] / v_max) * 35,
                    color=nodes_live['value_demand'],
                    colorscale='Turbo',
                    showscale=True,
                    colorbar=dict(title="<b>Demand (MW)</b>", thickness=15),
                    line=dict(width=1.5, color='white')
                )
            ))

        # 5. CUSTOM LEGEND (Dummy Traces)
        legend_data = [
            ("#00C853", "In-flow (negative interchange)"),
            ("#FF3D00", "Out-Flow (positive interchange)"),
            ("#d1d1d1", "No Flow (no interchange)")
        ]
        for color, name in legend_data:
            fig_map.add_trace(go.Scattergeo(
                lon=[None], lat=[None], mode='lines',
                line=dict(width=3, color=color),
                name=name, showlegend=True
            ))

        # 6. LAYOUT CONFIGURATION
        fig_map.update_layout(
            geo=dict(
                scope='usa', projection_type='albers usa',
                showland=True, landcolor="#f1f2f6", subunitcolor="#dfe4ea"
            ),
            margin={"r":0,"t":40,"l":0,"b":0}, 
            height=750,
            showlegend=True,
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.1,             # Slightly below the map
                xanchor="center",
                x=0.5,
                # IMPROVEMENTS FOR VISIBILITY:
                bgcolor="rgba(0,0,0,0)",    # Fully transparent background
                font=dict(
                    size=12,
                    color="white"           # Or "black" if your app theme is light
                ),
                itemsizing="constant"       # Makes the legend lines easier to see
            )
        )
        
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.error("⚠️ No grid data found for the selected time.")

st.markdown("---")



# SECTION 2: Line chart for SUM of D, DF, FLOW and Net Generation
st.subheader("Combined Grid Operations for: Demand, Net Generation, and Interchange")

# Date Range Picker (Default: Last 7 days)
col_a, col_b = st.columns(2)
with col_a:
    start_dt = st.date_input("Start Date", value=pd.to_datetime("2026-04-01"))
with col_b:
    end_dt = st.date_input("End Date", value=pd.to_datetime("2026-04-08"))

# Fetch Data
df_ts = get_timeseries_data(start_dt, end_dt)

if not df_ts.empty:
    fig_ts = go.Figure()

    # Define the lines to plot
    lines = [
        ('demand', 'Demand', '#00a8ff', 'solid'),
        ('demand_forecast', 'Demand Forecast', '#00a8ff', 'dot'),
        ('net_generation', 'Net Generation', '#fbc531', 'solid'),
        ('total_interchange', 'Total Interchange', '#4cd137', 'solid')
    ]

    for col, name, color, dash in lines:
        fig_ts.add_trace(go.Scatter(
            x=df_ts['timestamp'],
            y=df_ts[col],
            name=name,
            line=dict(color=color, width=2, dash=dash),
            mode='lines'
        ))

    fig_ts.update_layout(
        template="plotly_dark",
        hovermode="x unified",
        xaxis_title="Date",
        yaxis_title="Megawatthours (MWh)",
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5),
        margin=dict(l=0, r=0, t=20, b=0),
        height=500,
        # Synchronize Y-axis to handle the negative interchange if needed
        yaxis=dict(zeroline=True, zerolinewidth=1, zerolinecolor='rgba(255,255,255,0.2)')
    )

    st.plotly_chart(fig_ts, use_container_width=True)
else:
    st.info("No data available for the selected date range.")


st.markdown("---")



# SECTION 3: REGIONAL ENERGY OVERVIEW
st.subheader("Regional Energy Composition")

# --- DATE PARAMETERS ---

c1, c2 = st.columns(2)
with c1:
    bar_start = st.date_input("Start Date", value=pd.to_datetime("2026-04-01"), key="bar_start")
with c2:
    bar_end = st.date_input("End Date", value=pd.to_datetime("2026-04-08"), key="bar_end")

# Fetch Data
df_bar = get_regional_summary_data(bar_start, bar_end)

if not df_bar.empty:

    # Create the chart
    fig_bar = px.bar(
        df_bar,
        x="category",
        y="total_value",
        color="region_country_name",
        title=f"Totals by Region ({bar_start} to {bar_end})",
        labels={
            "category": "Metric",
            "total_value": "Total MWh",
            "region_country_name": "Region Name"
        },
        template="plotly_dark",
        barmode="group" # 'group' for side-by-side comparison, 'stack' for total volume
    )

    fig_bar.update_layout(
        xaxis={'categoryorder':'total descending'},
        legend=dict(
            orientation="h",
            yanchor="bottom", y=-0.4,
            xanchor="center", x=0.5
        ),
        height=600,
        margin=dict(b=100) # Space for the horizontal legend
    )

    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.info("No data found for the selected range.")

st.markdown("---")

# SECTION 4: NET GENERATION BY ENERGY TYPE 
with st.container():
    st.subheader("Electricity Generation By Energy Source")
    
    # --- 1. SET DEFAULT DATES ---
    wc1, wc2 = st.columns(2)
    with wc1:
        # Default start set to 2026-04-01
        start_date = st.date_input("Start Date", value=pd.to_datetime("2026-04-01"), key="gen_start")
    with wc2:
        # Default end set to 2026-04-08
        end_date = st.date_input("End Date", value=pd.to_datetime("2026-04-08"), key="gen_end")
    
    gen_df = get_weekly_gen_mix(start_date, end_date)
    
    if not gen_df.empty:
        gen_df['timestamp'] = pd.to_datetime(gen_df['full_date']) + pd.to_timedelta(gen_df['hour_24'], unit='h')
        
        # --- 2. DEFINE HIGH-CONTRAST COLOR MAP ---
        # This replaces px.colors.qualitative.Dark24 with industry-standard colors
        color_map = {
            "Solar": "#FFD700",          # Bright Yellow
            "Wind": "#1B9E77",           # Green
            "Natural gas": "#E69F00",    # Orange
            "Coal": "#56B4E9",           # Light Blue
            "Nuclear": "#9b59b6",        # Purple
            "Hydro and pumped storage": "#0072B2", # Deep Blue
            "Petroleum": "#8B4513",      # Brown
            "Geothermal": "#D55E00",     # Vermillion
            "Battery storage": "#CC79A7",# Pink
            "Other": "#999999"           # Grey
        }
        
        fig_gen = px.area(
            gen_df, 
            x="timestamp", 
            y="value", 
            color="source",
            # Apply the custom color map
            color_discrete_map=color_map,
            # Fallback to a bright palette if a source isn't in our map
            color_discrete_sequence=px.colors.qualitative.Pastel
        )
        
        # --- 3. FIX UI CLARITY ---
        fig_gen.update_layout(
            hovermode="x unified", 
            height=600, 
            xaxis_title="Time", 
            yaxis_title="MW Generated",
            template="plotly_dark",
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=-0.3,
                xanchor="center",
                x=0.5
            )
        )
        
        # Improves line visibility between stacked areas
        fig_gen.update_traces(line=dict(width=0.5))
        
        st.plotly_chart(fig_gen, use_container_width=True)
    else:
        st.warning("No generation data for this period.")

st.markdown("---")


# RAW DATA EXPANDER AT THE VERY BOTTOM
with st.expander("Hourly Electricity Demand and Interchange Data Visualisation:"):
    st.dataframe(df_map)