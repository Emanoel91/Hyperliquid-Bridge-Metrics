import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

# --- Page Config ------------------------------------------------------------------------------------------------------
st.set_page_config(
    page_title="Hyperliquid Bridge Metrics",
    page_icon="https://img.cryptorank.io/coins/hyperliquid1699003432264.png",
    layout="wide"
)

# --- Title with Logo -----------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="display: flex; align-items: center; gap: 15px;">
        <img src="https://img.cryptorank.io/coins/hyperliquid1699003432264.png" alt="Hyperliquid Logo" style="width:60px; height:60px;">
        <h1 style="margin: 0;">Hyperliquid Bridge Metrics</h1>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Builder Info ---------------------------------------------------------------------------------------------------------
st.markdown(
    """
    <div style="margin-top: 20px; margin-bottom: 20px; font-size: 16px;">
        <div style="display: flex; align-items: center; gap: 10px;">
            <img src="https://pbs.twimg.com/profile_images/1841479747332608000/bindDGZQ_400x400.jpg" style="width:25px; height:25px; border-radius: 50%;">
            <span>Built by: <a href="https://x.com/0xeman_raz" target="_blank">Eman Raz</a></span>
        </div>
    </div>
    """,
    unsafe_allow_html=True
)

st.info("📊Charts initially display data for a default time range. Select a custom range to view results for your desired period.")
st.info("⏳On-chain data retrieval may take a few moments. Please wait while the results load.")

# --- Snowflake Connection ----------------------------------------------------------------------------------------
snowflake_secrets = st.secrets["snowflake"]
user = snowflake_secrets["user"]
account = snowflake_secrets["account"]
private_key_str = snowflake_secrets["private_key"]
warehouse = snowflake_secrets.get("warehouse", "")
database = snowflake_secrets.get("database", "")
schema = snowflake_secrets.get("schema", "")

private_key_pem = f"-----BEGIN PRIVATE KEY-----\n{private_key_str}\n-----END PRIVATE KEY-----".encode("utf-8")
private_key = serialization.load_pem_private_key(
    private_key_pem,
    password=None,
    backend=default_backend()
)
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

conn = snowflake.connector.connect(
    user=user,
    account=account,
    private_key=private_key_bytes,
    warehouse=warehouse,
    database=database,
    schema=schema
)

# --- Date Inputs -------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    timeframe = st.selectbox("Select Time Frame", ["week", "month", "day"])

with col2:
    start_date = st.date_input("Start Date", value=pd.to_datetime("2023-02-26"))

with col3:
    end_date = st.date_input("End Date", value=pd.to_datetime("2025-08-31"))

st.markdown(
    """
    <div style="background-color:#c3c3c3; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Bridge Flows </h2>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Row 1 ---------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_hyperliquid_data_over_time(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with tab1 as (
SELECT 
  date(block_timestamp) as day,
  'USDC.e' as token,
  sum(
    CASE 
      when TO_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4') then amount
      when FROM_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4') then -1 * amount
    END
  ) as net_deposit,
  sum(net_deposit) over (ORDER by day) as TVL
  
FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
WHERE (TO_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4')
OR FROM_address LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4'))
AND contract_address LIKE lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')
GROUP BY 1 

UNION all 

SELECT 
  date(block_timestamp) as day,
  'USDC' as token,
  sum(
    CASE 
      when TO_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7') then amount
      when FROM_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7') then -1 * amount
    END
  ) as net_deposit,
  sum(net_deposit) over (ORDER by day) as TVL
  
FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
WHERE (TO_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7')
OR FROM_address LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7'))
AND contract_address LIKE lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831')
GROUP BY 1,2
), tab2 as ( 
SELECT
  date as d1,
  sum(supply) as stablecoin_suppy
 
FROM (
SELECT
  date,
  symbol,
  sum(case when type like 'mint' then amount when type like 'burn' then -amount else 0 end) as net_mint,
  sum(net_mint) over (partition by symbol order by date) as supply

from (
  SELECT
    date(block_timestamp) date,
    case when from_address like '0x0000000000000000000000000000000000000000' then 'mint'
    when to_address like '0x0000000000000000000000000000000000000000' then 'burn' else 'otehr' end as type,
    SYMBOL,
    amount 
  
  from ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  where contract_address in (
    lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831'),
    lower('0xFd086bC7CD5C481DCC9C85ebE478A1C0b69FCbb9'),
    lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8'),
    lower('0xda10009cbd5d07dd0cecc66161fc93d7c9000da1')
  )
)
GROUP BY 1,2)
GROUP by 1)
SELECT 
  *,
  100 * (tvl/stablecoin_suppy) as percent_of_sablecoins_in_hyperliquid

from tab1
  left outer join tab2
    on d1 = day
    order by day 

    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
hyperliquid_data_over_time = load_hyperliquid_data_over_time(timeframe, start_date, end_date)
# --- Row 2 charts -------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    fig_stacked = px.bar(
        hyperliquid_data_over_time,
        x="DAY",
        y="TVL",
        color="TOKEN",
        title="Daily Hyperliquid TVL by Token"
    )
    fig_stacked.update_layout(
        barmode="stack",
        xaxis_title="",
        yaxis_title="USD",
        legend=dict(
            orientation="h", 
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        legend_title_text=""  
    )
    st.plotly_chart(fig_stacked, use_container_width=True)

with col2:
    fig2 = px.bar(
        hyperliquid_data_over_time,
        x="DAY",
        y="NET_DEPOSIT",
        title="Daily Hyperliquid Net Deposits",
        color_discrete_sequence=["#e2fb43"]
    )
    fig2.update_layout(xaxis_title="", yaxis_title="USD", bargap=0.2)
    st.plotly_chart(fig2, use_container_width=True)

with col3:
    fig3 = go.Figure()
    fig3.add_trace(
        go.Scatter(
            x=hyperliquid_data_over_time["DAY"],
            y=hyperliquid_data_over_time["PERCENT_OF_SABLECOINS_IN_HYPERLIQUID"],
            name="PERCENT_OF_SABLECOINS_IN_HYPERLIQUID",
            mode="lines",
            yaxis="y1"
        )
    )
    
    fig3.update_layout(
        title="Daily Percent of Arbitrum Stablecoins in Hyperliquid",
        yaxis=dict(title="%"),
        xaxis=dict(title=" "),
        legend=dict(
            orientation="h",   
            yanchor="bottom", 
            y=1.05,           
            xanchor="center",  
            x=0.5
        )
    )
    st.plotly_chart(fig3, use_container_width=True)

st.markdown(
    """
    <div style="background-color:#c3c3c3; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Bridge Deposits/Withdraws</h2>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Row 2 ---------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_hyperliquid_bridge_data(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    SELECT
  date_trunc('week', day) as week,
  action_type,
  count(DISTINCT user) as users,
  count(DISTINCT tx_hash) as events,
  sum(amount) as volume

FROM (
SELECT 
  date(block_timestamp) as day,
  CASE 
      when TO_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4') then 'Deposit'
      when FROM_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4') then 'Withdrawl'
  END as action_type,
  tx_hash,
  CASE 
      when TO_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4') then from_address
      when FROM_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4') then to_address
  END as user,
  amount  
  
FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
WHERE (TO_ADDRESS LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4')
OR FROM_address LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4'))
AND contract_address LIKE lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')

UNION all 

SELECT 
  date(block_timestamp) as day,
  CASE 
      when TO_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7') then 'Deposit'
      when FROM_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7') then 'Withdrawl'
  END as action_type,
  tx_hash,
  CASE 
      when TO_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7') then from_address
      when FROM_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7') then to_address
  END as user,
  amount  
  
FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
WHERE (TO_ADDRESS LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7')
OR FROM_address LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7'))
AND contract_address LIKE lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831')
)
GROUP BY 1,2 

    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
hyperliquid_bridge_data = load_hyperliquid_bridge_data(timeframe, start_date, end_date)
# --- Row 2 charts -------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

with col1:
    fig_stacked = px.bar(
        hyperliquid_bridge_data,
        x="WEEK",
        y="VOLUME",
        color="ACTION_TYPE",
        title="Weekly Bridge Volume by Action Type"
    )
    fig_stacked.update_layout(
        barmode="stack",
        xaxis_title="",
        yaxis_title="USD",
        legend=dict(
            orientation="h", 
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        legend_title_text=""  
    )
    st.plotly_chart(fig_stacked, use_container_width=True)

with col2:
    fig_stacked = px.bar(
        hyperliquid_bridge_data,
        x="WEEK",
        y="USERS",
        color="ACTION_TYPE",
        title="Weekly Bridge Users by Action Type"
    )
    fig_stacked.update_layout(
        barmode="stack",
        xaxis_title="",
        yaxis_title="Wallet count",
        legend=dict(
            orientation="h", 
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        legend_title_text=""  
    )
    st.plotly_chart(fig_stacked, use_container_width=True)

with col3:
    fig_stacked = px.bar(
        hyperliquid_bridge_data,
        x="WEEK",
        y="EVENTS",
        color="ACTION_TYPE",
        title="Weekly Bridge Events by Action Type"
    )
    fig_stacked.update_layout(
        barmode="stack",
        xaxis_title="",
        yaxis_title="Txns count",
        legend=dict(
            orientation="h", 
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        legend_title_text=""  
    )
    st.plotly_chart(fig_stacked, use_container_width=True)

# --- Row 3 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_hyperliquid_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    SELECT 
  round(avg(amount)) as "Avg Deposit Size USD",
  round(median(amount)) as "Median Deposit Size USD",
  count(*) as "Total Deposits"
     
FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
WHERE (
  to_address LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4')
  and contract_address LIKE lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')
)
OR (
  to_address LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7')
  and contract_address LIKE lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831')
)
    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
df_hyperliquid_stats = load_hyperliquid_stats(start_date, end_date)
# --- KPI Row ------------------------------------------------------------------------------------------------------
col1, col2, col3 = st.columns(3)

col1.metric(
    label="Avg Deposit Size (USD)",
    value=f"${df_hyperliquid_stats["Avg Deposit Size USD"][0]:,} "
)

col2.metric(
    label="Median Deposit Size (USD)",
    value=f"${df_hyperliquid_stats["Median Deposit Size USD"][0]:,} "
)

col3.metric(
    label="Total Deposits",
    value=f"{df_hyperliquid_stats["Total Deposits"][0]:,} Txns"
)

# --- Row 4 --------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_deposit_distribution(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    SELECT 
  CASE when amount < 100 then 'a/ below $100'
  when amount < 1000 then 'b/ $100 - $1K'
  when amount < 10000 then 'c/ $1K - $10K'
  when amount < 100000 then 'd/ $10K - $100K'
  else 'e/ S100K+' end as deposit_size,
  count(*) as deposits
     
FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
WHERE (
  to_address LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4')
  and contract_address LIKE lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')
)
OR (
  to_address LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7')
  and contract_address LIKE lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831')
)
GROUP BY 1 
    """

    return pd.read_sql(query, conn)

# --- Load Data --------------------------------------------------------------------------------------
deposit_distribution = load_deposit_distribution(start_date, end_date)
# ----------------------------------------------------------------------------------------------------
bar_fig = px.bar(
    deposit_distribution,
    x="DEPOSIT_SIZE",
    y="DEPOSITS",
    title="Breakdown of Deposits by Size",
    color_discrete_sequence=["#97fce4"]
)
bar_fig.update_layout(
    xaxis_title="Deposit Size",
    yaxis_title="Txns count",
    bargap=0.2
)

# ---------------------------------------
color_scale = {
    'a/ below $100': '#97fce4',        
    'b/ $100 - $1K': '#4ee4c1',
    'c/ $1K - $10K': '#1bba94',
    'd/ $10K - $100K': '#069a77',
    'e/ S100K+': '#017459'
}

fig_donut_volume = px.pie(
    deposit_distribution,
    names="DEPOSIT_SIZE",
    values="DEPOSITS",
    title="Share of Deposits by Size",
    hole=0.5,
    color="DEPOSIT_SIZE",
    color_discrete_map=color_scale
)

fig_donut_volume.update_traces(textposition='outside', textinfo='percent+label', pull=[0.05]*len(deposit_distribution))
fig_donut_volume.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(bar_fig, use_container_width=True)

with col2:
    st.plotly_chart(fig_donut_volume, use_container_width=True)

st.markdown(
    """
    <div style="background-color:#c3c3c3; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Depositor Metrics</h2>
    </div>
    """,
    unsafe_allow_html=True
)

# --- Row 5 ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_total_hyperliquid_stats(start_date, end_date):
    
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with overview as (
SELECT
  day,
  count(*) as new_depositors,
  sum(new_depositors) over (ORDER by day) as total_depositors 

FROM (
  SELECT 
    FROM_ADDRESS,
    MIN(date(block_timestamp)) as day
     
  FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  WHERE (
    to_address LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4')
    and contract_address LIKE lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')
  )
  OR (
    to_address LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7')
    and contract_address LIKE lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831')
  )
  GROUP BY 1 
)
GROUP BY 1 
ORDER by day DESC
limit 1)

select TOTAL_DEPOSITORS
from overview

    """

    df = pd.read_sql(query, conn)
    return df

# --- Load Data ----------------------------------------------------------------------------------------------------
total_hyperliquid_stats = load_total_hyperliquid_stats(start_date, end_date)
# --- KPI Row ------------------------------------------------------------------------------------------------------
col1 = st.columns(1)[0]

col1.metric(
    label="Total Hyperliquid Depositors",
    value=f"💼{total_hyperliquid_stats['TOTAL_DEPOSITORS'][0]:,} Wallets"
)
# --- Row 6 ---------------------------------------------------------------------------------------------------------
@st.cache_data
def load_new_depositors_over_time(timeframe, start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    SELECT
  day,
  count(*) as new_depositors,
  sum(new_depositors) over (ORDER by day) as total_depositors 

FROM (
  SELECT 
    FROM_ADDRESS,
    MIN(date(block_timestamp)) as day
     
  FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  WHERE (
    to_address LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4')
    and contract_address LIKE lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')
  )
  OR (
    to_address LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7')
    and contract_address LIKE lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831')
  )
  GROUP BY 1 
)
GROUP BY 1 
ORDER by day DESC

    """

    return pd.read_sql(query, conn)

# --- Load Data ----------------------------------------------------------------------------------------------------
new_depositors_over_time = load_new_depositors_over_time(timeframe, start_date, end_date)
# --- Row 3 --------------------------------------------------------------------------------------------------------

fig1 = go.Figure()

fig1.add_trace(go.Bar(
    x=new_depositors_over_time["DAY"], 
    y=new_depositors_over_time["NEW_DEPOSITORS"], 
    name="New Depositors", 
    yaxis="y1",
    marker_color="#8ef1d9"
))

fig1.add_trace(go.Scatter(
    x=new_depositors_over_time["DAY"], 
    y=new_depositors_over_time["TOTAL_DEPOSITORS"], 
    name="Total Depositors", 
    mode="lines", 
    yaxis="y2",
    line=dict(color="#0c8669")
))

fig1.update_layout(
    title="Daily New and Total Hyperliquid Depositors",
    yaxis=dict(title="Wallet count"),  
    yaxis2=dict(title="Wallet count", overlaying="y", side="right"),  
    xaxis=dict(title=" "),
    barmode="group",
    legend=dict(
        orientation="h",   
        yanchor="bottom", 
        y=1.05,           
        xanchor="center",  
        x=0.5
    )
)
st.plotly_chart(fig1, use_container_width=True)

st.markdown(
    """
    <div style="background-color:#c3c3c3; padding:1px; border-radius:10px;">
        <h2 style="color:#000000; text-align:center;">Past 30 Day Depositor Metrics</h2>
    </div>
    """,
    unsafe_allow_html=True
)
# --- Row 7 ---------------------------------------------------------------------------------------------------------------------
@st.cache_data
def load_Depositors_by_Arbitrum_Use_Group(start_date, end_date):
    start_str = start_date.strftime("%Y-%m-%d")
    end_str = end_date.strftime("%Y-%m-%d")

    query = f"""
    with tab1 as (
  SELECT 
    FROM_ADDRESS as user1,
    MIN(date(block_timestamp)) as first_deposit_day,
    sum(amount) as deposit_volume
       
  FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  WHERE (
    to_address LIKE lower('0xC67E9Efdb8a66A4B91b1f3731C75F500130373A4')
    and contract_address LIKE lower('0xFF970A61A04b1cA14834A43f5dE4533eBDDB5CC8')
  )
  OR (
    to_address LIKE lower('0x2Df1c51E09aECF9cacB7bc98cB1742757f163dF7')
    and contract_address LIKE lower('0xaf88d065e77c8cC2239327C5EDb3A432268e5831')
  )
  GROUP BY 1 
  HAVING first_deposit_day >= DATEADD(day, -30, CURRENT_DATE())
), tab2 as (
  SELECT
    FROM_address as user2,
    min(block_timestamp) as first_arb_transaction
  FROM ARBITRUM_ONCHAIN_CORE_DATA.CORE.FACT_TRANSACTIONS
  WHERE from_address in (SELECT user1 from tab1)
  GROUP BY 1 
)
SELECT 
  CASE when ABS(DATEDIFF(hour, first_arb_transaction, first_deposit_day)) <= 24 
       then 'Deposit Wallet' 
       else 'Arbitrum User Wallet' 
  end as wallet_type,
  count(*) as wallets,
  avg(deposit_volume) as avg_user_deposit_volume,
  median(deposit_volume) as median_user_deposit_volume
FROM tab1
  LEFT outer JOIN tab2 
    on user1 = user2 
GROUP BY 1 
    """

    return pd.read_sql(query, conn)

# --- Load Data --------------------------------------------------------------------------------------
Depositors_by_Arbitrum_Use_Group = load_Depositors_by_Arbitrum_Use_Group(start_date, end_date)
# ----------------------------------------------------------------------------------------------------
bar_fig = px.bar(
    Depositors_by_Arbitrum_Use_Group,
    x="WALLET_TYPE",
    y="AVG_USER_DEPOSIT_VOLUME",
    title="Avg Deposit by Wallet Type",
    color_discrete_sequence=["#03ad85"]
)
bar_fig.update_layout(
    xaxis_title="Wallet Type",
    yaxis_title="$USD",
    bargap=0.2
)

# ---------------------------------------
bar_fig = px.bar(
    Depositors_by_Arbitrum_Use_Group,
    x="WALLET_TYPE",
    y="MEDIAN_USER_DEPOSIT_VOLUME",
    title="Median Deposit by Wallet Type",
    color_discrete_sequence=["#03ad85"]
)
bar_fig.update_layout(
    xaxis_title="Wallet Type",
    yaxis_title="$USD",
    bargap=0.2
)

# ---------------------------------------
color_scale = {
    'Arbitrum User Wallet': '#97fce4',        
    'Deposit Wallet': '#068f6e'
}

fig_donut_volume = px.pie(
    Depositors_by_Arbitrum_Use_Group,
    names="WALLET_TYPE",
    values="WALLETS",
    title="Depositors by Arbitrum Use Group",
    hole=0.5,
    color="WALLET_TYPE",
    color_discrete_map=color_scale
)

fig_donut_volume.update_traces(textposition='outside', textinfo='percent+label', pull=[0.05]*len(Depositors_by_Arbitrum_Use_Group))
fig_donut_volume.update_layout(showlegend=True, legend=dict(orientation="v", y=0.5, x=1.1))

col1, col2 = st.columns(2)

with col1:
    st.plotly_chart(bar_fig, use_container_width=True)
with col2:
    st.plotly_chart(bar_fig, use_container_width=True)    
with col3:
    st.plotly_chart(fig_donut_volume, use_container_width=True)
