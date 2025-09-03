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
