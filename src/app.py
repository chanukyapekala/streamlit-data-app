import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

# Function to execute SQL queries
def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config()  # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

# Set up Streamlit page
st.set_page_config(layout="wide")

# Caching queries with a 30-second time-to-live

@st.cache_data(ttl=30)
def getCategorizedBillingData():
    return sqlQuery("""
        select
        case
          when sku_name like '%SERVERLESS%SQL%' then 'SERVERLESS_SQL'
          when sku_name like '%ALL%PURPOSE%' then 'ALL_PURPOSE'
          when sku_name like '%ALL%PURPOSE%Compute' then 'ALL_PURPOSE_SERVERLESS'
          else 'Others'
        end as product,
        usage_date date,
        cast(sum(usage_quantity) as int) as usage
        from system.billing.usage
        group by sku_name, usage_date
        order by product desc
    """)

@st.cache_data(ttl=30)
def getTablePrivileges():
    return sqlQuery("""
        SELECT
          t.table_catalog,
          t.table_schema,
          t.table_name,
          t.table_owner,
          p.privilege_type,
          p.grantee,
          p.grantor
        FROM
          system.information_schema.tables t
        JOIN
          system.information_schema.table_privileges p
        ON
          t.table_catalog = p.table_catalog
          AND t.table_schema = p.table_schema
          AND t.table_name = p.table_name
        WHERE
          t.table_schema NOT IN ('information_schema', 'default')
        ORDER BY
          t.table_catalog,
          t.table_schema,
          t.table_name;
    """)

@st.cache_data(ttl=30)
def getSchemata():
    return sqlQuery("""
        SELECT * FROM system.information_schema.schemata;
    """)

# Fetch data for all queries
categorized_billing_data = getCategorizedBillingData()
table_privileges = getTablePrivileges()
schemata = getSchemata()

# Dashboard layout
st.header("My Databricks Governance Dashboard")

# Section 1: Categorized Billing Usage
st.subheader("Categorized Billing Usage")
col1, col2 = st.columns([1, 2])
with col1:
    st.subheader("Filter by Date")
    start_date = st.date_input("Start Date")
    end_date = st.date_input("End Date")
    filtered_data = categorized_billing_data[(categorized_billing_data['date'] >= start_date) & (categorized_billing_data['date'] <= end_date)]
with col2:
    st.bar_chart(filtered_data, x="product", y="usage")
st.dataframe(data=categorized_billing_data, height=400, use_container_width=True)

# Section 2: Table Privileges
st.subheader("Table Privileges")
st.dataframe(data=table_privileges, height=400, use_container_width=True)

# Section 3: Schemata
st.subheader("Available Schemas")
st.dataframe(data=schemata, height=400, use_container_width=True)