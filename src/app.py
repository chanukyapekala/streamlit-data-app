import os
from databricks import sql
from databricks.sdk.core import Config
import streamlit as st
import pandas as pd

# Ensure environment variable is set correctly
assert os.getenv('DATABRICKS_WAREHOUSE_ID'), "DATABRICKS_WAREHOUSE_ID must be set in app.yaml."

def sqlQuery(query: str) -> pd.DataFrame:
    cfg = Config() # Pull environment variables for auth
    with sql.connect(
        server_hostname=cfg.host,
        http_path=f"/sql/1.0/warehouses/{os.getenv('DATABRICKS_WAREHOUSE_ID')}",
        credentials_provider=lambda: cfg.authenticate
    ) as connection:
        with connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()

st.set_page_config(layout="wide")

@st.cache_data(ttl=30)  # only re-query if it's been 30 seconds
def getData():
    # This example query depends on the nyctaxi data set in Unity Catalog, see https://docs.databricks.com/en/discover/databricks-datasets.html for details
    return sqlQuery("select sku_name as product, cast(sum(usage_quantity) as int) as usage, usage_date date from system.billing.usage group by sku_name, usage_date order by usage desc")

data = getData()

st.header("My Databricks Billing Usage")
col1, col2 = st.columns([1, 2])
with col1:
    st.subheader("Filter by date")
    startDate = st.date_input("StartDate")
    endDate = st.date_input("EndDate")
    data = data[(data['date'] >= startDate) & (data['date'] <= endDate)]
with col2:
    st.bar_chart(data, x="product", y="usage")

st.dataframe(data=data, height=600, use_container_width=True)