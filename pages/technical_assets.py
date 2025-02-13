import streamlit as st
import pandas as pd
import snowflake.connector
from snowflake.snowpark import Session 
from pages.intro import get_snowflake_session
  



@st.cache_data
def main():
   
    session  = get_snowflake_session()
    if session:    
        try:
           
            select_all_query = "SELECT * FROM DATA_GOV_POC.POC_TABLES.BUSINESS_GLOSSARY"
            select_all_df = session.sql(select_all_query).to_pandas()

            st.title("Technical Assets")
            st.dataframe(select_all_df,hide_index=True)

if __name__ == "__main__":
    main()