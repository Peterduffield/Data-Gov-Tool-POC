import streamlit as st
import pandas as pd
from snowflake.connector import connect 
from snowflake.snowpark import Session 


@st.cache_resource
def create_snowflake_session(snowflake_account, snowflake_user, snowflake_password, snowflake_role, snowflake_wh, snowflake_db, snowflake_schema):
  # Replace with user credentials and connection details (consider environment variables)
  account = snowflake_account
  user = snowflake_user
  password = snowflake_password
  role = snowflake_role
  warehouse = snowflake_wh
  database = snowflake_db
  schema = snowflake_schema

  # Create connection options dictionary
  options = {
   "account": account,
   "user": user,
   "password": password,
   "role": role,
   "warehouse": warehouse,
   "database": database,
   "schema": schema,

  }

  # Create a session using Session.builder.configs
  snowflake_session = Session.builder.configs(options).create()
  return snowflake_session

@st.cache(allow_output_mutation=True)
def get_snowflake_session():
    snowflake_account = st.session_state.get("snowflake_account")
    snowflake_user = st.session_state.get("snowflake_user")
    snowflake_db = st.session_state.get("snowflake_db")
    snowflake_schema = st.session_state.get("snowflake_schema")
    snowflake_password = st.session_state.get("snowflake_password")
    snowflake_role = st.session_state.get("snowflake_role")
    snowflake_wh = st.session_state.get("snowflake_wh")
    return create_snowflake_session(snowflake_account, snowflake_user, snowflake_password, snowflake_role, snowflake_wh, snowflake_db, snowflake_schema)
    

def workbook_types():
    faq_markdown = """  
        ## 1. Overview
    """

    st.markdown(faq_markdown)

def main():
    st.title("Intro")
    st.subheader("User Credentials")


if __name__ == "__main__":
    main()