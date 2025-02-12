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

def overview():
    faq_markdown = """  
        ## Breif Overview
        ***details***
    """

    st.markdown(faq_markdown)

def main():
    st.title("Intro")
    overview()

if __name__ == "__main__":
    main()