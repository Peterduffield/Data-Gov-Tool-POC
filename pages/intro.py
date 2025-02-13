import streamlit as st
import pandas as pd
from snowflake.connector import connect 
from snowflake.snowpark import Session 


# Function to create a Snowflake session (cached based on database and schema)
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

def overview():
    faq_markdown = """  
        #Breif Overview
        ***details***
    """
    st.markdown(faq_markdown)   
 
def main():
    st.title("User Credentials")
    col1, col2 = st.columns(2)
    with col1:
        snowflake_account = st.text_input("Enter Snowflake Account:", value="jsa18243")
        snowflake_user = st.text_input("Enter Snowflake User:", value="lkr_python_runner") 
        snowflake_password = st.text_input("Enter Snowflake Password:", type= "password")
        snowflake_role = st.text_input("Enter Snowflake Role:", value="DATA_ENGINEER")
    with col2:
        snowflake_wh = st.text_input("Enter Snowflake Warehouse:", value="DEMO_WH")
        snowflake_db = st.text_input("Enter Snowflake Database Name:", value="LOOKER")
        snowflake_schema = st.text_input("Enter Snowflake Schema Name:", value="LOOKER_SOURCE")
    if st.button("CONNECT SNOWFLAKE"):
        st.session_state["snowflake_account"] = snowflake_account
        st.session_state["snowflake_user"] = snowflake_user
        st.session_state["snowflake_password"] = snowflake_password
        st.session_state["snowflake_role"] = snowflake_role
        st.session_state["snowflake_wh"] = snowflake_wh
        st.session_state["snowflake_db"] = snowflake_db
        st.session_state["snowflake_schema"] = snowflake_schema

        session = get_snowflake_session()
        st.session_state["session"] = session
        if session:
            st.success ("Succsessfully connected to Snowflake!")
        else:
            st.error ("Error connecting, please check credentials")
    overview()

if __name__ == "__main__":
    main()