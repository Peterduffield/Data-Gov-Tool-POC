import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import snowflake.snowpark.functions as F
from datetime import date

from pages import intro
from pages import analytics
from pages import engineering
from pages import enablement
from pages import apps
from pages import architecture
from pages import managed_services
from pages import tech_ops
from pages import ai_ml
from pages import em

def snowpark_conn(
        connection_parameters: dict = { "account": "xx",
        "user": "xx",
        "password": "xx",
        "role": "DATA_ENGINEER",
        "warehouse": "DEMO_WH"
        }):
    
    session = Session.builder.configs(connection_parameters).create()  
    session.use_database("LOOKER")
    session.use_schema("LOOKER_SOURCE")
    return session

demo_session = snowpark_conn()
df = demo_session.sql('select * from LOOKER.LOOKER_SOURCE.DASHBOARD limit 10')
st.write(df)


PAGES = {
        "Intro": intro,
        "Analytics": analytics,
        "Engineering": engineering,
        "Enablement": enablement,
        "Apps": apps,
        "Architecture": architecture,
        "Managed Services": managed_services,
        "TechOps": tech_ops,
        "AI/ML": ai_ml,
        "EM": em,
    }

def main():
    st.sidebar.title("Navigation")

    if 'personal_access_token_name' not in st.session_state or 'personal_access_token_secret' not in st.session_state:
        with st.sidebar:
            st.write("Enter your Tableau credentials")
            personal_access_token_name = st.text_input('Personal Access Token Name')
            personal_access_token_secret = st.text_input('Personal Access Token Secret', type='password')
            if st.button('Submit Credentials'):
                st.session_state['personal_access_token_name'] = personal_access_token_name
                st.session_state['personal_access_token_secret'] = personal_access_token_secret

    if 'personal_access_token_name' in st.session_state and 'personal_access_token_secret' in st.session_state:
        choice = st.sidebar.radio("Select a page:", list(PAGES.keys()))

        if choice in PAGES:
            PAGES[choice].main()
    
    choice = st.sidebar.radio("Select a page:", list(PAGES.keys()))
    if choice in PAGES:
        PAGES[choice].main()
if __name__ == "__main__":
    main()