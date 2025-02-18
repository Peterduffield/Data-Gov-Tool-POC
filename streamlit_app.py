from mmap import PAGESIZE
from snowflake.snowpark import Session
import streamlit as st
from pages import intro
from pages import technical_assets
from pages import employees



import streamlit as st

def main():
    st.sidebar.title("Navigation")

    # Define PAGES before using it
    PAGES = {
        "INTRO": intro,
        "Domain Inventory": technical_assets,
        "Assets by Employee": employees,
    }

    if st.session_state.get('session', False): 
        choice = st.sidebar.radio("Select a page:", list(PAGES.keys()))
        if choice in PAGES:
            PAGES[choice].main()

if __name__ == "__main__":
    main()
