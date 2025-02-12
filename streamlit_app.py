from mmap import PAGESIZE
from snowflake.snowpark import Session
import streamlit as st
from pages import intro



import streamlit as st

def main():
    st.sidebar.title("Navigation")

    # Define PAGES before using it
    PAGES = {
        "INTRO": intro,
    }

   # Check if data has been uploaded (replace 'data_uploaded' with the actual key)
    if st.session_state.get('session', False):  # Check with a default value of False
        # If data has been uploaded, allow access to all pages
        choice = st.sidebar.radio("Select a page:", list(PAGES.keys()))
        if choice in PAGES:
            PAGES[choice].main()
    else:
        # If data has not been uploaded, only display the first three pages
        choice = st.sidebar.radio("Select a page:", ["Looker Assessment Info", "User Credentials", "Data Upload"])
        if choice in PAGES:
            PAGES[choice].main()

if __name__ == "__main__":
    main()
