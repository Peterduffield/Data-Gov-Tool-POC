import streamlit as st
import pandas as pd
from snowflake.snowpark import Session

from pages import intro


PAGES = {
        "Intro": intro
    }

def main():
    st.sidebar.title("Navigation")

    choice = st.sidebar.radio("Select a page:", list(PAGES.keys()))
    if choice in PAGES:
        PAGES[choice].main()
if __name__ == "__main__":
    main()