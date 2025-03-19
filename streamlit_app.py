from snowflake.snowpark import Session
import streamlit as st

def main():

    conn = st.connection("snowflake")
    df = conn.query("SELECT * FROM BUSINESS_GLOSSARY")
    table = df.to_pandas()
    st.dataframe(table)
if __name__ == "__main__":
    main()
