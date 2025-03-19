from snowflake.snowpark import Session
import streamlit as st

# Create a function to connect using Snowpark
SF_CREDENTIALS = {
    "account": "jsa18243",
    "user": "lkr_python_runner",
    "password": "pythonpassword",
    "role": "DATA_ENGINEER",
    "warehouse": "DEMO_WH",
    "database": "DATA_GOV_POC",
    "schema": "POC_TABLES"
}

def create_snowflake_session():
    return Session.builder.configs(SF_CREDENTIALS).create()


def main():
    col1, col2 = st.columns([1,4])
    with col1:
        st.image("https://tercera.io/wp-content/uploads/2021/11/hakkoda_logo.png")
    with col2:
        st.title("Data Governance Tool")
    session = create_snowflake_session()
    
    # Run SQL query
    df = session.sql("SELECT * FROM BUSINESS_GLOSSARY").to_pandas()
    
    # Display results in Streamlit
    st.dataframe(df)

if __name__ == "__main__":
    main()
