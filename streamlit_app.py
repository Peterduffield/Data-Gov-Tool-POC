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
session = create_snowflake_session()
# Run SQL query
business_glossary_tbl = session.sql("SELECT * FROM BUSINESS_GLOSSARY").to_pandas()

# List of KeY Business Terms
key_term_list = business_glossary_tbl['KEY_BUSINESS_TERM_NAME'].to_list()

def main():
    col1, col2 = st.columns([1,3], gap="large")
    with col1:
        st.image("https://tercera.io/wp-content/uploads/2021/11/hakkoda_logo.png")
    with col2:
        st.title("Data Governance Tool")

    col3, col4, col5 = st.columns([2,3,1])
    with col3:
        st.write(" ")    
    with col4:
        st.subheader("Key Business Term Glossary")
    with col5:
        st.write(" ") 
   
    selected_business_term = st.selectbox("Select a Business Term", key_term_list,index=None)
    st.write(selected_business_term)
    
    # Display results in Streamlit
    st.dataframe(business_glossary_tbl)

if __name__ == "__main__":
    main()
