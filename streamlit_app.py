from snowflake.snowpark import Session
import streamlit as st

# Create a function to connect using Snowpark
def create_snowflake_session():
    connection_parameters = {
        "account": st.secrets["connections"]["snowflake"]["account"],
        "user": st.secrets["connections"]["snowflake"]["user"],
        "password": st.secrets["connections"]["snowflake"]["password"],
        "role": st.secrets["connections"]["snowflake"]["role"],
        "warehouse": st.secrets["connections"]["snowflake"]["warehouse"],
        "database": st.secrets["connections"]["snowflake"]["database"],
        "schema": st.secrets["connections"]["snowflake"]["schema"]
    }
    return Session.builder.configs(connection_parameters).create()

def main():
    st.title("Snowflake Data Viewer")

    session = create_snowflake_session()
    
    # Run SQL query
    df = session.sql("SELECT * FROM BUSINESS_GLOSSARY").to_pandas()
    
    # Display results in Streamlit
    st.dataframe(df)

if __name__ == "__main__":
    main()
