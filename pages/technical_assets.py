import streamlit as st
import pandas as pd
from snowflake.connector import connect 
from snowflake.snowpark import Session 
from pages.intro import get_snowflake_session 

def description():
    faq_markdown = """  
        #breif Description
        ***details***
    """
    st.markdown(faq_markdown)   
 
@st.cache_data
def main():
    session  = get_snowflake_session()
    if session:    
        try:
            # Execute SQL queries
            test_query = "SELECT * from BUSINESS_GLOSSARY"
            test_df = session.sql(test_query).to_pandas()

            # Display data            
            st.title("Technical Assets") 

            st.markdown(f"<h1> {test_df.shape[0]} </h1>", unsafe_allow_html=True)    
        
            description()
        except Exception as e:
            st.error(f"Error fetching data from Snowflake: {str(e)}")
if __name__ == "__main__":
    main()