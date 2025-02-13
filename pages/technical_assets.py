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
 
def main():
    session  = get_snowflake_session()
    if session:    
        try:
            # Execute SQL queries
            test_query = "SELECT * from USE_CASE_INVENTORY"
            test_df = session.sql(test_query).to_pandas()
            test_query_2 = "SELECT * FROM USE_CASE_INVENTORY WHERE asset_id = 1"
            test_df_2 = session.sql(test_query_2).to_pandas()

            # Display data            
            st.title("Use Case and Domain Inventory") 
            st.dataframe(test_df, hide_index=True)    
            st.write("")
            description()
            col1, col2 = st.columns([1, 3])
            with col1:
                select_filter = st.radio("Select One", ["Primary Domain", "Use Case"])
            with col2:
                if select_filter == "Primary Domain":
                    selected_domain = st.radio("Select One", ["Asset management", "Finance", "Procurment", "Compliance", "Operations"],
                             captions=[
                                 "Related Domain(s): Operations",
                                 "Related Domain(s): Asset management",
                                 "Related Domain(s): Operations, Finance",
                                 "Related Domain(s): Asset management, Operations",
                                 "Related Domain(s): Asset management, Finance",
                             ],)
                    if selected_domain == "Asset management":
                        df = st.dataframe(test_df,hide_index=True)
                    else:
                        df = st.dataframe(test_df_2,hide_index=True)
                if select_filter == "Use Case":
                    selected_use_case = st.radio("Select One", ["Asset Lifecycle Optimization", "Depreciation Forecasting Accuracy", "Vendor Performance Management", "Regulatory Compliance Automation", "Predictive Maintenance for Assets"],
                             captions=[
                                 "Business Use Case: Implement predictive maintenance and usage-based lifecycle tracking.",
                                 "Business Use Case: Enhance depreciation models using real-time asset usage and condition data.",
                                 "Business Use Case: Develop a vendor scorecard tracking repair quality, cost, and response time.",
                                 "Business Use Case: Build a compliance dashboard that monitors asset-related regulations in real time.",
                                 "Business Use Case: Implement IoT-based sensors and machine learning models for failure prediction.",
                             ],)
                    if selected_use_case == "Asset Lifecycle Optimization":
                        df = st.dataframe(test_df,hide_index=True)
                    else:
                        df = st.dataframe(test_df_2,hide_index=True)
            
            df

                
        except Exception as e:
            st.error(f"Error fetching data from Snowflake: {str(e)}")
if __name__ == "__main__":
    main()