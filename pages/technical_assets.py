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
            select_all_query = "SELECT * FROM BUSINESS_GLOSSARY"
            asset_man_query = "SELECT * FROM BUSINESS_GLOSSARY WHERE DOMAIN = 'Operations'"
            finance_query = "SELECT * FROM BUSINESS_GLOSSARY WHERE DOMAIN = 'Asset Management'"
            procurment_query = "SELECT * FROM BUSINESS_GLOSSARY WHERE DOMAIN in ('Operations', 'Finance')"
            compliance_query = "SELECT * FROM BUSINESS_GLOSSARY WHERE DOMAIN in ('Asset Management', 'Operations')"
            operations_query = "SELECT * FROM BUSINESS_GLOSSARY WHERE DOMAIN in ('Asset Management', 'Finance')"
            data_catalog_query = "SELECT * FROM DATA_CATALOG"

            select_all_df = session.sql(select_all_query).to_pandas()
            business_key_term_list = select_all_df['KEY_BUSINESS_TERM_NAME'].tolist()
            business_asset_list = [value.replace(" ", "_") if value is not None else "Unknown" for value in business_key_term_list]

            asset_man_df = session.sql(asset_man_query).to_pandas()
            finance_df = session.sql(finance_query).to_pandas()
            procurment_df = session.sql(procurment_query).to_pandas()
            compliance_df = session.sql(compliance_query).to_pandas()
            operations_df = session.sql(operations_query).to_pandas()
            catalog_df = session.sql(data_catalog_query).to_pandas()


            # Display data            
            st.title("Use Case and Domain Inventory") 
            st.write("")
            description()
            select_filter = st.radio("Select One", ["Primary Domain", "Use Case"])
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
                    st.dataframe(asset_man_df,hide_index=True)
                if selected_domain == "Finance":
                    st.dataframe(finance_df,hide_index=True)
                if selected_domain == "Procurment":
                    st.dataframe(procurment_df,hide_index=True)
                if selected_domain == "Compliance":
                    st.dataframe(compliance_df,hide_index=True)
                if selected_domain == "Operations":
                    st.dataframe(operations_df,hide_index=True)

            if select_filter == "Use Case":
                selected_use_case = st.radio("Select One", ["Asset Lifecycle Optimization", "Depreciation Forecasting Accuracy", "Vendor Performance Management", "Regulatory Compliance Automation", "Predictive Maintenance for Assets"],
                             captions=[
                                 "Business Use Case: Implement predictive maintenance and usage-based lifecycle tracking.",
                                 "Business Use Case: Enhance depreciation models using real-time asset usage and condition data.",
                                 "Business Use Case: Develop a vendor scorecard tracking repair quality, cost, and response time.",
                                 "Business Use Case: Build a compliance dashboard that monitors asset-related regulations in real time.",
                                 "Business Use Case: Implement IoT-based sensors and machine learning models for failure prediction.",
                             ],)
                if selected_domain == "Asset Lifecycle Optimization":
                    st.dataframe(asset_man_df,hide_index=True)
                if selected_domain == "Depreciation Forecasting Accuracy":
                    st.dataframe(finance_df,hide_index=True)
                if selected_domain == "Vendor Performance Managemen":
                    st.dataframe(procurment_df,hide_index=True)
                if selected_domain == "Regulatory Compliance Automation":
                    st.dataframe(compliance_df,hide_index=True)
                if selected_domain == "Predictive Maintenance for Assets":
                    st.dataframe(operations_df,hide_index=True)

            asset_id_selected = st.selectbox("Select One:", ["Search..."] + business_asset_list)
            filtered_df = catalog_df[catalog_df["ATTRIBUTE_NAME"] == asset_id_selected]
            st.dataframe(filtered_df, hide_index=True)
        except Exception as e:
            st.error(f"Error fetching data from Snowflake: {str(e)}")
if __name__ == "__main__":
    main()