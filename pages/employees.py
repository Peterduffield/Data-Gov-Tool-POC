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
            select_all_employee_query = "SELECT * FROM EMPLOYEES"

           
            select_all_employee_df = session.sql(select_all_employee_query).to_pandas()
            employee_name_list = select_all_employee_df['EMPLOYEE_NAME'].tolist()

            # Display data            
            st.title("Employee Catalog") 
            st.write("")
            description()
            selected_employee = st.selectbox("Select One", employee_name_list)
            col1, col2 = st.columns(2)
            with col1:
                if selected_employee != "Select One":
                    employee_business_role = select_all_employee_df[select_all_employee_df["EMPLOYEE_NAME"] == selected_employee]
                    st.markdown(f"<h1> {employee_business_role.shape[3]} </h1>", unsafe_allow_html=True) 
            with col2:
                if selected_employee != "Select One":
                    employee_business_role = select_all_employee_df[select_all_employee_df["EMPLOYEE_NAME"] == selected_employee]
                    st.markdown(f"<h1> {employee_business_role.shape[4]} </h1>", unsafe_allow_html=True)                 
        except Exception as e:
            st.error(f"Error fetching data from Snowflake: {str(e)}")
if __name__ == "__main__":
    main()