from snowflake.snowpark import Session
import streamlit as st
import pandas as pd

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
data_catalog_tbl = session.sql("SELECT * FROM DATA_CATALOG").to_pandas()
employee_catalog_tbl = session.sql("SELECT * FROM EMPLOYEE_CATALOG").to_pandas()
employee_use_case_catalog_tbl = session.sql("SELECT e.*, i.* FROM EMPLOYEE_CATALOG e LEFT JOIN USE_CASE_INVENTORY_TBL i ON e.EMPLOYEE_NAME = i.BUSINESS_STAKEHOLDER").to_pandas()
employee_glossary_tbl = session.sql("SELECT DISTINCT e.*,  bg.* from EMPLOYEE_CATALOG e LEFT JOIN BUSINESS_GLOSSARY bg ON bg.Data_Owner_Employee_Name = e.EMPLOYEE_NAME OR bg.Data_Steward_Employee_Name = e.EMPLOYEE_NAME WHERE e.GOVERNANCE_ROLE = 'Data Owner' or e.GOVERNANCE_ROLE = 'Data Steward'").to_pandas()
employee_catalog_tbl = session.sql("SELECT DISTINCT e.*,  dc.* from EMPLOYEE_CATALOG e LEFT JOIN DATA_CATALOG dc ON dc.Data_Custodian = e.EMPLOYEE_NAME OR dc.Technical_Data_Steward = e.EMPLOYEE_NAME WHERE e.GOVERNANCE_ROLE = 'Data Custodian' or e.GOVERNANCE_ROLE = 'Technical Data Steward'").to_pandas()
use_case_inventory_tbl = session.sql("SELECT * from USE_CASE_INVENTORY_TBL").to_pandas()

def main():

    st.markdown(
        """
        <style>
        .title-container {
            display: flex;
            flex-direction: column;
            align-items: center;
            text-align: center;
        }
        .title-container h1 {
            font-size: 2.5em; /* Adjust size as needed */
            margin-bottom: 45px; /* Spacing between title and subtitle */
        }
        </style>
        <div class="title-container">
            <h1>Data Governance</h1>
        </div>
        """,
        unsafe_allow_html=True
    )
    tab1, tab2, tab3, tab4 = st.tabs(['Business Glossary', 'Data Catalog', 'Data Role Assignments', 'Use Case Inventory'])
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            selected_business_term = st.selectbox("Select a Business Term", business_glossary_tbl['KEY_BUSINESS_TERM_NAME'].to_list(),index=None, key= "Select Term to Filter")
        with col2:
            selected_business_domain = st.selectbox("Select a Business Domain", business_glossary_tbl['DOMAIN'].to_list(), index=None)
        # Apply filters only if selections are made
        filtered_df = business_glossary_tbl  # Default to all rows
        if selected_business_term:
            filtered_df = filtered_df[filtered_df["KEY_BUSINESS_TERM_NAME"] == selected_business_term]
        if selected_business_domain:
            filtered_df = filtered_df[filtered_df["DOMAIN"] == selected_business_domain]

        col3,col4,col5 =st.columns([1,3,4])
        with col3:

            selected_data_owner = filtered_df['DATA_OWNER_EMPLOYEE_NAME'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Data Owner:</p>
                    <h4 class="value">{selected_data_owner}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )            
            
            selected_data_steward = filtered_df['DATA_STEWARD_EMPLOYEE_NAME'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Technical Data Steward</p>
                    <h4 class="value">{selected_data_steward}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )            

        with col4:
            selected_definition = filtered_df['DEFINITION'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Definition:</p>
                    <h4 class="value">{selected_definition}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )            

            selected_authoratative_source = filtered_df['AUTHORITATIVE_SOURCE'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Authoritative Source</p>
                    <h4 class="value">{selected_authoratative_source}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )            
        
        with col5:
            st.write('Related Critical Data Elements and Database')
            data_catalog_tbl["CATALOG_ID"] = data_catalog_tbl["CATALOG_ID"].astype(str)
            selected_glossary_ids = (
            filtered_df['RELATED_TO_CATALOG_ID_S_']
            .astype(str)       # Ensure it's a string
            .str.split(',\s*') # Split by commas and optional spaces
            .explode()         # Flatten the list
            .astype(str)       # Ensure values remain as strings
            .tolist()          # Convert to a Python list
            )   
                
            related_catalog_id_df = data_catalog_tbl[data_catalog_tbl['CATALOG_ID'].astype(str).isin(selected_glossary_ids)]
            related_attribute_name = related_catalog_id_df['ATTRIBUTE_NAME'].to_list()
            related_attribute_db = related_catalog_id_df['DATABASE_NAME'].to_list()        
            related_info = ", ".join(
                [f"Attribute: {attr}  Database: {db}" for attr, db in zip(related_attribute_name, related_attribute_db)]
            )
            st.markdown(f"#### {related_info}", unsafe_allow_html=True) 
        st.subheader(" ")
        # Display DataFrame
        st.dataframe(filtered_df, hide_index=True)
        st.divider()

        related_data_catalog_tbl, related_business_glossary_tbl = st.columns(2)
        with related_data_catalog_tbl:
            st.write("Related Critical Data Element Catalog")
            st.dataframe(related_catalog_id_df, hide_index=True)

        with related_business_glossary_tbl:
            st.write("Related Key Business Term Glossary")

            business_glossary_tbl["GLOSSARY_ID"] = business_glossary_tbl["GLOSSARY_ID"].astype(str)
            selected_catalog_ids = (
            related_catalog_id_df['MAPS_TO_GLOSSARY_ID_S_']
            .astype(str)       # Ensure it's a string
            .str.split(',\s*') # Split by commas and optional spaces
            .explode()         # Flatten the list
            .astype(str)       # Ensure values remain as strings
            .tolist()          # Convert to a Python list
            )
            related_glossery_df = business_glossary_tbl[business_glossary_tbl["GLOSSARY_ID"].isin(selected_catalog_ids)]
            st.dataframe(related_glossery_df, hide_index=True)


        with st.popover("Update Relationships"):
            st.markdown(" ")
            critical_element_to_update = st.selectbox("Select a Data Element", data_catalog_tbl['ATTRIBUTE_NAME'].to_list(),index=None)
            key_term_to_update = st.selectbox("Select a Business Term", business_glossary_tbl['KEY_BUSINESS_TERM_NAME'].to_list(),index=None, key= "Update Relations Term")
            update, remove = st.columns(2)
            with update:
                if st.button('Update Related Catalog ID'):
                    if key_term_to_update and critical_element_to_update:
                        # Call the stored procedure via Snowflake
                        try:
                            result = session.sql(f"""
                                CALL STREAMLIT_UPDATE_RELATED_DATA_ELEMENTS(
                                    '{critical_element_to_update}', 
                                    '{key_term_to_update}'
                                )
                            """).collect()
                            
                            # Show the result message
                            st.success("Procedure executed successfully!")
                            st.write(result)
                        
                        except Exception as e:
                            st.error(f"An error occurred: {str(e)}")
                    else:
                        st.warning("Please select both a business term and a data element.")
            with remove:
                if st.button('Remove Related Catalog ID'):
                    if key_term_to_update and critical_element_to_update:
                        # Call the stored procedure via Snowflake
                        try:
                            result = session.sql(f"""
                                CALL STREAMLIT_REMOVE_RELATED_DATA_ELEMENTS(
                                    '{critical_element_to_update}', 
                                    '{key_term_to_update}'
                                )
                            """).collect()
                            
                            # Show the result message
                            st.success("Procedure executed successfully!")
                            st.write(result)
                        
                        except Exception as e:
                            st.error(f"An error occurred: {str(e)}")
                    else:
                        st.warning("Please select both a business term and a data element.")

        st.markdown(
        """
        <style>
        .container {
            display: flex;
            justify-content: center;
        }
        .container img {
            transform: scale(0.5);
        }
        </style>
        <div class="container">
            <img src="https://tercera.io/wp-content/uploads/2021/11/hakkoda_logo.png" alt="Hakkoda Logo">
        </div>
        """,
        unsafe_allow_html=True,
        )
    with tab2:
        filtered_data_catalog_tbl = data_catalog_tbl
        selected_filter = st.toggle("Filter by Application or Platform/Server")
        if selected_filter:
            selected_platform_server = st.selectbox("Select a Platform/Server:", data_catalog_tbl['PLATFORM_SERVER'].unique(), index=None)
            if selected_platform_server:
                filtered_data_catalog_tbl = data_catalog_tbl[data_catalog_tbl['PLATFORM_SERVER'] == selected_platform_server]
                st.dataframe(filtered_data_catalog_tbl, hide_index=True)  
            else:
                st.dataframe(filtered_data_catalog_tbl, hide_index=True)       
        else:    
            selected_application = st.selectbox("Select an Application Name:", data_catalog_tbl['APPLICATION_NAME'].unique(), index=None)
            if selected_application:
                filtered_data_catalog_tbl = data_catalog_tbl[data_catalog_tbl['APPLICATION_NAME'] == selected_application]
                st.dataframe(filtered_data_catalog_tbl, hide_index=True)  
            else: 
                st.dataframe(filtered_data_catalog_tbl, hide_index=True)          
        st.write(" ")
        st.divider()
        st.write(" ")
        st.subheader("Attribute Search")
        selected_attribute_catalog_tbl = data_catalog_tbl
        select_box, application = st.columns([3,1])
        with select_box:
            selected_data_attribute = st.selectbox("Select an Attribute:", data_catalog_tbl['ATTRIBUTE_NAME'].unique(), index=None)
            if selected_data_attribute:
                selected_attribute_catalog_tbl = data_catalog_tbl[data_catalog_tbl['ATTRIBUTE_NAME'] == selected_data_attribute]
        with application:
            attribute_application = selected_attribute_catalog_tbl['APPLICATION_NAME'].iloc[0]
            st.markdown(
                f"""
                <style>
                .container {{
                    display: flex;
                    flex-direction: column;
                    align-items: center;
                    text-align: center;
                    width: 100%;
                }}
                .label {{
                    text-align: left;
                    width: 100%;
                    font-size: .9em;
                    color: #555;
                    margin-bottom: 2px;
                }}
                .value {{
                    text-align: center;
                    font-size: 1.5em;
                    font-weight: bold;
                    margin-top: 2px;
                }}
                </style>
                
                <div class="container">
                    <div class="label">Application:</div> <!-- Left-aligned label -->
                    <div class="value">{attribute_application}</div> <!-- Center-aligned value -->
                </div>
                """,
                unsafe_allow_html=True
            )
        col6, col7, col8, col9 = st.columns(4)
        with col6:
            selected_data_custodian = selected_attribute_catalog_tbl['DATA_CUSTODIAN'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Data Custodian:</p>
                    <h4 class="value">{selected_data_custodian}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )

            selected_tech_data_steward = selected_attribute_catalog_tbl['TECHNICAL_DATA_STEWARD'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Technical Data Steward:</p>
                    <h4 class="value">{selected_tech_data_steward}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )            
        with col7:
            selected_data_tag = selected_attribute_catalog_tbl['TAGS'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Tag:</p>
                    <h4 class="value">{selected_data_tag}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )            

            selected_data_db_name = selected_attribute_catalog_tbl['DATABASE_NAME'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Database Name:</p>
                    <h4 class="value">{selected_data_db_name}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )
        with col8:
            selected_data_desc = selected_attribute_catalog_tbl['ATTRIBUTE_DESCRIPTION'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Description:</p>
                    <h4 class="value">{selected_data_desc}</h4>
                </div>
                """,
                unsafe_allow_html=True
            ) 

            selected_data_classification = selected_attribute_catalog_tbl['DATA_CLASSIFICATION'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Classification:</p>
                    <h4 class="value">{selected_data_classification}</h4>
                </div>
                """,
                unsafe_allow_html=True
            ) 
        with col9:
            selected_data_update_freq = selected_attribute_catalog_tbl['UPDATE_FREQUENCY'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Update Frequency:</p>
                    <h4 class="value">{selected_data_update_freq}</h4>
                </div>
                """,
                unsafe_allow_html=True
            ) 

            selected_data_verified = selected_attribute_catalog_tbl['IS_VARIFIED'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Is Verified:</p>
                    <h4 class="value">{selected_data_verified}</h4>
                </div>
                """,
                unsafe_allow_html=True
            ) 

        st.dataframe(selected_attribute_catalog_tbl, hide_index=True)
        st.markdown(
        """
        <style>
        .container {
            display: flex;
            justify-content: center;
        }
        .container img {
            transform: scale(0.5);
        }
        </style>
        <div class="container">
            <img src="https://tercera.io/wp-content/uploads/2021/11/hakkoda_logo.png" alt="Hakkoda Logo">
        </div>
        """,
        unsafe_allow_html=True,
        )
    with tab3:
        domain_filter_employee_tbl = employee_use_case_catalog_tbl
        selected_domain = st.selectbox("Select a Domain:", employee_use_case_catalog_tbl['PRIMARY_DOMAIN'].unique())
        if selected_domain:
           domain_filter_employee_tbl = employee_use_case_catalog_tbl[employee_use_case_catalog_tbl['PRIMARY_DOMAIN'] == selected_domain]
        st.dataframe(domain_filter_employee_tbl)

        st.divider()

        business_data_toggle = st.toggle("Filter by Business Governance Role or Data Governance Role")
        if business_data_toggle:
            employee_gov_role_tbl = employee_catalog_tbl
            selected_data_gov_role = st.selectbox("Select a Data Governace Role:", ['Data Custodian', 'Technical Data Steward'], index=None)
            if selected_data_gov_role:
                employee_gov_role_tbl = employee_catalog_tbl[employee_catalog_tbl['GOVERNANCE_ROLE'] == selected_data_gov_role]
        else:
            employee_gov_role_tbl = employee_glossary_tbl
            selected_business_gov_role = st.selectbox("Select a Business Governance Role:", ['Data Owner', 'Data Steward'], index=None)
            if selected_business_gov_role:
                employee_gov_role_tbl = employee_glossary_tbl[employee_glossary_tbl['GOVERNANCE_ROLE'] == selected_business_gov_role]

        st.dataframe(employee_gov_role_tbl, hide_index=True)

        st.markdown(
        """
        <style>
        .container {
            display: flex;
            justify-content: center;
        }
        .container img {
            transform: scale(0.5);
        }
        </style>
        <div class="container">
            <img src="https://tercera.io/wp-content/uploads/2021/11/hakkoda_logo.png" alt="Hakkoda Logo">
        </div>
        """,
        unsafe_allow_html=True,
        )       
    with tab4:
        use_case_tbl = use_case_inventory_tbl
        selected_use_case = st.selectbox("Select a Use Case:", use_case_inventory_tbl['DATA_USE_CASE_NAME'], index=None)
        if selected_use_case:
            use_case_tbl = use_case_inventory_tbl[use_case_inventory_tbl['DATA_USE_CASE_NAME'] == selected_use_case]
        col10, col11, col12 = st.columns([2,2,4])
        with col10:
            selected_business_stakeholder = use_case_tbl['BUSINESS_STAKEHOLDER'].iloc[0]
            st.markdown(
            f"""
            <style>
            .custom-container {{
                text-align: center;  /* Center the value */
            }}
            .label {{
                text-align: left;  /* Left-align the label */
                font-size: 1.1em;  /* Adjust font size if needed */
                margin-bottom: 5px; /* Small space between label and value */
            }}
            .value {{
                font-size: 1.5em;  /* Adjust font size of the value */
            }}
            </style>
                    
            <div class="custom-container">
                <p class="label">Business Stakeholder:</p>
                <h4 class="value">{selected_business_stakeholder}</h4>
            </div>
            """,
            unsafe_allow_html=True
            )        

            selected_primary_domain = use_case_tbl['PRIMARY_DOMAIN'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Primary Domain:</p>
                    <h4 class="value">{selected_primary_domain}</h4>
                </div>
                """,
                unsafe_allow_html=True
            ) 

        with col11:    
            selected_business_line = use_case_tbl['BUSINESS_LINE'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Business Line:</p>
                    <h4 class="value">{selected_business_line}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )             
        
            selected_strat_obj = use_case_tbl['STRATEGIC_OBJECTIVE'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Strategic Objective</p>
                    <h4 class="value">{selected_strat_obj}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )             
        
        with col12:
            selected_problem_statement = use_case_tbl['PROBLEM_STATEMENT'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Problem Statement:</p>
                    <h4 class="value">{selected_problem_statement}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )             
            
            selected_use_case_desc = use_case_tbl['DATA_USE_CASE_DESCRIPTION'].iloc[0]
            st.markdown(
                f"""
                <style>
                .custom-container {{
                    text-align: center;  /* Center the value */
                }}
                .label {{
                    text-align: left;  /* Left-align the label */
                    font-size: 1.1em;  /* Adjust font size if needed */
                    margin-bottom: 5px; /* Small space between label and value */
                }}
                .value {{
                    font-size: 1.5em;  /* Adjust font size of the value */
                }}
                </style>
                
                <div class="custom-container">
                    <p class="label">Description:</p>
                    <h4 class="value">{selected_use_case_desc}</h4>
                </div>
                """,
                unsafe_allow_html=True
            )             

        st.dataframe(use_case_tbl, hide_index=True)



if __name__ == "__main__":
    main()
