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
data_catalog_tbl = session.sql("SELECT * FROM DATA_CATALOG").to_pandas()


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
            margin-bottom: 5px; /* Spacing between title and subtitle */
        }
        .title-container h2 {
            font-size: 1.5em; /* Adjust size as needed */
            color: #888; /* Example subtitle color */
        }
        </style>
        <div class="title-container">
            <h1>Data Governance Tool</h1>
            <h2>Key Business Term Glossary</h2>
        </div>
        """,
        unsafe_allow_html=True
    )

    col1, col2 = st.columns(2)
    with col1:
        selected_business_term = st.selectbox("Select a Business Term", business_glossary_tbl['KEY_BUSINESS_TERM_NAME'].to_list(),index=None)
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
        st.write('Data Owner')
        selected_data_owner = filtered_df['DATA_OWNER_EMPLOYEE_NAME'].iloc[0]
        st.markdown(f"## {selected_data_owner}", unsafe_allow_html=True)
        
        st.write('Data Steward')
        selected_data_steward = filtered_df['DATA_STEWARD_EMPLOYEE_NAME'].iloc[0]
        st.markdown(f"## {selected_data_steward}", unsafe_allow_html=True)

    with col4:
        st.write('Definition')
        selected_definition = filtered_df['DEFINITION'].iloc[0]
        st.markdown(f"## {selected_definition}", unsafe_allow_html=True)

        st.write('Authoritative Source(s)')
        selected_authoratative_source = filtered_df['AUTHORITATIVE_SOURCE'].iloc[0]
        st.markdown(f"## {selected_authoratative_source}",unsafe_allow_html=True)
    
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
        st.markdown(f"## {related_info}", unsafe_allow_html=True) 

    # Combine attributes and databases into a formatted string

    # Display DataFrame
    st.dataframe(filtered_df, hide_index=True)
  


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

if __name__ == "__main__":
    main()
