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
            font-size: 2em; /* Adjust size as needed */
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
        unsafe_allow_html=True  # ✅ Fixed the typo
    )

   
    selected_business_term = st.selectbox("Select a Business Term", key_term_list,index=None)
    st.write(selected_business_term)
    
    # Display results in Streamlit
    st.dataframe(business_glossary_tbl)

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
