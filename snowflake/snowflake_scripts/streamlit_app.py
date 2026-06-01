import streamlit as st
from snowflake.snowpark.context import get_active_session

session = get_active_session()
st.set_page_config(page_title="Snowflake DB Clone Tool", layout="centered")


st.title("❇️ Snowflake DB Clone Tool")


roles = [r["name"] for r in session.sql("SHOW ROLES").collect()]



# -----------------------
# INPUTS
# -----------------------

existing_databases = [database["name"] for database in session.sql(f"SHOW DATABASES").collect()]

target_db = st.text_input(
    "Target DB name",
    value="YOUR_DB_NAME_WITH_YOUR_NAME"
)

source_db = st.selectbox(
    "Clone from DB",
    existing_databases
)

owner_role = st.selectbox(
    "Owner role",
    roles
    #["DEVELOPMENT", "SYSADMIN", "SECURITYADMIN"]
)

readonly_role = st.text_input(
    "Read-only role",
    value="read_only_role"
)

# -----------------------
# WARNING LOGIC
# -----------------------

target_db_info = session.sql(f"SHOW DATABASES LIKE '{target_db}'").collect()



if target_db_info:
    target_db_exists, target_db_owner = target_db_info[0]["name"], target_db_info[0]["owner"] 
    st.warning(
        f"{target_db} already exists (owner: {target_db_owner}). "
        "It will be dropped and recreated if you proceed."
    )

# -----------------------
# SQL GENERATION
# -----------------------

def generate_sql(target_db, source_db, owner_role, readonly_role):
    return f"""
-- Drop if exists
DROP DATABASE IF EXISTS {target_db};

-- Clone database
CREATE DATABASE {target_db} CLONE {source_db};

-- Assign ownership

GRANT OWNERSHIP ON DATABASE {target_db} TO ROLE {owner_role};

-- Read-only access
CREATE ROLE IF NOT EXISTS {readonly_role};

GRANT USAGE ON DATABASE {target_db} TO ROLE {readonly_role};
-- Allow schema access
GRANT USAGE ON ALL SCHEMAS IN DATABASE {target_db} TO ROLE {readonly_role};

GRANT USAGE ON FUTURE SCHEMAS IN DATABASE {target_db} TO ROLE {readonly_role};

-- Existing tables
GRANT SELECT ON ALL TABLES IN DATABASE {target_db} TO ROLE {readonly_role};

-- Future tables
GRANT SELECT ON FUTURE TABLES IN DATABASE {target_db} TO ROLE {readonly_role};

-- Existing views
GRANT SELECT ON ALL VIEWS IN DATABASE {target_db} TO ROLE {readonly_role};

-- Future views
GRANT SELECT ON FUTURE VIEWS IN DATABASE {target_db} TO ROLE {readonly_role};

-- Warehouse grant

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE {readonly_role};

"""

if "sql" not in st.session_state:
    st.session_state.sql = ""

if st.button("Refresh SQL"):
    st.session_state.sql = generate_sql(
        target_db, source_db, owner_role, readonly_role
    )

# -----------------------
# OUTPUT SQL
# -----------------------

st.subheader("Generated SQL")

st.text_area(
    "SQL Preview",
    value=st.session_state.sql,
    height=300
)

# -----------------------
# EXECUTE BUTTON
# -----------------------

if st.button("🚀 Execute SQL"):
    if not st.session_state.sql:
        st.error("Generate SQL first")
    else:
        try:
            # Split by semicolon and filter out empty strings to execute line-by-line
            statements = [stmt.strip() for stmt in st.session_state.sql.split(";") if stmt.strip()]
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            for i, stmt in enumerate(statements):
                status_text.text(f"Executing: {stmt.split()[0]}...") # e.g., "Executing: CREATE..."
                session.sql(stmt).collect()
                progress_bar.progress((i + 1) / len(statements))
            
            status_text.empty()
            progress_bar.empty()
            st.success(f"🎉 Success! **{target_db}** has been successfully cloned from **{source_db}**.")
            
        except Exception as e:
            st.error(f"❌ Execution failed: {str(e)}")