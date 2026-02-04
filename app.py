import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import json

st.success("✅ Streamlit app loaded successfully")
st.write("If you can see this, Streamlit is running.")

# ------------------------------------
# Page Configuration
# ------------------------------------
st.set_page_config(
    page_title="Policy & Control Search",
    layout="wide"
)

# ------------------------------------
# Get Snowflake Session (AUTO AUTH)
# ------------------------------------
session = get_active_session()

# ------------------------------------
# Header
# ------------------------------------
st.title("📄 Policy & Control Search")
st.caption("Enterprise policy search using Snowflake Cortex Search")

# ------------------------------------
# Sidebar - Search Inputs
# ------------------------------------
# ------------------------------------
# Sidebar - Search Filters (Dynamic)
# ------------------------------------
st.sidebar.header("🔎 Search Filters")

# ------------------------------------
# Allowed business values (CONTROLLED)
# ------------------------------------
ALLOWED_LOB = ["Retail", "Corporate", "SME"]
ALLOWED_STATE = ["CA", "NY", "TX"]
ALLOWED_VERSION = ["v1", "v2"]

# ------------------------------------
# Load distinct filter values from table
# ------------------------------------
@st.cache_data
def load_filter_values():
    df = session.sql("""
        SELECT DISTINCT
            LOB,
            STATE,
            VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_CHUNKS
    """).to_pandas()

    return {
        "LOB": sorted(df["LOB"].dropna().unique().tolist()),
        "STATE": sorted(df["STATE"].dropna().unique().tolist()),
        "VERSION": sorted(df["VERSION"].dropna().unique().tolist())
    }

filter_values = load_filter_values()

# ------------------------------------
# Intersect DB values with allowed values
# ------------------------------------
lob_options = ["All"] + [v for v in ALLOWED_LOB if v in filter_values["LOB"]]
state_options = ["All"] + [v for v in ALLOWED_STATE if v in filter_values["STATE"]]
version_options = ["All"] + [v for v in ALLOWED_VERSION if v in filter_values["VERSION"]]

# ------------------------------------
# Search Inputs
# ------------------------------------
search_text = st.sidebar.text_input(
    "Search Query",
    placeholder="e.g. What is the termination clause?"
)

lob = st.sidebar.selectbox(
    "LOB",
    lob_options
)

state = st.sidebar.selectbox(
    "State",
    state_options
)

version = st.sidebar.selectbox(
    "Version",
    version_options
)

top_k = st.sidebar.slider(
    "Top Results",
    min_value=1,
    max_value=10,
    value=5
)

# ------------------------------------
# RBAC (Read-only display)
# ------------------------------------
st.sidebar.markdown("---")

current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]
current_role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

st.sidebar.write("👤 User:", current_user)
st.sidebar.write("🎭 Role:", current_role)

# ------------------------------------
# Search Button
# ------------------------------------
search_btn = st.sidebar.button("🔍 Search")
# ------------------------------------
# Helper: Build Cortex Filter Object
# ------------------------------------
def build_filter_sql():
    filters = []
    if lob != "All":
        filters.append(f"'LOB','{lob}'")
    if state != "All":
        filters.append(f"'STATE','{state}'")
    if version != "All":
        filters.append(f"'VERSION','{version}'")

    return ",".join(filters)

# ------------------------------------
# Execute Search
# ------------------------------------
if search_btn and search_text:

    st.subheader("📌 Search Results")

    filter_sql = build_filter_sql()

    # --------------------------------
    # Cortex Search SQL
    # --------------------------------
    cortex_sql = f"""
    SELECT
        DOC_NAME,
        SECTION_TITLE,
        CHUNK_TEXT,
        SCORE
    FROM TABLE(
        CORTEX.SEARCH(
            'POLICY_SEARCH_SVC',
            '{search_text}',
            OBJECT_CONSTRUCT_KEEP_NULL({filter_sql}),
            {top_k}
        )
    )
    """

    try:
        # Execute query
        results_df = session.sql(cortex_sql).to_pandas()

        # Convert output to JSON (VARIANT)
        query_output_json = json.loads(
            results_df.to_json(orient="records")
        )

        # Display results
        if results_df.empty:
            st.warning("No results found. Please adjust filters.")
        else:
            for _, row in results_df.iterrows():
                st.markdown(f"""
                ### {row['SECTION_TITLE']}
                **📄 Document:** {row['DOC_NAME']}  
                **🎯 Score:** {round(row['SCORE'], 3)}

                {row['CHUNK_TEXT']}
                ---
                """)

        # --------------------------------
        # Audit Logging (MATCHES YOUR TABLE)
        # --------------------------------
        session.create_dataframe(
            [[
                search_text,
                None if lob == "All" else lob,
                None if state == "All" else state,
                None if version == "All" else version,
                cortex_sql,
                query_output_json,
                len(query_output_json),
                None,
                None,
                datetime.now()
            ]],
            schema=[
                "SEARCH_TEXT",
                "LOB",
                "STATE",
                "VERSION",
                "QUERY_TEXT",
                "QUERY_OUTPUT",
                "RESULT_COUNT",
                "USER_NAME",
                "ROLE_NAME",
                "SEARCH_TS"
            ]
        ).write.save_as_table(
            "POLICY_SEARCH_AUDIT",
            mode="append"
        )

        # Update USER + ROLE from Snowflake context
        session.sql("""
            UPDATE POLICY_SEARCH_AUDIT
            SET
                USER_NAME = CURRENT_USER(),
                ROLE_NAME = CURRENT_ROLE()
            WHERE USER_NAME IS NULL
        """).collect()

    except Exception as e:
        st.error("❌ Error while executing search")
        st.code(str(e))

# ------------------------------------
# Footer
# ------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex Search • Streamlit in Snowflake")
