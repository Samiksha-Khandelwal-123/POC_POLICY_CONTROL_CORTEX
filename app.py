import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import datetime
import json

# -------------------------------------------------
# Page Configuration
# -------------------------------------------------
st.set_page_config(
    page_title="Policy & Control Search",
    layout="wide"
)

# -------------------------------------------------
# Snowflake Session (Auto-auth in SiS)
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Header
# -------------------------------------------------
st.title("📄 Policy & Control Search")
st.caption("Semantic policy search using Snowflake Cortex embeddings")

# -------------------------------------------------
# Sidebar – Search Filters
# -------------------------------------------------
st.sidebar.header("🔎 Search Filters")

# -------------------------------------------------
# Load filter values dynamically from DOCUMENT_CHUNKS
# -------------------------------------------------
@st.cache_data
def load_filter_values():
    df = session.sql("""
        SELECT DISTINCT
            LOB,
            STATE,
            VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_CHUNKS
        ORDER BY 1,2,3
    """).to_pandas()

    return {
        "LOB": sorted(df["LOB"].dropna().unique().tolist()),
        "STATE": sorted(df["STATE"].dropna().unique().tolist()),
        "VERSION": sorted(df["VERSION"].dropna().unique().tolist())
    }

filters = load_filter_values()

# -------------------------------------------------
# Sidebar Inputs
# -------------------------------------------------
search_text = st.sidebar.text_input(
    "Search Query",
    placeholder="e.g. What is the termination clause?"
)

lob = st.sidebar.selectbox(
    "LOB",
    filters["LOB"]
)

state = st.sidebar.selectbox(
    "State",
    filters["STATE"]
)

version = st.sidebar.selectbox(
    "Version",
    filters["VERSION"]
)

top_k = st.sidebar.slider(
    "Top Results",
    min_value=1,
    max_value=20,
    value=10
)

search_btn = st.sidebar.button("🔍 Search")

# -------------------------------------------------
# RBAC Info (Optional – Read Only)
# -------------------------------------------------
st.sidebar.markdown("---")
current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]
current_role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

st.sidebar.write("👤 User:", current_user)
st.sidebar.write("🎭 Role:", current_role)

# -------------------------------------------------
# Execute Search
# -------------------------------------------------
if search_btn:

    if not search_text.strip():
        st.warning("Please enter a search query.")
    else:
        st.subheader("📌 Search Results")

        # ---------------------------------------------
        # Call Stored Procedure
        # ---------------------------------------------
        search_sql = f"""
            CALL AI_POC_DB.HEALTH_POLICY_POC.SEARCH_POLICY_CLAUSE(
                '{search_text}',
                '{state}',
                '{lob}',
                '{version}'
            )
        """

        try:
            results_df = session.sql(search_sql).to_pandas()

            # -----------------------------------------
            # No Results
            # -----------------------------------------
            if results_df.empty:
                st.warning("No matching clauses found. Try different filters.")
            else:
                for idx, row in results_df.iterrows():
                    st.markdown(f"""
                    ### 🔹 Result {idx + 1}
                    **🎯 Score:** {round(row["SCORE"], 3)}  
                    **📄 Citation:** {row["CITATION"]}

                    {row["EXCERPT"]}
                    ---
                    """)

            # -----------------------------------------
            # Audit Logging (Optional but recommended)
            # -----------------------------------------
            audit_df = session.create_dataframe(
                [[
                    search_text,
                    lob,
                    state,
                    version,
                    search_sql,
                    json.loads(results_df.to_json(orient="records")),
                    len(results_df),
                    current_user,
                    current_role,
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
            )

            audit_df.write.save_as_table(
                "AI_POC_DB.HEALTH_POLICY_POC.POLICY_SEARCH_AUDIT",
                mode="append"
            )

        except Exception as e:
            st.error("❌ Error while executing search")
            st.code(str(e))

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex Embeddings • Streamlit in Snowflake")
