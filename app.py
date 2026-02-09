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
# Get Snowflake Session
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Get Logged-in User (CORRECT for Streamlit in Snowflake)
# -------------------------------------------------
user_ctx = st.user
current_user = user_ctx.user_name   # ✅ DO NOT use CURRENT_USER()
current_role = session.sql(
    "SELECT CURRENT_ROLE()"
).collect()[0][0]

# # -------------------------------------------------
# # Debug Context (can be removed later)
# # -------------------------------------------------
# st.sidebar.markdown("### 🔍 Debug Context")
# st.sidebar.write("CURRENT_USER:", current_user)
# st.sidebar.write("CURRENT_ROLE:", current_role)

# -------------------------------------------------
# Fetch App Role from Authorization Table
# -------------------------------------------------
def get_app_role(user_name):
    df = session.sql("""
        SELECT APP_ROLE
        FROM AI_POC_DB.HEALTH_POLICY_POC.APP_USER_ACCESS
        WHERE (
            UPPER(USER_NAME) = UPPER(:1)
            OR UPPER(USER_NAME) = SPLIT(UPPER(:1), '@')[0]
        )
        AND IS_ACTIVE = TRUE
    """, [user_name]).to_pandas()

    if df.empty:
        return None

    return df.iloc[0]["APP_ROLE"]

app_role = get_app_role(current_user)

# -------------------------------------------------
# Authorization Gate
# -------------------------------------------------
if not app_role:
    st.error("❌ You are not authorized to access this application.")

    st.markdown("### 🔍 Authorization Debug")
    debug_df = session.sql("""
        SELECT USER_NAME, APP_ROLE, IS_ACTIVE, CREATED_TS
        FROM AI_POC_DB.HEALTH_POLICY_POC.APP_USER_ACCESS
    """).to_pandas()
    st.dataframe(debug_df)

    st.stop()

# -------------------------------------------------
# Session State
# -------------------------------------------------
st.session_state.authenticated = True
st.session_state.username = current_user
st.session_state.role = app_role

# -------------------------------------------------
# Sidebar – User Info
# -------------------------------------------------
st.sidebar.success("Authenticated via Snowflake")
st.sidebar.write("👤 User:", current_user)
st.sidebar.write("🛡️ App Role:", app_role.upper())

# -------------------------------------------------
# Header
# -------------------------------------------------
st.title("📄 Policy & Control Search")
st.caption("Semantic policy search using Snowflake Cortex embeddings")

# -------------------------------------------------
# Sidebar – Search Filters
# -------------------------------------------------
st.sidebar.header("🔎 Search Filters")

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

search_text = st.sidebar.text_input(
    "Search Query",
    placeholder="e.g.termination clause"
)

lob = st.sidebar.selectbox("LOB", filters["LOB"])
state = st.sidebar.selectbox("State", filters["STATE"])
version = st.sidebar.selectbox("Version", filters["VERSION"])

top_k = st.sidebar.slider("Top Results", 1, 20, 10)
search_btn = st.sidebar.button("🔍 Search")

# -------------------------------------------------
# Execute Search
# -------------------------------------------------
if search_btn:

    if not search_text.strip():
        st.warning("Please enter a search query.")
    else:
        st.subheader("📌 Search Results")

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

            if results_df.empty:
                st.warning("No matching clauses found.")
            else:
                results_df.columns = (
                    results_df.columns.str.replace('"', '')
                    .str.strip()
                    .str.upper()
                )

                results_df = results_df.sort_values("SCORE", ascending=False)

                for _, row in results_df.iterrows():
                    with st.container():
                        st.markdown(f"### 📄 {row['CITATION']}")
                        st.markdown("**Excerpt:**")
                        st.markdown(row["EXCERPT"])
                        st.divider()

            # -------------------------------------------------
            # Audit Logging
            # -------------------------------------------------
            audit_df = session.create_dataframe(
                [[
                    search_text,
                    lob,
                    state,
                    version,
                    search_sql,
                    json.loads(results_df.to_json(orient="records")),
                    len(results_df),
                    current_user
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
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
