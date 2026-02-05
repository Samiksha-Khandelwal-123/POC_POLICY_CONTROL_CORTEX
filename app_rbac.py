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
# RBAC Users (POC Only)
# -------------------------------------------------
USERS = {
    "admin": {"password": "admin123", "role": "admin"},
    "user": {"password": "user123", "role": "user"}
}

# -------------------------------------------------
# Session defaults
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# -------------------------------------------------
# Login Dialog (POPUP)
# -------------------------------------------------
@st.dialog("🔐 Login", width="small")
def login_dialog():
    username = st.text_input("User ID")
    password = st.text_input("Password", type="password")

    if st.button("Login"):
        user = USERS.get(username)

        if user and user["password"] == password:
            st.session_state.authenticated = True
            st.session_state.username = username
            st.session_state.role = user["role"]
            st.success("Login successful")
            st.rerun()
        else:
            st.error("Invalid credentials")

# -------------------------------------------------
# Force login popup at app start
# -------------------------------------------------
if not st.session_state.authenticated:
    login_dialog()
    st.stop()

# -------------------------------------------------
# Snowflake Session
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Header
# -------------------------------------------------
st.title("📄 Policy & Control Search")
st.caption("Semantic policy search using Snowflake Cortex embeddings")

# -------------------------------------------------
# Sidebar – User Info & Logout
# -------------------------------------------------
with st.sidebar:
    st.write(f"👤 User: {st.session_state.username}")
    st.write(f"🔑 Role: {st.session_state.role}")

    if st.button("Logout"):
        st.session_state.clear()
        st.rerun()

# -------------------------------------------------
# Sidebar – Search Filters
# -------------------------------------------------
st.sidebar.header("🔎 Search Filters")

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

search_text = st.sidebar.text_input(
    "Search Query",
    placeholder="e.g. What is the termination clause?"
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
                    results_df.columns
                        .str.replace('"', '')
                        .str.strip()
                        .str.upper()
                )

                results_df = (
                    results_df
                        .sort_values("SCORE", ascending=False)
                        .head(top_k)
                )

                # -------------------------------------------------
                # RBAC Rendering
                # -------------------------------------------------
                for _, row in results_df.iterrows():
                    with st.container():

                        if st.session_state.role == "admin":
                            st.markdown(f"### 📄 {row['CITATION']}")
                            st.markdown("**Excerpt:**")
                            st.markdown(row["EXCERPT"])
                        else:
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
                        st.session_state.username,
                        st.session_state.role,
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
