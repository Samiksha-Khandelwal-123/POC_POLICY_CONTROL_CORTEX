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
# Session State Initialization
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.role = None
    st.session_state.username = None

# -------------------------------------------------
# Sidebar Login (RBAC)
# -------------------------------------------------
st.sidebar.title("🔐 Login")

if not st.session_state.authenticated:

    username = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")
    role = st.sidebar.selectbox("Login As", ["admin", "user"])

    login_btn = st.sidebar.button("Login")

    if login_btn:
        # 🔹 DEMO AUTH (replace with DB / LDAP if needed)
        if username and password:
            st.session_state.authenticated = True
            st.session_state.role = role
            st.session_state.username = username
            #st.experimental_rerun()
        else:
            st.sidebar.error("Invalid credentials")

    # Stop app execution until login.
    st.stop()

# -------------------------------------------------
# Logged-in Sidebar Info
# -------------------------------------------------
st.sidebar.success(f"Logged in as {st.session_state.role.upper()}")
st.sidebar.write("👤 User:", st.session_state.username)

logout_btn = st.sidebar.button("Logout")
if logout_btn:
    st.session_state.clear()
    #st.experimental_rerun()

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
                    results_df.columns.str.replace('"', '').str.strip().str.upper()
                )

                results_df = results_df.sort_values("SCORE", ascending=False)

                # -------------------------------------------------
                # RBAC Result Rendering
                # -------------------------------------------------
                for _, row in results_df.iterrows():
                    with st.container():

                        # ADMIN → Citation + Excerpt
                        if st.session_state.role == "admin":
                            st.markdown(f"### 📄 {row['CITATION']}")
                            st.markdown("**Excerpt:**")
                            st.markdown(row["EXCERPT"])

                        # USER → Excerpt only
                        else:
                            st.markdown("### 📄 Policy Match")
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
