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
# SAFE Session State Initialization (VERY IMPORTANT)
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False

if "username" not in st.session_state:
    st.session_state["username"] = None

if "app_role" not in st.session_state:
    st.session_state["app_role"] = None

# -------------------------------------------------
# Get Snowflake Session
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Helper: Fetch App Role
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

# -------------------------------------------------
# LOGIN SCREEN (Shown first)
# -------------------------------------------------
if not st.session_state["authenticated"]:

    st.title("🔐 Policy Search Login")
    st.caption("Authenticate to access Policy & Control Search")

    with st.form("login_form"):
        login_user = st.text_input(
            "Username",
            placeholder="e.g. username or username@company.com"
        )
        login_btn = st.form_submit_button("Login")

    if login_btn:

        if not login_user.strip():
            st.warning("Please enter your username.")
            st.stop()

        role = get_app_role(login_user)

        if not role:
            st.error("❌ You are not authorized to access this application.")
            st.stop()

        # ✅ Successful login
        st.session_state["authenticated"] = True
        st.session_state["username"] = login_user
        st.session_state["app_role"] = role

        #st.experimental_rerun()

    st.stop()

# -------------------------------------------------
# USER CONTEXT (After Login)
# -------------------------------------------------
current_user = st.session_state["username"]
app_role = st.session_state["app_role"]

current_role = session.sql(
    "SELECT CURRENT_ROLE()"
).collect()[0][0]

# -------------------------------------------------
# Sidebar – User Info
# -------------------------------------------------
st.sidebar.success("Authenticated")
st.sidebar.write("👤 User:", current_user)

if app_role:
    st.sidebar.write("🛡️ App Role:", app_role.upper())
else:
    st.sidebar.write("🛡️ App Role: UNKNOWN")

if st.sidebar.button("🚪 Logout"):
    st.session_state.clear()
    #st.experimental_rerun()
# -------------------------------------------------
# Header
# -------------------------------------------------
st.title("📄 Policy & Control Search")
st.caption("Semantic policy search using Snowflake Cortex")

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
    placeholder="e.g. termination clause"
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
        st.stop()

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
                    st.markdown("**Details:**")
                    st.markdown(row["EXCERPT"])
                    st.divider()

        # -------------------------------------------------
        # Audit Logging (FIXED column count)
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
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
