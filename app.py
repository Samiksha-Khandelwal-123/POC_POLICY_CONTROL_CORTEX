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
# SAFE Session State Initialization
# -------------------------------------------------
for key in ["authenticated", "username", "app_role"]:
    if key not in st.session_state:
        st.session_state[key] = None if key != "authenticated" else False

# -------------------------------------------------
# Snowflake Session
# -------------------------------------------------
session = get_active_session()

# -------------------------------------------------
# Authentication Function
# -------------------------------------------------
def authenticate_user(user_name, password):
    df = session.sql("""
        SELECT USER_NAME, APP_ROLE
        FROM AI_POC_DB.HEALTH_POLICY_POC.APP_USER_ACCESS
        WHERE (
            UPPER(USER_NAME) = UPPER(:1)
            OR UPPER(USER_NAME) = SPLIT(UPPER(:1), '@')[0]
        )
        AND PASSWORD = :2
        AND IS_ACTIVE = TRUE
    """, [user_name, password]).to_pandas()

    if df.empty:
        return None, None

    return df.iloc[0]["USER_NAME"], df.iloc[0]["APP_ROLE"]

# -------------------------------------------------
# LOGIN SCREEN (FIRST PAGE)
# -------------------------------------------------
if not st.session_state.authenticated:

    st.title("🔐 Login – Policy & Control Search")
    st.caption("Enter your application credentials")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_btn = st.form_submit_button("Login")

    if login_btn:
        if not username or not password:
            st.warning("Please enter username and password.")
            st.stop()

        user, role = authenticate_user(username, password)

        if not role:
            st.error("❌ Invalid username or password.")
            st.stop()

        # ✅ Login success
        st.session_state.authenticated = True
        st.session_state.username = user
        st.session_state.app_role = role

        #st.experimental_rerun()

    st.stop()

# -------------------------------------------------
# USER CONTEXT
# -------------------------------------------------
current_user = st.session_state.username
app_role = st.session_state.app_role

current_role = session.sql(
    "SELECT CURRENT_ROLE()"
).collect()[0][0]

# -------------------------------------------------
# Sidebar – User Info
# -------------------------------------------------
st.sidebar.success("Authenticated")
st.sidebar.write("👤 User:", current_user)
st.sidebar.write("🛡️ App Role:", app_role.upper())

if st.sidebar.button("🚪 Logout"):
    st.session_state.clear()
    #st.experimental_rerun()

# -------------------------------------------------
# Header
# -------------------------------------------------
st.title("📄 Policy & Control Search")
st.caption("Semantic search using Snowflake Cortex")

# -------------------------------------------------
# Sidebar – Filters
# -------------------------------------------------
st.sidebar.header("🔎 Search Filters")

def load_filters():
    df = session.sql("""
        SELECT DISTINCT LOB, STATE, VERSION
        FROM AI_POC_DB.HEALTH_POLICY_POC.DOCUMENT_CHUNKS
        ORDER BY 1,2,3
    """).to_pandas()

    return {
        "LOB": sorted(df["LOB"].dropna().unique()),
        "STATE": sorted(df["STATE"].dropna().unique()),
        "VERSION": sorted(df["VERSION"].dropna().unique())
    }

filters = load_filters()

search_text = st.sidebar.text_input("Search Query")
lob = st.sidebar.selectbox("LOB", filters["LOB"])
state = st.sidebar.selectbox("State", filters["STATE"])
version = st.sidebar.selectbox("Version", filters["VERSION"])

search_btn = st.sidebar.button("🔍 Search")

# -------------------------------------------------
# Search Execution
# -------------------------------------------------
if search_btn:

    if not search_text:
        st.warning("Enter a search query.")
        st.stop()

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
        # Audit Log
        # -------------------------------------------------
        query_output = []

        if results_df is not None and not results_df.empty:
            #query_output = json.loads(results_df.to_json(orient="records"))
            query_output = results_df
            
        audit_df = session.create_dataframe(
            [[
                search_text,
                lob,
                state,
                version,
                query_output,
                len(results_df),
                current_user,
                app_role,
                current_role,
                datetime.now()
            ]],
            schema=[
                "SEARCH_TEXT",
                "LOB",
                "STATE",
                "VERSION",
                "QUERY_OUTPUT",
                "RESULT_COUNT",
                "USER_NAME",
                "APP_ROLE",
                "SNOWFLAKE_ROLE",
                "SEARCH_TS"
            ]
        )

        audit_df.write.save_as_table(
            "AI_POC_DB.HEALTH_POLICY_POC.POLICY_SEARCH_AUDIT",
            mode="append"
        )

    except Exception as e:
        st.error("❌ Search failed")
        st.code(str(e))

# -------------------------------------------------
# Footer
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
