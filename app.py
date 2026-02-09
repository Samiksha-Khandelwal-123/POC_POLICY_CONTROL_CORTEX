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

session = get_active_session()

# -------------------------------------------------
# Session State Initialization
# -------------------------------------------------
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False
    st.session_state.username = None
    st.session_state.app_role = None

# -------------------------------------------------
# LOGIN FUNCTION
# -------------------------------------------------
def authenticate_user(username, password):
    df = session.sql("""
        SELECT USER_NAME, APP_ROLE
        FROM AI_POC_DB.HEALTH_POLICY_POC.APP_USER_ACCESS
        WHERE UPPER(USER_NAME) = UPPER(:1)
          AND PASSWORD = :2
          AND IS_ACTIVE = TRUE
    """, [username, password]).to_pandas()

    if df.empty:
        return None, None

    return df.iloc[0]["USER_NAME"], df.iloc[0]["APP_ROLE"]

# -------------------------------------------------
# LOGIN PAGE
# -------------------------------------------------
if not st.session_state.authenticated:

    st.title("🔐 Application Login")
    st.caption("Policy & Control Search Portal")

    with st.form("login_form"):
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        login_btn = st.form_submit_button("Login")

    if login_btn:
        user, role = authenticate_user(username, password)

        if not user:
            st.error("❌ Invalid username or password")
        else:
            st.session_state.authenticated = True
            st.session_state.username = user
            st.session_state.app_role = role
            st.experimental_rerun()

    st.stop()

# -------------------------------------------------
# AUTHENTICATED USER CONTEXT
# -------------------------------------------------
current_user = st.session_state.username
app_role = st.session_state.app_role
current_role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

# -------------------------------------------------
# SIDEBAR – USER INFO
# -------------------------------------------------
st.sidebar.success("Authenticated")
st.sidebar.write("👤 User:", current_user)
st.sidebar.write("🛡️ App Role:", app_role.upper())
st.sidebar.write("❄️ Snowflake Role:", current_role)

if st.sidebar.button("🚪 Logout"):
    for k in st.session_state.keys():
        del st.session_state[k]
    st.experimental_rerun()

# -------------------------------------------------
# HEADER
# -------------------------------------------------
st.title("📄 Policy & Control Search")
st.caption("Semantic policy search using Snowflake Cortex embeddings")

# -------------------------------------------------
# LOAD FILTER VALUES
# -------------------------------------------------
@st.cache_data
def load_filter_values():
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

filters = load_filter_values()

# -------------------------------------------------
# SIDEBAR – SEARCH FILTERS
# -------------------------------------------------
st.sidebar.header("🔎 Search Filters")

search_text = st.sidebar.text_input("Search Query")
lob = st.sidebar.selectbox("LOB", filters["LOB"])
state = st.sidebar.selectbox("State", filters["STATE"])
version = st.sidebar.selectbox("Version", filters["VERSION"])
top_k = st.sidebar.slider("Top Results", 1, 20, 10)
search_btn = st.sidebar.button("🔍 Search")

# -------------------------------------------------
# EXECUTE SEARCH
# -------------------------------------------------
if search_btn:

    if not search_text.strip():
        st.warning("Please enter a search query.")
    else:
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
                results_df.columns = results_df.columns.str.upper()

                for _, row in results_df.iterrows():
                    st.markdown(f"### 📄 {row['CITATION']}")
                    st.markdown(row["EXCERPT"])
                    st.divider()

            # -------------------------------------------------
            # AUDIT LOGGING (FIXED COLUMN COUNT)
            # -------------------------------------------------
            audit_df = session.create_dataframe(
                [[
                    search_text,
                    lob,
                    state,
                    version,
                    search_sql,
                    json.dumps(results_df.to_dict("records")),
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
# FOOTER
# -------------------------------------------------
st.divider()
st.caption("Powered by Snowflake Cortex • Streamlit in Snowflake")
