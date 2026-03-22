import streamlit as st
import snowflake.connector
import pandas as pd
import re

# ---------------------------
# PAGE CONFIG
# ---------------------------
st.set_page_config(page_title="AI Data Analyst", layout="wide")

# ---------------------------
# CONNECT TO SNOWFLAKE
# ---------------------------
@st.cache_resource

def init_connection():
    return snowflake.connector.connect(
        user=st.secrets["SNOWFLAKE_USER"],
        password=st.secrets["SNOWFLAKE_PASSWORD"],
        account=st.secrets["SNOWFLAKE_ACCOUNT"],
        warehouse=st.secrets["SNOWFLAKE_WAREHOUSE"],
        database=st.secrets["SNOWFLAKE_DATABASE"],
        schema=st.secrets["SNOWFLAKE_SCHEMA"]
    )

conn = init_connection()

# ---------------------------
# RUN QUERY FUNCTION
# ---------------------------
def run_query(query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        cursor.close()
        return df
    except Exception as e:
        raise Exception(f"Query failed: {e}")

# ---------------------------
# CLEAN SQL OUTPUT
# ---------------------------
def clean_sql(text):
    text = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE)
    text = text.replace("\\", "")
    text = text.replace(";", "")
    return text.strip()

# ---------------------------
# GENERATE SQL USING CORTEX
# ---------------------------
def generate_sql(question):
    prompt = f"""
You are a strict SQL generator.

Rules:
- ONLY return SQL query
- NO explanation
- ALWAYS use FULLY QUALIFIED TABLE NAMES

DATABASE: AI_ANALYTICS_DB
SCHEMA: GOLD

TABLES:

AI_ANALYTICS_DB.GOLD.FACT_SALES(order_id, product_id, customer_id, order_date, price, freight_value)

AI_ANALYTICS_DB.GOLD.MONTHLY_SALES(year, month, revenue)

AI_ANALYTICS_DB.GOLD.CUSTOMER_REVENUE(customer_id, total_spent, total_orders)

AI_ANALYTICS_DB.GOLD.FACT_REVIEWS(order_id, review_score, review_creation_date, review_comment_message)

IMPORTANT:
- DO NOT invent columns
- DO NOT change column names
- ALWAYS prefix tables
- If random review → ORDER BY RANDOM() LIMIT 1
- review_score is between 1 and 5

QUESTION:
{question}
"""

    cortex_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        $$ {prompt} $$
    );
    """

    result = run_query(cortex_query)
    raw_sql = result.iloc[0, 0]

    return clean_sql(raw_sql)

# ---------------------------
# GENERATE SUMMARY
# ---------------------------
def generate_summary(question, df):
    if df.empty:
        return "No data available."

    data_sample = df.head(5).to_string(index=False)

    prompt = f"""
Answer the question using the data provided.

Question: {question}
Data:
{data_sample}

Rules:
- Return ONLY one sentence
- No brackets
- No extra comments
- No formatting suggestions
- No commas inside numbers
- No extra text

Example:
The total number of orders is 112650
"""

    cortex_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        $$ {prompt} $$
    );
    """

    result = run_query(cortex_query)
    return result.iloc[0, 0].strip()

# ---------------------------
# UI
# ---------------------------
st.title("🤖 AI Data Analyst")
st.markdown("Ask questions about your Snowflake data")

# Chat history
if "history" not in st.session_state:
    st.session_state.history = []

user_input = st.text_input("💬 Ask your question:")

if user_input:
    try:
        with st.spinner("🤖 Thinking..."):

            # Generate SQL
            sql_query = generate_sql(user_input)

            # Save history
            st.session_state.history.append(("User", user_input))
            st.session_state.history.append(("SQL", sql_query))

            # Run query
            df = run_query(sql_query)

            # ---------------------------
            # TABS UI
            # ---------------------------
            tab1, tab2, tab3 = st.tabs(["🧠 SQL", "📊 Results", "💡 Insight"])

            # SQL TAB
            with tab1:
                st.code(sql_query, language="sql")

            # RESULTS TAB
            with tab2:
                if df.empty:
                    st.warning("No data found.")
                else:
                    st.dataframe(df)

                    # Metric view
                    if df.shape == (1, 1):
                        st.metric("Result", df.iloc[0, 0])

                    # Chart
                    if len(df.columns) >= 2:
                        numeric_cols = df.select_dtypes(include='number').columns
                        if len(numeric_cols) > 0:
                            st.bar_chart(df.set_index(df.columns[0]))

            # INSIGHT TAB
            with tab3:
                summary = generate_summary(user_input, df)
                st.success(summary)

    except Exception as e:
        st.error("⚠️ Error executing query.")
        st.code(str(e))

# ---------------------------
# SIDEBAR HISTORY
# ---------------------------
st.sidebar.title("📝 Chat History")

for role, msg in st.session_state.history[::-1]:
    if role == "User":
        st.sidebar.markdown(f"**🧑 {msg}**")
    else:
        st.sidebar.code(msg)