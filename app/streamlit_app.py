import streamlit as st
import snowflake.connector
import pandas as pd
import re

# ---------------------------
# CONFIG
# ---------------------------
st.set_page_config(page_title="AI Data Analyst", layout="wide")

# ---------------------------
# CONNECTION
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
# QUERY EXECUTION
# ---------------------------
def run_query(query):
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
        return df
    finally:
        cursor.close()

# ---------------------------
# CLEAN SQL
# ---------------------------
def clean_sql(text):
    text = re.sub(r"```sql|```", "", text, flags=re.IGNORECASE)
    text = text.replace("\\", "")
    text = text.replace(";", "")
    return text.strip()

# ---------------------------
# GENERATE SQL
# ---------------------------
def generate_sql(question):
    prompt = f"""
You are an expert Snowflake SQL generator.

STRICT RULES:
- Only return SQL query
- No explanation
- Use only given tables
- Use correct column names
- Always use FULLY QUALIFIED TABLE NAMES

DATABASE: AI_ANALYTICS_DB
SCHEMA: GOLD

TABLES:

FACT_SALES(order_id, product_id, customer_id, order_date, price, freight_value)

DIM_PRODUCTS(product_id, product_category_name)

CUSTOMER_REVENUE(customer_id, total_spent, total_orders)

MONTHLY_SALES(year, month, revenue)

FACT_REVIEWS(order_id, review_score, review_creation_date, review_comment_message)

RELATIONSHIPS:
FACT_SALES.product_id = DIM_PRODUCTS.product_id

RULES:
- For category → join DIM_PRODUCTS
- For random row → ORDER BY RANDOM() LIMIT 1
- For top → ORDER BY DESC LIMIT 5

QUESTION:
{question}
"""

    cortex_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        $$ {prompt} $$
    );
    """

    df = run_query(cortex_query)
    return clean_sql(df.iloc[0, 0])

# ---------------------------
# FIX SQL IF ERROR
# ---------------------------
def fix_sql(question, bad_sql, error):
    prompt = f"""
Fix this SQL query.

Error:
{error}

Bad SQL:
{bad_sql}

Use correct table names and columns.

Return only fixed SQL.
"""

    query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        $$ {prompt} $$
    );
    """

    df = run_query(query)
    return clean_sql(df.iloc[0, 0])

# ---------------------------
# SAFE EXECUTION (RETRY LOGIC)
# ---------------------------
def execute_with_retry(question):
    sql = generate_sql(question)

    try:
        df = run_query(sql)
        return sql, df
    except Exception as e:
        try:
            fixed_sql = fix_sql(question, sql, str(e))
            df = run_query(fixed_sql)
            return fixed_sql, df
        except Exception as e2:
            raise Exception(f"Failed after retry:\n{e2}")
    
    if not sql.lower().startswith("select"):
        raise Exception("Invalid SQL generated")

# ---------------------------
# SUMMARY
# ---------------------------
def generate_summary(question, df):
    if df.empty:
        return "No data found."

    data_sample = df.head(5).to_string(index=False)

    prompt = f"""
Answer the question in one simple sentence.

Question: {question}
Data:
{data_sample}

Rules:
- One sentence only
- No extra text
- No formatting suggestions
"""

    query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        $$ {prompt} $$
    );
    """

    df2 = run_query(query)
    return df2.iloc[0, 0].strip()

# ---------------------------
# UI
# ---------------------------
st.title("AI Data Analyst")

if "history" not in st.session_state:
    st.session_state.history = []

question = st.text_input("Ask your question")

if question:
    try:
        with st.spinner("Thinking..."):

            sql, df = execute_with_retry(question)

            tab1, tab2, tab3 = st.tabs(["SQL", "Results", "Insight"])

            with tab1:
                st.code(sql)

            with tab2:
                st.dataframe(df)

                if df.shape == (1, 1):
                    st.metric("Result", df.iloc[0, 0])

                if len(df.columns) >= 2:
                    st.bar_chart(df.set_index(df.columns[0]))

            with tab3:
                st.success(generate_summary(question, df))

            st.session_state.history.append((question, sql))

    except Exception as e:
        st.error(str(e))

# ---------------------------
# SIDEBAR
# ---------------------------
st.sidebar.title("History")

for q, s in reversed(st.session_state.history):
    st.sidebar.write(q)
    st.sidebar.code(s)