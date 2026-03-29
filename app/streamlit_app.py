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
    text = text.replace("`", "").replace("\\", "").strip()
    select_index = text.lower().find("select")
    if select_index == -1:
        raise Exception("No valid SQL found from AI")
    sql = text[select_index:]
    sql = re.split(r";|\n\n", sql)[0]
    return sql.strip()

# ---------------------------
# FAST PATH FOR REVIEWS
# ---------------------------
def fast_path_reviews(question):
    q = question.lower()
    if "random" in q and "review" in q:
        for i in range(1, 6):
            if str(i) in q:
                return f"""
                SELECT review_comment_message, review_score
                FROM AI_ANALYTICS_DB.GOLD.FACT_REVIEWS
                WHERE review_score = {i}
                AND review_comment_message IS NOT NULL
                ORDER BY RANDOM()
                LIMIT 5
                """

    if "how many" in q and "review" in q:
        for i in range(1, 6):
            if str(i) in q:
                return f"""
                SELECT COUNT(*) AS total_reviews
                FROM AI_ANALYTICS_DB.GOLD.FACT_REVIEWS
                WHERE review_score = {i}
                """

    if "average" in q and "review" in q:
        return """
        SELECT AVG(review_score) AS avg_rating
        FROM AI_ANALYTICS_DB.GOLD.FACT_REVIEWS
        """
    return None

# ---------------------------
# GENERATE SQL
# ---------------------------
def generate_sql(question):
    prompt = f"""
    You are an expert Snowflake SQL generator.
    STRICT RULES:
    - ONLY return SQL starting EXACTLY with SELECT
    - If the question is about reviews, ALWAYS include 'review_score' and 'review_comment_message' in the SELECT.
    - Use only given tables.
    
    DATABASE: AI_ANALYTICS_DB
    SCHEMA: GOLD
    TABLES:
    FACT_SALES(order_id, product_id, customer_id, order_date, price, freight_value)
    DIM_PRODUCTS(product_id, product_category_name)
    CUSTOMER_REVENUE(customer_id, total_spent, total_orders)
    MONTHLY_SALES(year, month, revenue)
    FACT_REVIEWS(order_id, review_score, review_creation_date, review_comment_message)
    
    QUESTION:
    {question}
    """
    cortex_query = f"""
    SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', $$ {prompt} $$);
    """
    df = run_query(cortex_query)
    return clean_sql(df.iloc[0, 0])

# ---------------------------
# FIX SQL & EXECUTION
# ---------------------------
def fix_sql(question, bad_sql, error):
    prompt = f"Fix this SQL. Error: {error}. Bad SQL: {bad_sql}. Return only SQL."
    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', $$ {prompt} $$);"
    df = run_query(query)
    return clean_sql(df.iloc[0, 0])

def execute_with_retry(question):
    sql = generate_sql(question)
    try:
        df = run_query(sql)
        return sql, df
    except Exception as e:
        fixed_sql = fix_sql(question, sql, str(e))
        df = run_query(fixed_sql)
        return fixed_sql, df

# ---------------------------
# SUMMARY
# ---------------------------
def generate_summary(question, df):
    if df.empty: return "No data found."
    data_sample = df.head(5).to_string(index=False)
    prompt = f"Answer in one simple sentence. Question: {question} Data: {data_sample}"
    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', $$ {prompt} $$);"
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
            fast_sql = fast_path_reviews(question)
            if fast_sql:
                sql = fast_sql
                df = run_query(sql)
            else:
                sql, df = execute_with_retry(question)

            tab1, tab2, tab3 = st.tabs(["SQL", "Results", "Insight"])

            with tab1:
                st.code(sql)

            with tab2:
                st.subheader("Data Results")
                st.dataframe(df, use_container_width=True, hide_index=True)

                if "REVIEW_COMMENT_MESSAGE" in df.columns:
                    st.divider()
                    st.subheader("Advanced Review Analysis")

                    # Get data and drop empty messages
                    review_data = df.dropna(subset=["REVIEW_COMMENT_MESSAGE"]).head(5)
                    
                    analysis_rows = []
                    for _, row in review_data.iterrows():
                        msg = row["REVIEW_COMMENT_MESSAGE"]
                        
                        # --- SENTIMENT LOGIC BLOCK ---
                        score = row.get("REVIEW_SCORE", 0)
                        if score >= 4:
                            sentiment_word = "Positive"
                        elif score == 3:
                            sentiment_word = "Neutral"
                        else:
                            sentiment_word = "Negative"
                        # -----------------------------

                        analysis_rows.append({
                            "Review Content": msg,
                            "Star Rating": score,
                            "Sentiment": sentiment_word
                        })

                    if analysis_rows:
                        sentiment_df = pd.DataFrame(analysis_rows)
                        st.dataframe(sentiment_df, use_container_width=True, hide_index=True)
                    else:
                        st.write("No review messages found to analyze.")

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
    st.sidebar.write(f"**Q:** {q}")
    st.sidebar.code(s)