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
    cortex_query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', $$ {prompt} $$);"
    df = run_query(cortex_query)
    return clean_sql(df.iloc[0, 0])

# ---------------------------
# FETCH MEANING (AI)
# ---------------------------
def get_meaning(text):
    prompt = f"""
    Explain the meaning of this review in one short English sentence: "{text}"
    Return ONLY the explanation, no headers or extra text.
    """
    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', $$ {prompt} $$);"
    df_m = run_query(query)
    return df_m.iloc[0, 0].strip()

# ---------------------------
# EXECUTION LOGIC
# ---------------------------
def execute_with_retry(question):
    sql = generate_sql(question)
    try:
        df = run_query(sql)
        return sql, df
    except Exception as e:
        # Simple fix attempt
        prompt = f"Fix this SQL. Error: {e}. Bad SQL: {sql}."
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', $$ {prompt} $$);"
        fixed_sql = clean_sql(run_query(query).iloc[0, 0])
        df = run_query(fixed_sql)
        return fixed_sql, df

# ---------------------------
# SUMMARY
# ---------------------------
def generate_summary(question, df):
    if df.empty: return "No data found."
    data_sample = df.head(5).to_string(index=False)
    prompt = f"Summarize this data for the question '{question}': {data_sample}"
    query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', $$ {prompt} $$);"
    return run_query(query).iloc[0, 0].strip()

# ---------------------------
# UI
# ---------------------------
st.title("AI Data Analyst")

if "history" not in st.session_state:
    st.session_state.history = []

question = st.text_input("Ask your question (e.g., 'Show me 5 random 1 star reviews')")

if question:
    try:
        with st.spinner("Analyzing Data..."):
            fast_sql = fast_path_reviews(question)
            if fast_sql:
                sql = fast_sql
                df = run_query(sql)
            else:
                sql, df = execute_with_retry(question)

            tab1, tab2, tab3 = st.tabs(["SQL", "Results", "Insight"])

            with tab1:
                st.code(sql, language="sql")

            with tab2:
                st.subheader("Raw Data Results")
                st.dataframe(df, use_container_width=True, hide_index=True)

                if "REVIEW_COMMENT_MESSAGE" in df.columns:
                    st.divider()
                    st.subheader("Advanced Review Analysis")

                    # Processing the top 5 valid reviews
                    review_subset = df.dropna(subset=["REVIEW_COMMENT_MESSAGE"]).head(5)
                    
                    analysis_rows = []
                    
                    # FOR LOOP FOR CUSTOM REQUIREMENTS
                    for _, row in review_subset.iterrows():
                        msg = row["REVIEW_COMMENT_MESSAGE"]
                        score = row.get("REVIEW_SCORE", 0)
                        
                        # 1. IF-ELSE BLOCK FOR SENTIMENT WORD
                        if score >= 4:
                            sentiment_word = "Positive"
                        elif score == 3:
                            sentiment_word = "Neutral"
                        else:
                            sentiment_word = "Negative"
                        
                        # 2. FETCH MEANING IN ENGLISH
                        meaning_english = get_meaning(msg)
                        
                        analysis_rows.append({
                            "Review Content": msg,
                            "Sentiment": sentiment_word,
                            "Meaning (English)": meaning_english
                        })

                    if analysis_rows:
                        sentiment_df = pd.DataFrame(analysis_rows)
                        st.dataframe(sentiment_df, use_container_width=True, hide_index=True)
                    else:
                        st.info("No text reviews found to analyze.")

            with tab3:
                st.success(generate_summary(question, df))

            st.session_state.history.append((question, sql))

    except Exception as e:
        st.error(f"Error executing request: {e}")

# ---------------------------
# SIDEBAR
# ---------------------------
st.sidebar.title("History")
for q, s in reversed(st.session_state.history):
    st.sidebar.markdown(f"**Q:** {q}")
    st.sidebar.code(s, language="sql")