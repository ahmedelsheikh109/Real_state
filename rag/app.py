import os
import sys
import json
import logging
import streamlit as st
import warnings
from dotenv import load_dotenv
from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_classic.chains import create_sql_query_chain
from langchain_core.prompts import PromptTemplate
from sqlalchemy import create_engine

# 1. Fix Windows console encoding for Arabic/Emojis
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

warnings.filterwarnings("ignore")

# Streamlit UI Setup
st.set_page_config(page_title="Real Estate AI Assistant", page_icon="🏢")
st.title("🏢 Real Estate AI Assistant")
st.markdown("**Powered by Google Gemini & Databricks**")

# Load Env Vars
load_dotenv()

host = os.environ.get("DATABRICKS_HOST")
token = os.environ.get("DATABRICKS_TOKEN")
http_path = os.environ.get("DATABRICKS_HTTP_PATH")
gemini_api_key = os.environ.get("GEMINI_API_KEY")

if not all([host, token, http_path, gemini_api_key]):
    st.error("Error: Missing one or more environment variables in .env")
    st.stop()

clean_host = host.replace("https://", "")
uri = f"databricks://token:{token}@{clean_host}?http_path={http_path}&catalog=workspace"

@st.cache_resource
def init_db_and_llm():
    # 2. Use logging instead of print
    logging.info("Connecting to Databricks Data Warehouse...")
    try:
        # 10. Add pool_pre_ping and pool_recycle
        engine = create_engine(
            uri, 
            connect_args={"_tls_no_verify": True},
            pool_pre_ping=True,
            pool_recycle=3600
        )
        # 5. Limit Databricks Schema Loading
        tables_to_include = [
            "real_estate_gold_fact_sales",
            "real_estate_gold_dim_location",
            "real_estate_gold_dim_developer",
            "real_estate_gold_dim_property"
        ]
        db = SQLDatabase(engine, schema="default", include_tables=tables_to_include, sample_rows_in_table_info=2)
        logging.info(f"Connected successfully! Indexed tables: {db.get_usable_table_names()}")
    except Exception as e:
        logging.error(f"Failed to connect to Databricks: {e}")
        st.error(f"Failed to connect to Databricks: {e}")
        st.stop()

    # Initialize Gemini Model
    llm = ChatGoogleGenerativeAI(model="gemini-2.5-flash", google_api_key=gemini_api_key, temperature=0)
    return db, llm

db, llm = init_db_and_llm()

# 4. Separate Classification Chain (with JSON output)
classifier_prompt_template = """You are a router for a Real Estate Assistant.
Your job is to determine if the user's question is related to real estate data analysis (IN_SCOPE) or not.

Analyze the question carefully. Output ONLY a valid JSON object with two keys: "status" and "message".
Do not wrap it in markdown block.

Rules for "status" and "message":
- If the question is purely about real estate investments, property prices, developers, locations, or sales:
  "status": "IN_SCOPE", "message": ""
- If it is about personal chat, general coding, sports, weather, etc.:
  "status": "OOS_GENERAL", "message": "عذراً، أنا مساعد ذكي مخصص لتحليل البيانات العقارية والاستثمارية فقط."
- If asking about real estate out of our covered areas (e.g., London, Dubai):
  "status": "OOS_GEO", "message": "البيانات المتاحة لدي تغطي فقط المناطق الحالية في قاعدة بياناتنا."
- If asking for unsupported interior details (wall colors, appliances):
  "status": "OOS_UNSUPPORTED", "message": "تفاصيل التشطيب الداخلي غير متاحة في النظام حالياً."
- If ambiguous ("I want a good apartment" without context):
  "status": "OOS_AMBIGUOUS", "message": "هل يمكنك تحديد الميزانية التقريبية أو المنطقة المفضلة لتضييق نطاق البحث؟"

User Question: {question}
JSON Output:"""

classifier_prompt = PromptTemplate.from_template(classifier_prompt_template)
classifier_chain = classifier_prompt | llm

# 9. Cache the SQL Chain
@st.cache_resource
def init_sql_chain():
    # 8. Prevent Hallucination (Prompt Instruction)
    sql_prompt_template = """You are a Data Warehouse expert querying a Real Estate Database.
Given an input question, create a syntactically correct Databricks SQL query to run.
Return ONLY the SQL query string. Do NOT wrap it in markdown or ```sql code blocks.
Unless the user explicitly requests all records, limit result sets to 20 rows.

Important Data Model Context (Star Schema):
- The Fact table is `real_estate_gold_fact_sales`. It has foreign keys: `Location_Key`, `Developer_Key`, `Property_Key`.
- Metrics in Fact: `Total_Price`, `Size_SqM`, `Price_Per_SqM`, `Down_Payment_Pct`, `Installment_Years`.
- `real_estate_gold_dim_location` has `City`, `District`, `Distance_To_City_Center_KM`.
- `real_estate_gold_dim_developer` has `Developer_Name`, `Previous_Projects`.
- `real_estate_gold_dim_property` has `Unit_Type`, `Rooms_Count`, `Is_Ready_To_Move`.

Table Schemas:
{table_info}

Here is the conversation history for context (if any):
{history}

Question: {input}
Databricks SQL Query:"""
    
    prompt = PromptTemplate(
        input_variables=["input", "table_info", "history"],
        template=sql_prompt_template
    )
    return create_sql_query_chain(llm, db, prompt=prompt)

generate_query = init_sql_chain()

def get_answer(question, history_text):
    logging.info("[Gemini is analyzing the DWH...]")
    try:
        # Step 1: Classify Question (JSON)
        class_res = classifier_chain.invoke({"question": question})
        try:
            # 6. Eliminate double translation by grabbing Arabic message direct from JSON
            parsed_class = json.loads(class_res.content.replace("```json", "").replace("```", "").strip())
            if parsed_class.get("status") != "IN_SCOPE":
                return parsed_class.get("message", "عذراً، هذا السؤال خارج تخصصي.")
        except json.JSONDecodeError:
            logging.warning("Classifier returned invalid JSON. Assuming IN_SCOPE.")

        # Step 2: Text-to-SQL
        sql_query = generate_query.invoke({"question": question, "history": history_text})
        sql_query = sql_query.replace("```sql", "").replace("```", "").strip()
        
        # 3. Security Guardrail
        sql_upper = sql_query.upper()
        forbidden_keywords = ["DROP", "DELETE", "UPDATE", "INSERT", "ALTER", "TRUNCATE", "GRANT", "REVOKE"]
        if any(keyword in sql_upper for keyword in forbidden_keywords):
             return "عذراً، تم حظر هذا الطلب برمجياً لأسباب أمنية. الرجاء الالتزام بالاستفسارات عن البيانات."

        # 8. Safe Limit logic
        if (
            "LIMIT" not in sql_upper
            and "COUNT(" not in sql_upper
            and "AVG(" not in sql_upper
            and "SUM(" not in sql_upper
            and "MIN(" not in sql_upper
            and "MAX(" not in sql_upper
        ):
            sql_query += "\nLIMIT 20"

        # 2. Use st.write for SQL query debugging
        with st.expander("🔍 View Generated SQL"):
            st.code(sql_query, language="sql")
            
        logging.info(f"Executing SQL: {sql_query}")
        
        # Step 3: Run SQL in Databricks
        result = db.run(sql_query)
        
        # Step 4: Generate Natural Language Answer
        answer_prompt = f"""Given the following user question, the generated SQL query, and the result from the Databricks SQL Database, answer the user's question clearly.
        CRITICAL RULE: Your final answer MUST be in the EXACT same language that the user used in their Question. If they ask in English, answer in English. If Arabic, Arabic.
        Question: {question}
        SQL Query: {sql_query}
        Database Result: {result}
        Answer: """
        
        final_answer = llm.invoke(answer_prompt)
        return final_answer.content
    except Exception as e:
        logging.error(f"Error executing RAG workflow: {e}")
        return f"Error executing RAG workflow: {e}"

# Initialize chat history
if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# React to user input
if user_q := st.chat_input("Ask a question about your real estate data..."):
    st.chat_message("user").markdown(user_q)
    st.session_state.messages.append({"role": "user", "content": user_q})

    # 7. Chat History formatting
    recent_history = st.session_state.messages[-4:]
    history_str = ""
    if len(recent_history) > 1:
        history_str = "Conversation Context:\n"
        for msg in recent_history[:-1]:
            role_name = "User" if msg["role"] == "user" else "Assistant"
            history_str += f"{role_name}: {msg['content']}\n"

    with st.spinner("Analyzing Databricks Data Warehouse..."):
        response = get_answer(user_q, history_str)
    
    with st.chat_message("assistant"):
        st.markdown(response)
    st.session_state.messages.append({"role": "assistant", "content": response})
