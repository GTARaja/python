# Oracle SQL Monitoring Tool - Scalable & Modular

import pandas as pd
import threading
import time
import cx_Oracle
import datetime
from flask import Flask, render_template, jsonify
import plotly.graph_objs as go
from plotly.subplots import make_subplots
import os

# ===================== CONFIGURATION =====================
QUERY_CSV = "queries.csv"  # CSV file with SQL queries
QUERY_CONFIG = {}          # Stores query text, frequency, description
RESULTS = {}               # To store execution results per query
LOCK = threading.Lock()

# Oracle DB credentials (set as env vars or hardcode carefully)
DB_CONFIG = {
    'user': os.getenv('ORACLE_USER', 'your_user'),
    'password': os.getenv('ORACLE_PASSWORD', 'your_pass'),
    'dsn': os.getenv('ORACLE_DSN', 'localhost/orclpdb1')
}

# ===================== QUERY EXECUTOR =====================
def execute_query_periodically(query_id):
    query_text = QUERY_CONFIG[query_id]['query']
    frequency = QUERY_CONFIG[query_id]['frequency']
    description = QUERY_CONFIG[query_id]['desc']

    while True:
        start_time = datetime.datetime.now()
        try:
            with cx_Oracle.connect(**DB_CONFIG) as connection:
                cursor = connection.cursor()
                cursor.execute(query_text)
                result = cursor.fetchone()
                count_value = result[0] if result else 0
        except Exception as e:
            count_value = -1
            print(f"Error executing {query_id}: {e}")

        duration = (datetime.datetime.now() - start_time).total_seconds()
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        with LOCK:
            if query_id not in RESULTS:
                RESULTS[query_id] = {
                    'desc': description,
                    'data': []
                }
            RESULTS[query_id]['data'].append({
                'time': timestamp,
                'count': count_value,
                'duration': duration
            })

        time.sleep(frequency)

# ===================== LOADER =====================
def load_queries():
    df = pd.read_csv(QUERY_CSV)
    for index, row in df.iterrows():
        query_id = f"Query_{index+1}"
        QUERY_CONFIG[query_id] = {
            'query': row['query'],
            'frequency': int(row['frequency']),
            'desc': row.get('query_desc', f"Description for {query_id}")
        }
        threading.Thread(
            target=execute_query_periodically,
            args=(query_id,),
            daemon=True
        ).start()

# ===================== DASHBOARD =====================
app = Flask(__name__)

@app.route('/')
def index():
    return render_template("dashboard.html")

@app.route('/data')
def data():
    with LOCK:
        return jsonify(RESULTS)

# ===================== RUN =====================
if __name__ == '__main__':
    load_queries()
    app.run(debug=True, port=5000)
