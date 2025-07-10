# Oracle SQL Monitoring Tool - Scalable & Modular

import pandas as pd
import threading
import time
import cx_Oracle
import datetime
from flask import Flask, render_template, jsonify, request
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
                columns = [col[0] for col in cursor.description]
                result = cursor.fetchall()
                records = [{col: val for col, val in zip(columns, row)} for row in result]
        except Exception as e:
            records = [{"error": str(e)}]
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
                'duration': duration,
                'records': records
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
    from_ts = request.args.get('from')
    to_ts = request.args.get('to')

    with LOCK:
        filtered_results = {}
        for qid, info in RESULTS.items():
            filtered_data = []
            for entry in info['data']:
                entry_time = datetime.datetime.strptime(entry['time'], '%Y-%m-%d %H:%M:%S')
                if from_ts:
                    from_time = datetime.datetime.strptime(from_ts, '%Y-%m-%d %H:%M:%S')
                    if entry_time < from_time:
                        continue
                if to_ts:
                    to_time = datetime.datetime.strptime(to_ts, '%Y-%m-%d %H:%M:%S')
                    if entry_time > to_time:
                        continue
                filtered_data.append(entry)
            filtered_results[qid] = {
                'desc': info['desc'],
                'data': filtered_data
            }
        return jsonify(filtered_results)

# ===================== RUN =====================
if __name__ == '__main__':
    load_queries()
    app.run(debug=True, port=int(os.getenv('PORT', 5000)))
