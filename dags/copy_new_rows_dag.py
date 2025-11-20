from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

def sync_new_rows():
    # Connect to SOURCE (Docker Postgres on 5433)
    src_conn = psycopg2.connect(
        host="source-postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    src_cur = src_conn.cursor()

    # Connect to TARGET (Windows local Postgres on 5432)
    tgt_conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="local-airflow",
        user="pipeline_user",
        password="pipeline_pass"
    )
    tgt_cur = tgt_conn.cursor()

    # Step 1: Find the max id in secondary table
    tgt_cur.execute("SELECT COALESCE(MAX(id), 0) FROM secondary_table")
    last_id = tgt_cur.fetchone()[0]

    # Step 2: Fetch new rows from main_table
    src_cur.execute("""
        SELECT id, name, amount, created_at
        FROM main_table
        WHERE id > %s
        ORDER BY id;
    """, (last_id,))

    new_rows = src_cur.fetchall()

    # Step 3: Insert new rows into secondary table
    if new_rows:
        insert_query = """
            INSERT INTO secondary_table (id, name, amount, created_at)
            VALUES (%s, %s, %s, %s)
        """
        tgt_cur.executemany(insert_query, new_rows)
        tgt_conn.commit()

    # Cleanup
    src_cur.close()
    tgt_cur.close()
    src_conn.close()
    tgt_conn.close()


with DAG(
        dag_id='copy_new_rows_dag',
        default_args=default_args,
        schedule='@hourly',   # FIXED
        catchup=False
) as dag:

    sync_task = PythonOperator(
        task_id='sync_new_rows_task',
        python_callable=sync_new_rows
    )
