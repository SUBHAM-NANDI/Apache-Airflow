from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    'owner': 'airflow',
    'start_date': datetime.now() - timedelta(days=1)
}

def sync_new_rows():
    src_conn = psycopg2.connect(
        host="source-postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"
    )
    src_cur = src_conn.cursor()

    tgt_conn = psycopg2.connect(
        host="host.docker.internal",
        port=5432,
        database="local-airflow",
        user="pipeline_user",
        password="pipeline_pass"
    )
    tgt_cur = tgt_conn.cursor()

    tgt_cur.execute("SELECT COALESCE(MAX(id), 0) FROM secondary_table")
    last_id = tgt_cur.fetchone()[0]

    src_cur.execute("""
        SELECT id, name, amount, created_at
        FROM main_table
        WHERE id > %s
        ORDER BY id;
    """, (last_id,))

    new_rows = src_cur.fetchall()

    if new_rows:
        insert_query = """
            INSERT INTO secondary_table (id, name, amount, created_at)
            VALUES (%s, %s, %s, %s)
        """
        tgt_cur.executemany(insert_query, new_rows)
        tgt_conn.commit()

    src_cur.close()
    tgt_cur.close()
    src_conn.close()
    tgt_conn.close()

with DAG(
        dag_id='copy_new_rows_dag',
        default_args=default_args,
        schedule='@hourly',   # <-- THIS FIXES YOUR ERROR
        catchup=False
) as dag:

    sync_task = PythonOperator(
        task_id='sync_new_rows_task',
        python_callable=sync_new_rows
    )
