from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(seconds=10)
}

with DAG(
    dag_id='transaction_streaming_pipeline',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    description="Real-time transaction data pipeline using Kafka",
    tags=['kafka', 'realtime'],
) as dag:

    stream_raw_data = BashOperator(
        task_id='stream_raw_data',
        bash_command='timeout 10s python /opt/airflow/realtime_produce_data.py || [[ $? -eq 124 ]]'
    )

    process_and_clean_data = BashOperator(
        task_id='process_and_clean_data',
        bash_command='timeout 15s python /opt/airflow/process_raw_data.py || [[ $? -eq 124 ]]'
    )

    notify_pipeline_complete = BashOperator(
        task_id='notify_pipeline_complete',
        bash_command='echo "✅ Streaming pipeline executed!"'
    )

    [stream_raw_data, process_and_clean_data] >> notify_pipeline_complete
