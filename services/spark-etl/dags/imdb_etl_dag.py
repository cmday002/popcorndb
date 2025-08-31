"""
IMDb ETL Pipeline DAG

This Airflow DAG orchestrates the complete IMDb data ingestion pipeline:
1. Download IMDb data (if needed)
2. Run Spark ETL job
3. Validate data quality
4. Generate reports
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'popcorndb',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'imdb_etl_pipeline',
    default_args=default_args,
    description='IMDb data ingestion and ETL pipeline',
    schedule_interval='@daily',
    catchup=False,
    tags=['imdb', 'etl', 'spark', 'iceberg'],
)

# Task 1: Download IMDb data (optional - only if data doesn't exist)
download_data = BashOperator(
    task_id='download_imdb_data',
    bash_command="""
        # Check if data already exists
        if [ ! -f "/opt/data/raw/title.basics.tsv" ]; then
            echo "Downloading IMDb data..."
            python /opt/spark-apps/scripts/download_imdb_data.py
        else
            echo "IMDb data already exists, skipping download"
        fi
    """,
    dag=dag,
)

# Task 2: Run Spark ETL job
run_spark_etl = BashOperator(
    task_id='run_spark_etl',
    bash_command="""
        echo "Starting Spark ETL job..."
        spark-submit \
            --master local[*] \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.iceberg.type=hive \
            --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
            --conf spark.sql.catalog.iceberg.warehouse=/opt/data/warehouse \
            /opt/spark-apps/jobs/imdb_ingest.py
    """,
    dag=dag,
)

# Task 3: Run data quality checks
run_data_quality = BashOperator(
    task_id='run_data_quality_checks',
    bash_command="""
        echo "Running data quality checks..."
        spark-submit \
            --master local[*] \
            --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
            --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
            --conf spark.sql.catalog.iceberg.type=hive \
            --conf spark.sql.catalog.iceberg.uri=thrift://hive-metastore:9083 \
            --conf spark.sql.catalog.iceberg.warehouse=/opt/data/warehouse \
            /opt/spark-apps/jobs/data_quality_check.py
    """,
    dag=dag,
)

# Task 4: Generate summary report
def generate_summary_report(**context):
    """Generate a summary report of the ETL process."""
    import os
    import json
    
    # Get task instance info
    ti = context['ti']
    
    # Create a simple summary report
    report = {
        'dag_run_id': context['dag_run'].run_id,
        'execution_date': context['execution_date'].isoformat(),
        'status': 'completed',
        'tasks': {
            'download_data': 'success',
            'spark_etl': 'success',
            'data_quality': 'success'
        },
        'timestamp': datetime.now().isoformat()
    }
    
    # Save report to file
    report_path = f"/opt/data/warehouse/reports/imdb_etl_report_{context['execution_date'].strftime('%Y%m%d')}.json"
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    logging.info(f"Summary report generated: {report_path}")
    return report_path

generate_report = PythonOperator(
    task_id='generate_summary_report',
    python_callable=generate_summary_report,
    dag=dag,
)

# Task 5: Success notification (optional)
success_notification = BashOperator(
    task_id='success_notification',
    bash_command='echo "IMDb ETL pipeline completed successfully!"',
    dag=dag,
)

# Define task dependencies
download_data >> run_spark_etl >> run_data_quality >> generate_report >> success_notification

# Add task documentation
download_data.doc = "Download IMDb TSV data files if they don't exist"
run_spark_etl.doc = "Run the main Spark ETL job to process IMDb data into Iceberg tables"
run_data_quality.doc = "Run data quality checks to validate the ingested data"
generate_report.doc = "Generate a summary report of the ETL process"
success_notification.doc = "Send success notification"
