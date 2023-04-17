from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

def load_mysql_product_prices(**kwargs):
    data = pd.read_csv('gs://product/prices/2022-07-01.csv')
    
    target_db = "database_name"
    table = "product_prices"
    conn_str = Variable.get("conn_str")
    
    try:
        conn_str = f"{conn_str}/{target_db}"
        engine = create_engine(conn_str)
        data.to_sql(table, con=engine, if_exists='append', chunksize=1000)
    except SQLAlchemyError as e:
        raise ValueError(f"Error loading data into MySQL: {e}")

with DAG(
    "improved_dag",
    description="Example DAG",
    schedule_interval="30 1 * * *",
    start_date=days_ago(1),
    max_active_runs=1,
    catchup=False,
    tags=['example'],
) as dag:

    truncate_gcs_product_prices = GCSDeleteObjectsOperator(
        task_id='truncate_gcs_target',
        bucket_name='product',
        prefix='prices',
    )

    copy_product_prices = GCSToGCSOperator(
        task_id='copy_gcs_product_prices',
        source_bucket='vendor',
        source_object='product/prices/*',
        destination_bucket='product',
        destination_object='prices',
    )

    
    #It isn't a good practice to run python code from a different file, but for the sake of the challenge, this is the best way to do it.
    clean_product_prices = PythonOperator(
        task_id='clean_product_prices',
        python_callable=lambda: exec(open("project/data/utils/clean_product_prices.py").read()),
    )

    load_mysql = PythonOperator(
        task_id='load_mysql',
        python_callable=load_mysql_product_prices,
    )

    truncate_gcs_product_prices >> copy_product_prices >> clean_product_prices >> load_mysql
