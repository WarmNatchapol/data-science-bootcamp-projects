# import modules
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import requests
from airflow.providers.google.cloud.operators.dataproc import  DataprocCreateClusterOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocDeleteClusterOperator
from datetime import timedelta

# define arguments
default_args = {
    "owner": "Natchapol Laowiwatkasem",
    "depends_on_past": False,
    "retries": 1   
}

GOOGLE_CONN_ID = "google_cloud_default"
CLUSTER_NAME = "airflow-auto-cluster"
REGION = "asia-east2"
PROJECT_ID = "workshop-modified"
PYSPARK_URI = f"gs://asia-east2-modified-version-eaa4cbd5-bucket/data/pyspark_final.py"

# set pyspark job
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": PYSPARK_URI},
}

# connection to mysql and api
mysql_connection = "mysql_default"
conversion_rate_url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

# output path
audible_data_output = "/home/airflow/gcs/data/audible_data.csv"
conversion_output = "/home/airflow/gcs/data/conversion.csv"

# def function read audible data from mysql server
def data_mysql(audible_path):
    # connect to mysql server
    mysql = MySqlHook(mysql_connection)

    # query data as data frame from database
    audible_data = mysql.get_pandas_df(sql = "SELECT * FROM audible_data")
    audible_transaction = mysql.get_pandas_df(sql = "SELECT * FROM audible_transaction")

    # merge data
    df = audible_transaction.merge(audible_data, how = "left", left_on = "book_id", right_on = "Book_ID")

    # save file to gcs
    df.to_csv(audible_path, index = False)
    print(f"File is already save to {audible_path}")


# def function read conversion rate through API
def conversion_api(conversion_path):
    # get data
    response = requests.get(conversion_rate_url)
    result = response.json()
    
    # change to data frame and change column name
    df = pd.DataFrame(result)
    df = df.reset_index().rename(columns = {"index": "date"})

    # save file to gcs
    df.to_csv(conversion_path, index = False)
    print(f"File is already save to {conversion_path}")

# DAG
with DAG(
    "pipeline_dag_spark",
    default_args = default_args,
    description = "DAG to read and merge data with DataProc",
    start_date = days_ago(1),
    schedule_interval = "@once"
) as dag:

    dag.doc_md = """
    1. Read data and save to GCS
    2. Create DataProc Cluster
    3. Merge data and save to GCS
    4. Delete DataProc Cluster
    """

    create_cluster = DataprocCreateClusterOperator(
        task_id = "create_cluster",
        project_id = PROJECT_ID,
        num_workers = 0,
        region = REGION,
        cluster_name = CLUSTER_NAME,
    )

    read_data = PythonOperator(
        task_id = "read_data_from_mysql",
        python_callable = data_mysql,
        op_kwargs = {"audible_path": audible_data_output,},
    )

    read_conversion = PythonOperator(
        task_id = "read_conversion_through_api",
        python_callable = conversion_api,
        op_kwargs = {"conversion_path": conversion_output,},
    )

    submit_job = DataprocSubmitJobOperator(
        task_id = "pyspark_task", 
        job = PYSPARK_JOB, 
        region = REGION, 
        project_id = PROJECT_ID
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id = "delete_cluster", 
        project_id = PROJECT_ID, 
        cluster_name = CLUSTER_NAME, 
        region = REGION
    )

    # setup dependencies
    [read_data, read_conversion, create_cluster] >> submit_job >> delete_cluster
