# importing modules
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.mysql.hooks.mysql import MySqlHook
import pandas as pd
import requests
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# connection to mysql and api
mysql_connection = "mysql_default"
conversion_rate_url = "https://r2de2-workshop-vmftiryt6q-ts.a.run.app/usd_thb_conversion_rate"

# output path
audible_data_output = "/home/airflow/gcs/data/audible_data.csv"
conversion_output = "/home/airflow/gcs/data/conversion.csv"
final_output = "/home/airflow/gcs/data/output.csv"

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


# def function to merge 2 tables
def merge_data(audible_path, conversion_path, output_path):
    # read file from gcs
    audible_data = pd.read_csv(audible_data_output)
    conversion_data = pd.read_csv(conversion_output)

    # create date column that will use in merging step
    audible_data["date"] = audible_data["timestamp"]
    audible_data["date"] = pd.to_datetime(audible_data["date"]).dt.date
    conversion_data["date"] = pd.to_datetime(conversion_data["date"]).dt.date

    # merge 2 dataframe
    df = audible_data.merge(conversion_data, how = "left", left_on = "date", right_on = "date")

    # change price to float and add thai baht price column
    df["Price"] = df.apply(lambda a: a["Price"].replace("$",""), axis = 1)
    df["Price"] = df["Price"].astype(float)

    df["THBPrice"] = df["Price"] * df["conversion_rate"]

    # drop unused columns
    df = df.drop(["date", "book_id"], axis = 1)

    # save file to gcs
    df.to_csv(output_path, index = False)
    print(f"File is already save to {output_path}")

# DAG and tasks
with DAG(
    "pipeline_dag",
    start_date = days_ago(1),
    schedule_interval = "@once"
) as dag:

    dag.doc_md = """
    1. Read data from MySQL and API, then save to GCS
    2. Merged data and save to GCS
    3. Upload to BigQuery
    """

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

    merge_data = PythonOperator(
        task_id = "merge_data",
        python_callable = merge_data,
        op_kwargs = {
            "audible_path": audible_data_output,
            "conversion_path": conversion_output,
            "output_path": final_output,
        },
    )

    gcs_to_bq = GCSToBigQueryOperator(
        task_id = "gcs_to_bq",
        bucket = "asia-east2-r2de-workshop-pr-5d7269a8-bucket",
        source_objects = ["data/output.csv"],
        destination_project_dataset_table = "ws_project.audible_data",
        skip_leading_rows=1,
        schema_fields=[
            {
                "mode": "NULLABLE",
                "name": "timestamp",
                "type": "TIMESTAMP"
            },
            {
                "mode": "NULLABLE",
                "name": "user_id",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "country",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_ID",
                "type": "INTEGER"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Title",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Subtitle",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Author",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Book_Narrator",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Audio_Runtime",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Audiobook_Type",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Categories",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Rating",
                "type": "STRING"
            },
            {
                "mode": "NULLABLE",
                "name": "Total_No__of_Ratings",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "Price",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "conversion_rate",
                "type": "FLOAT"
            },
            {
                "mode": "NULLABLE",
                "name": "THBPrice",
                "type": "FLOAT"
            }
        ],
        write_disposition = "WRITE_APPEND",
    )

    # setting up dependencies
    [read_data, read_conversion] >> merge_data >> gcs_to_bq
