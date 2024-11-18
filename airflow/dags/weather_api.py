from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.utils.email import send_email
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import pandas as pd
import pytz
import os
import pendulum
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
import logging
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import io
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator




#Weather API: Extract
def extract_weather_data():
    http_hook = HttpHook(http_conn_id='openweather_api_chicago', method='GET') 

    api_key = Variable.get('openweather_api_key')
    if not api_key:
        raise ValueError("API key not found")

    city = 'Chicago' 
    endpoint = f'/data/2.5/weather?appid={api_key}&q={city}'
    response = http_hook.run(endpoint=endpoint)

    return response.json() 


#Transform data: Transform
def transform_weather_data(ti):

    #Ceating timestamp column when this script activate
    thailand_tz = pytz.timezone('Asia/Bangkok')
    current_timestamp = int(datetime.now(thailand_tz).timestamp())

    #Extract rawdata
    raw_data = ti.xcom_pull(task_ids='extract_data')

    #Flatten JSON data
    df_flat = pd.json_normalize(raw_data)


    def convert_unix_to_datetime(unix_time, timezone_str):
        utc_time = datetime.utcfromtimestamp(unix_time).replace(tzinfo=pytz.utc)
        return utc_time.astimezone(pytz.timezone(timezone_str)).strftime('%Y-%m-%d-%H:%M')

    def kelvin_to_celsius(temp_kelvin):
        return int(temp_kelvin - 273.15)

    df_flat['weather_id']              = df_flat['weather'].apply(lambda x: x[0]['id'] if isinstance(x, list) else None)
    df_flat['weather_main']            = df_flat['weather'].apply(lambda x: x[0]['main'] if isinstance(x, list) else None)
    df_flat['weather_description']     = df_flat['weather'].apply(lambda x: x[0]['description'] if isinstance(x, list) else None)

    df_flat['dt_chicago']              = df_flat['dt'].apply(lambda x: convert_unix_to_datetime(x, 'America/Chicago'))
    df_flat['dt_thailand']             = df_flat['dt'].apply(lambda x: convert_unix_to_datetime(x, 'Asia/Bangkok'))
    
    df_flat['sys.sunrise_chicago']     = df_flat['sys.sunrise'].apply(lambda x: convert_unix_to_datetime(x, 'America/Chicago'))
    df_flat['sys.sunrise_thailand']    = df_flat['sys.sunrise'].apply(lambda x: convert_unix_to_datetime(x, 'Asia/Bangkok'))
    
    df_flat['sys.sunset_chicago']      = df_flat['sys.sunset'].apply(lambda x: convert_unix_to_datetime(x, 'America/Chicago'))
    df_flat['sys.sunset_thailand']     = df_flat['sys.sunset'].apply(lambda x: convert_unix_to_datetime(x, 'Asia/Bangkok'))

    df_flat['main.temp_celsius']       = df_flat['main.temp'].apply(kelvin_to_celsius)
    df_flat['main.feels_like_celsius'] = df_flat['main.feels_like'].apply(kelvin_to_celsius)
    df_flat['main.temp_min_celsius']   = df_flat['main.temp_min'].apply(kelvin_to_celsius)
    df_flat['main.temp_max_celsius']   = df_flat['main.temp_max'].apply(kelvin_to_celsius)

    # Replace '.' with '_' in column names #For fixing erros when create table in BigQuery
    df_flat.columns = df_flat.columns.str.replace('.', '_', regex=False)


    #Add timestamp in first column
    df_flat.insert(0, 'event_timestamp', current_timestamp)

    df_flat = df_flat[[
        'event_timestamp',
        'name', 'coord_lat', 'coord_lon', 'dt', 'dt_chicago', 'dt_thailand',
        'sys_sunrise', 'sys_sunrise_chicago', 'sys_sunrise_thailand',
        'sys_sunset', 'sys_sunset_chicago', 'sys_sunset_thailand',
        'main_temp', 'main_temp_celsius', 'main_feels_like', 'main_feels_like_celsius',
        'main_temp_min', 'main_temp_min_celsius', 'main_temp_max', 'main_temp_max_celsius',
        'main_pressure', 'main_humidity', 'main_sea_level', 'main_grnd_level',
        'wind_speed', 'base', 'visibility', 'weather_id', 'weather_main', 'weather_description'
    ]]

    df_flat.drop(columns=['weather'], errors='ignore', inplace=True)
    
    ti.xcom_push(key='transformed_data', value=df_flat.to_dict())


#Saving CSV file to my local folder
def load_to_csv(ti):

    #Get the current time in Thailand
    thailand_tz  = pytz.timezone('Asia/Bangkok')
    now_thailand = datetime.now(thailand_tz)

    #Format the time as "year-month-day-hour-minute"
    formatted_time = now_thailand.strftime('%Y-%m-%d-%H-%M')

    #Define the directory and filename using Windows path
    output_dir  = Variable.get("local_csv_output_dir")
    output_file = os.path.join(output_dir, f'weather_chicago_{formatted_time}.csv')

    #Ensure that the directory exists
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    #Retrieve transformed data from XCom
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    
    #Convert dictionary back to DataFrame
    df = pd.DataFrame.from_dict(transformed_data)
    
    #Save DataFrame to CSV
    df.to_csv(output_file, index=False)


#Saving CSV file to GCP: Cloud Storage
def save_to_gcs_csv(ti):
    bucket_name = 'weather_api_data_chicago'
    folder_path = ''

    thailand_tz = pytz.timezone('Asia/Bangkok')
    now_thailand = datetime.now(thailand_tz)
    formatted_time = now_thailand.strftime('%Y-%m-%d-%H-%M')

    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.DataFrame.from_dict(transformed_data)

    csv_data = df.to_csv(index=False)

    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    file_name = f'weather_chicago_{formatted_time}.csv'

    #Upload the CSV file to GCS
    gcs_hook.upload(bucket_name=bucket_name, object_name=f'{folder_path}{file_name}', data=csv_data, mime_type='text/csv')

    #Push the file name to XCom for the next task to retrieve
    ti.xcom_push(key='latest_csv_file', value=file_name)


#Saving Parquet file to GCP: Cloud Storage
def save_to_gcs_parquet(ti):
    bucket_name = 'weather_api_data_chicago' 
    folder_path = ''

    thailand_tz    = pytz.timezone('Asia/Bangkok')
    now_thailand   = datetime.now(thailand_tz)
    formatted_time = now_thailand.strftime('%Y-%m-%d-%H-%M')

    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.DataFrame.from_dict(transformed_data)


    #Convert the DataFrame to a Parquet file in memory
    table = pa.Table.from_pandas(df)
    parquet_buffer = pa.BufferOutputStream()
    pq.write_table(table, parquet_buffer)

    #Get the buffer content (parquet file in bytes)
    parquet_data = parquet_buffer.getvalue().to_pybytes()

   
    #Initialize GCS Hook and upload the parquet file to GCS
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    file_name = f'weather_chicago_{formatted_time}.parquet'

    #Upload the parquet file as bytes
    gcs_hook.upload(bucket_name=bucket_name, object_name=f'{folder_path}{file_name}', data=parquet_data, mime_type='application/octet-stream')



#Saving CSV file to Amazon S3
def save_to_s3_csv(ti):
    bucket_name = 'weather-api-data-chicago'
    folder_path = ''

    thailand_tz = pytz.timezone('Asia/Bangkok')
    now_thailand = datetime.now(thailand_tz)
    formatted_time = now_thailand.strftime('%Y-%m-%d-%H-%M')

    #Retrieve transformed data from XCom
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.DataFrame.from_dict(transformed_data)

    #Convert the DataFrame to CSV format in memory
    csv_data = df.to_csv(index=False)

    #Use S3Hook to handle credentials
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_file_path = f'{folder_path}weather_chicago_{formatted_time}.csv'

    #Upload the CSV file to S3
    try:
        s3_hook.load_string(csv_data, key=s3_file_path, bucket_name=bucket_name, replace=True)
        logging.info(f"CSV file successfully uploaded to S3: s3://{bucket_name}/{s3_file_path}")
    except Exception as e:
        logging.error(f"Failed to upload CSV to S3: {str(e)}")
        raise e
    


#Saving CSV file to Postgres
def save_to_postgresql(ti):
    #Retrieve transformed data from XCom
    transformed_data = ti.xcom_pull(task_ids='transform_data', key='transformed_data')
    df = pd.DataFrame.from_dict(transformed_data)

    #Define the PostgreSQL table name
    table_name = 'weather_data_chicago'

    #Establish a connection to PostgreSQL using PostgresHook
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    #Load data to PostgreSQL
    with pg_hook.get_conn() as conn:
        # Convert DataFrame to CSV format and use StringIO to enable loading without saving to file
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False, header=False)
        csv_buffer.seek(0)

        #Execute the COPY command to load CSV data directly to PostgreSQL
        with conn.cursor() as cursor:
            cursor.copy_expert(f"""
                COPY {table_name} FROM STDIN WITH CSV DELIMITER ',' NULL AS '' 
            """, csv_buffer)
        conn.commit()
    

    

#Notification functions (Line)
def send_line_notify(message):
    token   = Variable.get("line_notify_token")  
    if not token:
        raise ValueError("LINE Notify token not found")
    headers = {"Authorization": f"Bearer {token}"}
    payload = {'message': message}
    response = requests.post("https://notify-api.line.me/api/notify", headers=headers, params=payload)
    
    return response.status_code



# Notification functions (Slack)
def send_slack_message(message):
    slack_webhook_url = Variable.get("slack_webhook_url")
    if not slack_webhook_url:
        raise ValueError("Slack webhook URL not found in Variables.")
    
    payload = {"text": message}
    response = requests.post(slack_webhook_url, json=payload)

    if response.status_code != 200:
        logging.error(f"Failed to send Slack message: {response.status_code}, {response.text}")
        raise ValueError(f"Slack message failed: {response.status_code}, {response.text}")
    else:
        logging.info(f"Slack message sent successfully: {message}")



# Notification alert
def notification_alert(context):
    task_instance = context.get('task_instance')
    task_state = task_instance.state
    if task_state == 'success':
        message = (
            f"✅ Task Succeeded\n"
            f"Task: {task_instance.task_id}\n"
            f"DAG: {task_instance.dag_id}\n"
            f"Execution Time: {context['execution_date']}\n"
            f"Log URL: {task_instance.log_url}"
        )
    else:
        message = (
            f"❌ Task Failed\n"
            f"Task: {task_instance.task_id}\n"
            f"DAG: {task_instance.dag_id}\n"
            f"Execution Time: {context['execution_date']}\n"
            f"Log URL: {task_instance.log_url}"
        )

    # Send notifications
    try:
        send_line_notify(message)
    except Exception as e:
        logging.error(f"LINE notification failed: {str(e)}")
    try:
        send_slack_message(message)
    except Exception as e:
        logging.error(f"Slack notification failed: {str(e)}")    

   
    



    

# Set timezone for Thailand (Asia/Bangkok) using pendulum
local_tz = pendulum.timezone('Asia/Bangkok')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2024, 9, 9),
    # 'email': ['test@test.com'],
    # 'email_on_failure': True,
    # 'email_on_retry': False,
    # 'email_on_success': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(minutes=1), # Task should be finish after 1 minutes
    'on_failure_callback': notification_alert,
    'on_success_callback': notification_alert
}

# Define the DAG
with DAG(
    dag_id='weather_etl_chicago',
    default_args=default_args,
    description='ETL for weather data using OpenWeather API',
    schedule_interval='0 9,12,15,18,21 * * *',  # Run at 9am, 12pm, 3pm, 6pm, 9pm
    start_date=pendulum.datetime(2024, 9, 9, tz=local_tz),
    catchup=False,
    tags=['weather_api']
) as dag:

    # Task 1: Extract weather data
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_weather_data
    )

    # Task 2: Transform weather data
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_weather_data
    )

    # Task 3: Load data to CSV
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_to_csv
    )

     # Task 4: Save CSV to GCS
    save_csv_to_gcs = PythonOperator(
        task_id='save_csv_to_gcs',
        python_callable=save_to_gcs_csv
    )

    # Task 5: Save Parquet to GCS
    save_parquet_to_gcs = PythonOperator(
        task_id='save_parquet_to_gcs',
        python_callable=save_to_gcs_parquet
    )


    # Task 6: Insert CSV file from Cloud Stroage to BigQuery
    load_csv_to_bigquery = BigQueryInsertJobOperator(
    task_id='load_csv_to_bigquery',
    configuration={
        "load": {
            "source_uris": ["gs://weather_api_data_chicago/{{ ti.xcom_pull(task_ids='save_csv_to_gcs', key='latest_csv_file') }}"],
            "destination_table": {
                "projectId": "persornal-projects",  
                "datasetId": "weather_api_data",    
                "tableId": "weather_data_chicago", 
            },
            "schema": {
                "fields": [
                    {"name": "event_timestamp",         "type": "INTEGER"},
                    {"name": "name",                    "type": "STRING"},
                    {"name": "coord_lat",               "type": "FLOAT"},
                    {"name": "coord_lon",               "type": "FLOAT"},
                    {"name": "dt",                      "type": "INTEGER"},
                    {"name": "dt_chicago",              "type": "STRING"},
                    {"name": "dt_thailand",             "type": "STRING"},
                    {"name": "sys_sunrise",             "type": "INTEGER"},
                    {"name": "sys_sunrise_chicago",     "type": "STRING"},
                    {"name": "sys_sunrise_thailand",    "type": "STRING"},
                    {"name": "sys_sunset",              "type": "INTEGER"},
                    {"name": "sys_sunset_chicago",      "type": "STRING"},
                    {"name": "sys_sunset_thailand",     "type": "STRING"},
                    {"name": "main_temp",               "type": "FLOAT"},
                    {"name": "main_temp_celsius",       "type": "INTEGER"},
                    {"name": "main_feels_like",         "type": "FLOAT"},
                    {"name": "main_feels_like_celsius", "type": "INTEGER"},
                    {"name": "main_temp_min",           "type": "FLOAT"},
                    {"name": "main_temp_min_celsius",   "type": "INTEGER"},
                    {"name": "main_temp_max",           "type": "FLOAT"},
                    {"name": "main_temp_max_celsius",   "type": "INTEGER"},
                    {"name": "main_pressure",           "type": "INTEGER"},
                    {"name": "main_humidity",           "type": "INTEGER"},
                    {"name": "main_sea_level",          "type": "INTEGER"},
                    {"name": "main_grnd_level",         "type": "INTEGER"},
                    {"name": "wind_speed",              "type": "FLOAT"},
                    {"name": "base",                    "type": "STRING"},
                    {"name": "visibility",              "type": "INTEGER"},
                    {"name": "weather_id",              "type": "INTEGER"},
                    {"name": "weather_main",            "type": "STRING"},
                    {"name": "weather_description",     "type": "STRING"}
                ]
            },
            "source_format": "CSV",
            "create_disposition": "CREATE_IF_NEEDED",
            "write_disposition": "WRITE_APPEND",
            "skip_leading_rows": 1,
            "max_bad_records": 10
        }
    },
    gcp_conn_id='google_cloud_default'
    )

    # Task7: Saving csv file to Amazon S3
    save_csv_to_s3 = PythonOperator(
        task_id='save_csv_to_s3',
        python_callable=save_to_s3_csv
    )

    # Task8: to save data to PostgreSQL
    save_to_postgres = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgresql
    )


    # Sequence of Pipeline Task
    # extract_data >> transform_data >> load_data
    extract_data >> transform_data >> [load_data, save_csv_to_gcs, save_parquet_to_gcs, save_csv_to_s3, save_to_postgres]

    save_csv_to_gcs >> load_csv_to_bigquery