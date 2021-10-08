
from datetime import timedelta
from datetime import datetime
from pathlib import Path
import os
import joblib
import json
import re
from tempfile import NamedTemporaryFile
from pymongo import MongoClient
import requests

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago

from minio import Minio

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

FILENAME="profile_data.json"

def write_pics_to_minio(file_path):
    minio_client = Minio(
        endpoint="minio:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )

    if not minio_client.bucket_exists("profile-pics"):
        minio_client.make_bucket("profile-pics")

    with open(file_path) as f:
        file_data = json.load(f)  # list of dicts with each dict containing one profile

    # now = str(datetime.now())
    # now_formatted = re.sub(r"[ .:]", "-", now)
    for profile in file_data:
        n_pics = len(profile["pic_addresses"])
        for i in range(n_pics):
            url = profile["pic_addresses"][i]
            id = str(profile["_id"]) + "_" + str(i) + ".jpg"
            # send this pic to minio
            response = requests.get(url)
            if response.status_code == 200:
                with open(id, 'wb') as f:
                    f.write(response.content)
            del response
            # put to minio container bucket
            minio_client.fput_object(
                bucket_name="profile-pics",
                object_name=id,
                file_path=id
            )
            os.remove(id)

def write_to_mongo(file_path):
    client = MongoClient('mongodb://airflow-example_mongo_1',
                        username="root",
                        password="example")
    db = client["tinder-data"]
    profile_collection = db.profiles
    with open(file_path) as f:
        file_data = json.load(f)

    try:
        result = profile_collection.insert_many(file_data, ordered=False)
    except:
        print("There were duplicates.")
        pass

    client.close()

def check_file_content(file_path):
    try:
        with open(file_path) as f:
            json.load(f)
    except ValueError as e:
        print('Invalid json file.' % e)
        raise

# def check_minio_bucket():
#     minio_client = Minio(
#         endpoint="minio:9000",
#         access_key="minio",
#         secret_key="minio123",
#         secure=False
#     )
#     if not minio_client.bucket_exists("profile-pics"):
#         minio_client.make_bucket("profile-pics")
    

with DAG(
    dag_id='our_first_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=days_ago(2),
    tags=['example'],
) as dag:

    # t1, t2 and t3 are examples of tasks created by instantiating operators

    t0 = BashOperator(
        task_id='create_connection_to_fs',
        bash_command='airflow connections import /opt/airflow/dags/connections/dag_1.yaml',
    )

    t1 = FileSensor(
        task_id='waiting_for_file',
        poke_interval=20,
        timeout=60*5,
        mode='reschedule',
        soft_fail=True,
        filepath=FILENAME,
        fs_conn_id='fs_default'
    )

    check_file_content = PythonOperator(
        task_id="check_file_content",
        python_callable=check_file_content,
        op_kwargs={'file_path': "/opt/airflow/raw_data/profile_data.json"},
    )

    # check_minio_bucket = PythonOperator(
    #     task_id="check_minio_bucket",
    #     python_callable=check_minio_bucket
    # )

    write_profiles = PythonOperator(
        task_id="write_profiles",
        python_callable=write_to_mongo,
        op_kwargs={'file_path': "/opt/airflow/raw_data/profile_data.json"},
    )

    write_pics_to_minio = PythonOperator(
        task_id="write_pics_to_minio",
        python_callable=write_pics_to_minio,
        op_kwargs={'file_path': "/opt/airflow/raw_data/profile_data.json"}
    )

    t4 = BashOperator(
        task_id='delete_connection_to_fs',
        bash_command='airflow connections delete fs_default',
    )

    
    t0 >> t1 >> check_file_content >> write_profiles >> write_pics_to_minio >> t4