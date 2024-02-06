from datetime import datetime, timedelta
import configparser
import os

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.operators.python import PythonOperator,BranchPythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


# Following are defaults which can be overridden later on
default_args = {
    'owner': 'Shaheen Khoja',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(days=1)
}


config = configparser.ConfigParser()
config_path = os.getcwd() + '/config/config.ini'
config.read(config_path)
my_bucket = config.get('AWS', 's3_bucket')
my_emr_cluster = config.get('AWS', 'emr_cluster')

SPARK_STEPS = [
    {
        "Name": "sales_processing",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/bin/spark-submit",
                "--master",
                "yarn",
                #    "--deploy-mode",
                #    "cluster/client",
                #    "--num-executors",
                #    "2",
                #    "--driver-memory",
                #    "512m",
                #    "--executor-memory",
                #    "3g",
                #    "--executor-cores",
                #    "2",
                f"s3://{my_bucket}/pyspark_sales.py",
                "{{  macros.ds_format(ds,'%Y-%m-%d','%Y%m%d')  }}",
                f"s3://{my_bucket}/" + "{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m/%d') }}/",
                f"{my_bucket}",
            ],
        },
    }
]



dag = DAG('s3-emr-snowflake', default_args=default_args,schedule_interval="@daily",catchup=False)


def check_s3_file_exist(ti=None):
    xcom_value = ti.xcom_pull(task_ids="list_objects")
    if xcom_value:
        return "emr_process_file"
    else:
        return "notify_vendor"

def notify_vendor():
    print("There are no files on S3")

def emr_process_file():
    return "Processing Files"

# Task to list objects in an S3 bucket with a specified prefix.
# The result is stored in XCom, which is used in the subsequent task to determine whether files exist.

task_list_s3 = S3ListOperator(
    task_id="list_objects",
    bucket=my_bucket,
    prefix="{{ macros.ds_format(ds,'%Y-%m-%d', '%Y/%m/%d') }}",
    delimiter=''
)


# A Python callable (_branch_func) is executed to check if files exist based on the information retrieved in the previous task.
# If files exist, it branches to the task emr_process_file; otherwise, it goes to notify_vendor.

task_check_s3_file_exist = BranchPythonOperator(
    task_id='check_s3_file_exist',
    python_callable=check_s3_file_exist,
    dag=dag)

# Placeholder task to notify vendors about missing files
task_notify_vendor = PythonOperator(
    task_id='notify_vendor',
    python_callable=notify_vendor,
    dag=dag)

# Placeholder task to echo "processing files"
task_emr_process_file = PythonOperator(
    task_id='emr_process_file',
    python_callable=emr_process_file,
    dag=dag)



# Adds Spark steps to an existing EMR cluster (CLUSTER_ID) for processing data.
# The Spark steps are defined in the SPARK_STEPS variable.
add_step_job = EmrAddStepsOperator(
    task_id="add_step_job",
    # job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster')['JobFlowId'] }}",
    job_flow_id=my_emr_cluster,
    steps=SPARK_STEPS,
    dag=dag,
)


# Wait for first Spark Jar step to complete in the previous step to complete before proceeding.
wait_for_step_job = EmrStepSensor(
    task_id="wait_for_step_job",
    # job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster')['JobFlowId'] }}",
    job_flow_id=my_emr_cluster,
    step_id="{{ task_instance.xcom_pull(task_ids='add_step_job')[0] }}",
    dag=dag,
)

# Loads data into Snowflake using a SQL script (sf_load.sql) in the sales_db database and sales_schema schema.
load_snowflake_task = SnowflakeOperator(
    task_id="load_snowflake",
    database="sales_db",
    schema="sales_schema",
    sql="snowflake_load.sql",
    dag=dag
)



(task_list_s3 >> task_check_s3_file_exist >> [task_notify_vendor, task_emr_process_file])

(task_emr_process_file >> add_step_job >> wait_for_step_job >> load_snowflake_task)
