# Sales Data Airflow Pipeline

## Overview
An end-to-end pipeline using Apache Airflow to detect raw sales data landing in AWS S3 bucket from vendor, process using Spark in AWS EMR clusters, and load output parquet files into Snowflake data warehouse for data usage and analysis. 

![WeCloudDataProjects-Mini-Project-8](https://github.com/khojashaheen/Sales-Data-Airflow-Pipeline/assets/132402838/2582b5bb-238b-49fc-8dd5-5779aac68bf8)


## Pre-Requisite
- AWS account
- Snowflake (deployed on AWS preferred)

## Installation:
### Step 1. Clone the Repository:
  - Use git clone https://github.com/khojashaheen/Sales-Data-Airflow-Pipeline to clone this repository to your local machine.

### Step 2. Complete Pre-requisites Steps

### Step 3. Set up environment:
#### Step 3.1 Set up IAM role with following policies:
  - AmazonS3FullAccess
  - AmazonEMRFullAccessPolicy_v2 
#### Step 3.2 Create EC2 with Ubuntu image, and attach IAM role to it:
  - create a folder for project in EC2 instance
  - copy files 'requirements.txt' and 'ubuntu_sys_init.sh' in the project folder 
  - chmod +x ./ubuntu_sys_init.sh
  - sudo ./ubuntu_sys_init.sh
#### Step 3.3 Install Airflow:
  - curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.8.1/docker-compose.yaml'
  - mkdir -p ./dags ./logs ./plugins ./config
  - echo -e "AIRFLOW_UID=$(id -u)" > .env
  - docker-compose up airflow-init
  - docker-compose up
### Step 4. Copy Files to EC2:
  - copy file in this project 'dags/s3-emr-snowflake-dag.py' under /dags in EC2 instance
  - replace <S3_Bucket> in file in 'config/config.ini' with AWS Landing S3 Bucket, and copy under /config folder in EC2 instance
### Step 5. Create EMR Cluster:
#### Step 5.1 Create default role 'EMR_DefaultRole' if not existing:
  - aws emr create-default-roles
  - assign the following policy 'AmazonElasticMapReduceforEC2Role' to 'EMR_DefaultRole'
#### Step 5.2 Set up EMR Cluster:
  - Version: emr-6.15.0
  - Application: Spark, Hadoop, Hive
  - EC2 instance profile = Any IAM role with AmazonS3FullAccess policy
  - Service role for Amazon EMR = EMR_DefaultRole
#### Step 5.3 Wait for around 10 mins:
  - copy cluster id and replace CLUSTER_ID in the config file.

### Step 6. Set Up Snowflake:
#### 6.1 Create Snowflake Database, Schema, Table:
  - replace <S3_BUCKET>, <KEY_ID>, <SECRET_KEY> in snowflake-init.sql
  - execute snowflake-init.sql in Snowflake
  - create Warehouse
### 7 Restart Airflow:
  - docker-compose up

#### 7.1 Under Graph, the services should look as follows:
<img width="700" alt="Picture2" src="https://github.com/khojashaheen/Sales-Data-Airflow-Pipeline/assets/132402838/094b9e65-a667-4225-a644-4171d745a1be">

#### 7.2 After a successful run, sales data are aggregated into Snowflake Database:
<img width="700" alt="Picture1" src="https://github.com/khojashaheen/Sales-Data-Airflow-Pipeline/assets/132402838/58855aa5-0c6f-4880-949b-62461ae21d3d">
