## DWH MINER
The in-house developed Python app for Extracting DWH data to AWS in parquet format 

## Core modules
* Connect :
    * MS SQL : based on pyodbc
    * AWS S3 : based on boto3
* Extractor : SQL Data extractor and write to parquet
* Queries : SQL Queries as python param methods

## News
Contact the author

## Dockerfile
* Packages being installed:
    * apt-utils
    * unzip
    * p7zip
    * unixodbc
    * unixodbc-dev
    * gcc
    * python3
    * python3-dev
    * curl
    * apt-transport-https
    * ca-certificates
    * msodbcsql17
    * python3-pip
    * pyodbc
    * boto3
    * pandas
    * pyarrow
    * sqlalchemy
* How to test the image before going live in production:
    * Change ~/bridget/dwh_miner/game_affinity_miner.py to point to the staging S3 bucket.
    * Build the image and tag it with stg-$VERSION and push the image to the AWS ECR repo 845432982891.dkr.ecr.eu-west-1.amazonaws.com/dwh_miner (you can use the script /bd-ml/sys/eks/dwh_miner/push-image.sh to do that).
    * Make sure the GA Pipeline is not running on Airflow Prod.
    * Run the DAG game_affinity_source_data from Airflow Prod after updating the DOCKER_IMAGE parameter in live.py to point to the new image.
