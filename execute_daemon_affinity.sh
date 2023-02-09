#!/bin/bash

# Get input arguments and store them into variables
start_date=$1
end_date=$2
s3_bucket_name=$3
sql_database_name=$4
sql_server_name=$5
sql_user_name=$6
sql_user_pwd=${DB_PASSWORD}


folder_name=/home/miner/dwh_miner/
file_name=execute_check

echo "Dynamic start date: " $start_date
echo "Dynamic end date: " $end_date

cd $folder_name
python3 dwh_processor.py $start_date $end_date $s3_bucket_name $sql_database_name $sql_server_name $sql_user_name $sql_user_pwd

#while read -r line; do
#        if [[ $line == *"$start_date"* ]]
#        then
#                echo "Execution was already completed today. Clear line to reload!"!
#        else
#                echo "Starting daily load!"
#        fi
#done < $file_name