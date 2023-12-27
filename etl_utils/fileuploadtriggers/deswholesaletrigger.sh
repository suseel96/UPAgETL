#!/bin/bash

FOLDER_TO_WATCH="/home/otsietladm/dev_file_uploads/des-wholesaleprice/pending"
PYTHON_SCRIPT="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"

while true; do
    inotifywait -e create -e moved_to -e modify "$FOLDER_TO_WATCH" 2>/dev/null |
    while read path action file; do
        python3 "$PYTHON_SCRIPT" "$file"
    done
done