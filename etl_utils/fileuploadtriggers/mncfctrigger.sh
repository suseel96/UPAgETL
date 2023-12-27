#!/bin/bash

FOLDER_TO_WATCH="/home/otsietladm/dev_file_uploads/*/pending"
# FOLDER_TO_WATCH="/home/otsietladm/dev_file_uploads/mncfc/pending"
PYTHON_SCRIPT="/home/otsietladm/etl_prod_stg_scripts/mncfcETL.py"

while true; do
    inotifywait -e create -e moved_to -e modify "$FOLDER_TO_WATCH" 2>/dev/null |
    while read path action file; do
        python3 "$PYTHON_SCRIPT" "$file" &
    done
done