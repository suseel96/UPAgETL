#!/bin/bash

declare -A folder_script_map
folder_script_map["/home/otsietladm/dev_file_uploads/des-wholesaleprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"
folder_script_map["/home/otsietladm/dev_file_uploads/mncfc/pending"]="/home/otsietladm/etl_prod_stg_scripts/mncfcETL.py"
folder_script_map["/home/otsietladm/dev_file_uploads/des-wholesaleprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"

while true; do
    file=$(inotifywait -e create --format '%f' -r -q --exclude '.*_processing' /path/to 2>/dev/null)
    if [ -n "$file" ]; then
        for folder in "${!folder_script_map[@]}"; do
            if [[ $file == $folder* ]]; then
                echo "File $file was uploaded to folder $folder. Triggering Python script..."
                python3 "${folder_script_map[$folder]}" "$file"
                break
            fi
        done
    fi
done