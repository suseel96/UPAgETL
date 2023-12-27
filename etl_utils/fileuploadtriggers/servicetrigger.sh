#!/bin/bash

declare -A folder_mapping
folder_mapping["/home/otsietladm/dev_file_uploads/des-wholesaleprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"
folder_mapping["/home/otsietladm/dev_file_uploads/mncfc/pending"]="/home/otsietladm/etl_prod_stg_scripts/mncfcETL.py"
folder_mapping["/home/otsietladm/dev_file_uploads/des-wholesaleprice/pending"]="/home/otsietladm/etl_prod_stg_scripts/desWholesalePricesETL.py"
# Add more folder-script mappings as needed

# Start monitoring the folders
for folder in "${!folder_mapping[@]}"; do
  inotifywait -m -e create "$folder" | while read -r directory event file; do
    if [[ "$event" == "CREATE" ]]; then
      echo "File '$file' created in '$directory'"
      python "${folder_mapping[$directory]}"
    fi
  done
done