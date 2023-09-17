#!/bin/bash

# Local folder path
local_folder="/home/higgsboson/Codes/Sem7/COL733"

# Remote server details
remote_user="baadalvm"
remote_server="10.17.7.180"
remote_folder="~/"

# Copy "logs.csv" from remote to local
scp "$remote_user@$remote_server:$remote_folder/COL733/logs.csv" "$local_folder"

# Check if the copy was successful
if [ $? -eq 0 ]; then
    echo "File 'logs.csv' copied successfully from remote to local."
else
    echo "Failed to copy 'logs.csv' from remote to local."
fi

scp "$remote_user@$remote_server:$remote_folder/COL733/serial_logs.csv" "$local_folder"

# Check if the copy was successful
if [ $? -eq 0 ]; then
    echo "File 'serial_logs.csv' copied successfully from remote to local."
else
    echo "Failed to copy 'serial_logs.csv' from remote to local."
fi
