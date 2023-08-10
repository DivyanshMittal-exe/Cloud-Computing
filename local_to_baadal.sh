#!/bin/bash

# Local folder path
local_folder="/home/higgsboson/Codes/Sem7/COL733"

# Remote server details
remote_user="baadalvm"
remote_server="10.17.7.180"
remote_folder="~/"

# Rsync command to send data from local to remote
rsync -avz --exclude='twcs.csv' --exclude='Test/' "$local_folder" "$remote_user@$remote_server:$remote_folder"

