#!/bin/bash

set -e

output_file="run.log"
rm -f "$output_file"
exec > >(tee -a "$output_file") 2>&1

sudo rm -rf /root/.cache

sudo apt update
sudo apt install python3-pip
sudo apt install virtualenv
sudo apt-get clean
sudo apt-get autoclean

sudo rm -rf venv
virtualenv venv
source venv/bin/activate

pip install -r requirements.txt

cd src

# Hugging Face Datasets

python3 download_datasets.py
python3 generate_charts_datasets.py

# Hugging Face Models

python3 download_models.py
#python3 download_params.py # Uncomment this line to retrieve the model params. Best to run this line by itself given that it is compute-intensive.
python3 generate_charts_models.py

deactivate
