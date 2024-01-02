# Install Virtual Environment
sudo apt install python3.10-venv
# Install pip
sudo apt install python3-pip
# Create Virtual Environment
python3 -m venv venv
# Activate Virtual Environment
source ./venv/bin/activate # In Linux

# venv\Scripts\activate # In Windows
# Install Dependencies
venv/bin/pip install -r requirements.txt
# Install Nginix
sudo apt install nginx -y
# For Postgres
sudo apt update -y && sudo apt install -y build-essential libpq-dev
venv/bin/pip install psycopg2-binary --no-binary psycopg2-binary
