# khtn_project
Đồ án kỹ thuật xử lý dữ liệu lớn - Xử lý và trực quan dữ liệu Biomedical Data Science

## Create virtual environment 
conda create -n myenv python==3.9

## Airflow Installation
1. Set Airflow Home
   ```export AIRFLOW_HOME=~/airflow```
   
3. Installl Airflow

AIRFLOW_VERSION=2.7.2

Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
See above for supported versions.
PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
For example this would install 2.7.2 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

## Initiate the project
1. Clone .py files into folder dags of your Airflow Home folder

2. Config your Google Cloud Storage and Big Query details in the airflow_dags.py script. Please also specify path to your credential json file in the script.

3. Run Airflow standalone
airflow standalone

4. Access the Airflow UI
Visit localhost:8080 in your browser and log in with the admin account details shown in the terminal.

5. Find DAG named "khtn" in your DAG list and activate it
