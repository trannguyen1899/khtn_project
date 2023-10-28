# khtn_project
Đồ án kỹ thuật xử lý dữ liệu lớn - Xử lý và trực quan dữ liệu Biomedical Data Science

Đoàn Đức Thế Anh	- Võ Nam Thục Đoan -	Nguyễn Ngọc Bảo Trân	- Hoàng Thị Hồng Hạnh	


## Create virtual environment 
```conda create -n myenv python==3.9```

## Install required packages 
```pip install -r requirements.txt```

## Airflow Installation
1. Set Airflow Home

```export AIRFLOW_HOME=~/airflow```
   
3. Installl Airflow

```AIRFLOW_VERSION=2.7.2```

Extract the version of Python you have installed. If you're currently using a Python version that is not supported by Airflow, you may want to set this manually.
See above for supported versions.
```PYTHON_VERSION="$(python --version | cut -d " " -f 2 | cut -d "." -f 1-2)"```

CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
For example this would install 2.7.2 with python 3.8: https://raw.githubusercontent.com/apache/airflow/constraints-2.7.2/constraints-3.8.txt

```pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"```

## Initiate the project
1. Clone folder "dags" into your Airflow Home folder

2. Config your Google Cloud Storage and Big Query details in the airflow_dags.py script. Please also specify path to your credential json file in the script.

3. Run Airflow standalone
```airflow standalone```

4. Access the Airflow UI
Visit ```localhost:8080``` in your browser and log in with the admin account details shown in the terminal.

5. Find DAG named "khtn" in your DAG list and activate it

## Graph visualization by Neo4j
1. Log in Neo4j workspace by log in info file.

2. Import data into database by Cypher code

3. Use Cypher code to query and visualize graph
   ```
      Ex:   MATCH (author1:Authors)-[:Authored]->(publication:Publications)<-[:Authored]-(author2:Authors)
            WHERE author1 <> author2
            WITH author1, author2, COUNT(publication) AS num_coauthored_publications
            WHERE num_coauthored_publications >= 15
            RETURN author1, author2, num_coauthored_publications
   ```
