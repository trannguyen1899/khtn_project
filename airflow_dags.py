import os
os.environ['AIRFLOW_HOME'] = '~/airflow'

from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator

import json
import pandas as pd
from google.oauth2 import service_account
from google.auth.transport.requests import Request
from google.cloud import storage
from google.cloud import bigquery
from io import StringIO

from crawl_scripts.main_crawler import main_crawler
from crawl_scripts.monthly_crawler import monthly_crawler
from crawl_scripts.rerun_error import rerun_error
from crawl_scripts.utils import to_json

import re


def crawl_data():
    # Define the path to your service account key JSON file.
    json_key_path = '/mnt/d/theta-voyager-402017-11cc1db952d3.json'

    # Load the service account key JSON from the file.
    credentials = service_account.Credentials.from_service_account_file(
        json_key_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    storage_client = storage.Client(credentials=credentials, project="theta-voyager-402017")

    main_crawler()
    while percent_err > 5:
        percent_err = rerun_error()
    path = to_json()

    # Bucket
    bucket = storage_client.get_bucket("khtn")
    blob = bucket.blob('data.json')
    blob.upload_from_filename(path)

def cleaning_crawled_data():
      # Define the path to your service account key JSON file.
    json_key_path = '/mnt/d/theta-voyager-402017-11cc1db952d3.json'

    # Load the service account key JSON from the file.
    credentials = service_account.Credentials.from_service_account_file(
        json_key_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    storage_client = storage.Client(credentials=credentials, project="theta-voyager-402017")

    # Bucket
    bucket = storage_client.get_bucket("khtn")

    # Read the JSON data from the blob
    blob = bucket.blob('data.json')
    json_data = blob.download_as_text()
    data = json.loads(json_data)

    # Create a DataFrame
    df = pd.DataFrame(data, columns=["PMID", "Data"])
    df1 = df[['PMID']]
    # Extract relevant information and create a new DataFrame
    df1["Title"] = df["Data"].apply(lambda x: x["title"] if x["title"] else None)
    df1["Authors_apa_name"] = df["Data"].apply(lambda x: '; '.join([author["apa_name"] for author in x["authors"]]) if x["authors"] else None)
    df1["Authors_full_name"] = df["Data"].apply(lambda x: '; '.join([author["last_name"] + " " + author["fore_name"] for author in x["authors"]]) if x["authors"] else None)
    df1["Affiliate"] = df["Data"].apply(lambda x: '; '.join([author["affiliate"] if author.get("affiliate") else '' for author in x["authors"]]) if x["authors"] else None)
    df1["Keywords"] = df["Data"].apply(lambda x: '; '.join([kw for kw in x["keywords"]]) if x["keywords"] else None)
    df1["Journal"] = df["Data"].apply(lambda x: x["journal"] if x["journal"] else None)
    df1["JournalAbrev"] = df["Data"].apply(lambda x: x["journal_abrev"] if x["journal_abrev"] else None)
    df1["Volume"] = df["Data"].apply(lambda x: x["volume"] if x["volume"] else None)
    df1["Year"] = df["Data"].apply(lambda x: x["year"] if x["year"] else None)
    df1["Month"] = df["Data"].apply(lambda x: x["month"] if x["month"] else None)
    df1["ReferenceIDList"] = df["Data"].apply(lambda x: '; '.join([ref for ref in x["reference_id_list"]]) if x["reference_id_list"] else None)
    df1["NumberOfCitation"] = df["Data"].apply(lambda x: x["num_cite"] if x["num_cite"] else None)

    # DROP NA 
    df1.dropna(subset=['Journal'], inplace=True)
    df1 = df1.dropna(subset=['Keywords'])

    # DROP DUPLICATE
    df1 = df1.drop_duplicates()
    df1 = df1.reset_index(drop=True)

    # CLEANING
    df1["Title"] = df1["Title"].str.replace('\n', '')
    df1["Keywords"] = df1["Keywords"].str.replace('\n', '')
    df1["Affiliate"] = df1["Affiliate"].str.replace('\n', '')
    df1["Authors_full_name"] = df1["Authors_full_name"].str.replace('\n', '')
    df1["Authors_apa_name"] = df1["Authors_apa_name"].str.replace('\n', '')
    df1["Affiliate"] = df1["Affiliate"].str.replace('\t', '')
    df1["Keywords"] = df1["Keywords"].str.replace('\t', '')
    df1['Keywords'] = df1['Keywords'].str.replace('"', '')
    df1['Keywords'] = df1['Keywords'].str.replace('#', '')
    df1["Authors_full_name"] = df1["Authors_full_name"].str.replace('\t', '')
    df1["Authors_apa_name"] = df1["Authors_apa_name"].str.replace('\t', '')
    df1["Title"] = df1["Title"].str.replace('\t', '')

    # df1['NumberOfCitation'].astype(int)
    df1['NumberOfCitation'] = pd.to_numeric(df1['NumberOfCitation'], errors='coerce')
    df1['NumberOfCitation'] = df1['NumberOfCitation'].fillna(0).astype(int)

    df1['Year'] = pd.to_numeric(df1['Year'], errors='coerce')
    df1['Year'] = df1['Year'].astype(int)

    df1['Month'] = pd.to_numeric(df1['Month'], errors='coerce')
    df1['Month'] = df1['Month'].astype(int)

    # to_csv
    csv_file_path = '/home/trannguyen/airflow/paper.csv'
    df1.to_csv(csv_file_path, index=False)

    # Upload the local file to GCS
    destination_blob_name = 'paper.csv'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_file_path)

def get_paper_rank():
    json_key_path = '/mnt/d/theta-voyager-402017-11cc1db952d3.json'

    # Load the service account key JSON from the file.
    credentials = service_account.Credentials.from_service_account_file(
        json_key_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    bq_client = bigquery.Client(credentials=credentials, project="theta-voyager-402017")
    storage_client = storage.Client(credentials=credentials, project="theta-voyager-402017")
    # Bucket
    bucket = storage_client.get_bucket("khtn")

    # Medline csv
    blob = bucket.blob("Medline.csv")
    csv_data = blob.download_as_text()
    Medline = pd.read_csv(StringIO(csv_data))

    # journal_rank csv
    blob = bucket.blob("journal_rank.csv")
    csv_data = blob.download_as_text()
    journal = pd.read_csv(StringIO(csv_data))

    # paper csv
    blob = bucket.blob("paper.csv")
    csv_data = blob.download_as_text()
    paper = pd.read_csv(StringIO(csv_data))

    merged_df = pd.merge(paper, Medline, on='JournalAbrev', how='left')
    merged_df['ISSN (Print)'] = merged_df['ISSN (Print)'].replace(r'^\s*$', None, regex=True)
    merged_df['ISSN (Online)'] = merged_df['ISSN (Online)'].replace(r'^\s*$', None, regex=True)
    
    merged_df['ISSN (Print)'].fillna('None', inplace=True)
    merged_df['ISSN (Online)'].fillna('None', inplace=True)
    merged_df['Rank'] = 'NotFound'

    def find_and_transfer_rank(row):
        code1 = row['ISSN (Print)']
        code2 = row['ISSN (Online)']
        for idx, issn in enumerate(journal['Issn']):
            if (code1 in issn) or (code2 in issn):
                return journal.loc[idx, 'SJR Best Quartile']
        return row['Rank']  # Default to existing value in "Rank2" column

    merged_df['Rank'] = merged_df.apply(find_and_transfer_rank, axis=1)
    merged_df['Rank'] = merged_df['Rank'].str.replace('-', 'Unranked')

    merged_df = merged_df[merged_df['Rank']!='NotFound']
    merged_df = merged_df[merged_df['Affiliate']!='']

    # to_csv
    csv_file_path = '/home/trannguyen/airflow/ranked_paper.csv'
    merged_df.to_csv(csv_file_path, index=False)

    # Upload the local file to GCS
    destination_blob_name = 'ranked_paper.csv'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_file_path)

    # Bigquery info
    project_id = 'theta-voyager-402017'
    dataset_id = 'khtn_bigdata'
    table_id = 'papers'


    # Get the BigQuery dataset and table
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    # table = bq_client.get_table(table_ref)

    # Specify the GCS URI of the CSV file
    gcs_uri = 'gs://khtn/ranked_paper.csv'

    # Load data from GCS into the BigQuery table
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip the header row if present
    # job_config.autodetect = True  # Automatically detect schema
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for the job to complete

def get_keyword_year():
    def remove_numbers_in_parentheses(input_string):
        if isinstance(input_string, str):
            pattern = r'\([^)]*\)'
            result = re.sub(pattern, '', input_string)
            return result
        else:
            return input_string

    json_key_path = '/mnt/d/theta-voyager-402017-11cc1db952d3.json'

    # Load the service account key JSON from the file.
    credentials = service_account.Credentials.from_service_account_file(
        json_key_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    bq_client = bigquery.Client(credentials=credentials, project="theta-voyager-402017")
    storage_client = storage.Client(credentials=credentials, project="theta-voyager-402017")
    
    # Bucket
    bucket = storage_client.get_bucket("khtn")

    # Ranked paper csv
    blob = bucket.blob("ranked_paper.csv")
    csv_data = blob.download_as_text()
    ranked_paper = pd.read_csv(StringIO(csv_data))

    ranked_paper['Keywords'] = ranked_paper['Keywords'].apply(remove_numbers_in_parentheses)

    ranked_paper['keyword_array'] = ranked_paper['Keywords'].str.lower().str.split('; ')
    key = ranked_paper[['Year', 'keyword_array']].explode('keyword_array')
    key['keyword_array'] = key['keyword_array'].str.strip()
    key.rename(columns={'keyword_array': 'Keywords'}, inplace=True)

    # to_csv
    csv_file_path = '/home/trannguyen/airflow/keywords.csv'
    key.to_csv(csv_file_path, index=False)

    # Upload the local file to GCS
    destination_blob_name = 'keywords.csv'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_file_path)

    # Bigquery info
    project_id = 'theta-voyager-402017'
    dataset_id = 'khtn_bigdata'
    table_id = 'keywords'

    # Get the BigQuery dataset and table
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    # table = bq_client.get_table(table_ref)

    # Specify the GCS URI of the CSV file
    gcs_uri = 'gs://khtn/keywords.csv'

    # Load data from GCS into the BigQuery table
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip the header row if present
    # job_config.autodetect = True  # Automatically detect schema
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

    load_job.result()  # Wait for the job to complete

def get_author():
    json_key_path = '/mnt/d/theta-voyager-402017-11cc1db952d3.json'

    # Load the service account key JSON from the file.
    credentials = service_account.Credentials.from_service_account_file(
        json_key_path,
        scopes=['https://www.googleapis.com/auth/cloud-platform']
    )

    bq_client = bigquery.Client(credentials=credentials, project="theta-voyager-402017")
    storage_client = storage.Client(credentials=credentials, project="theta-voyager-402017")

    # Bucket
    bucket = storage_client.get_bucket("khtn")

    # Ranked paper csv
    blob = bucket.blob("ranked_paper.csv")
    csv_data = blob.download_as_text()
    ranked_paper = pd.read_csv(StringIO(csv_data))

    # Read the JSON data from the blob
    blob = bucket.blob("sample.json")
    json_data = blob.download_as_text()
    data = json.loads(json_data)

    author_data = []

    # Iterate through the data and extract author information
    for entry in data:
        authors = entry[1]['authors']
        for author in authors:
            author_info = {
                "PMID": entry[0],
                "Author_LastName": author['last_name'],
                "Author_ForeName": author['fore_name'],
                # "Author_Initials": author['initials'],
                "Author_APA_Name": author['apa_name'],
                "Author_Affiliate": author['affiliate']
            }
            author_data.append(author_info)

    # Create a DataFrame from the author data
    author_df = pd.DataFrame(author_data)
    author_df['Author_Affiliate'].fillna('None', inplace=True)
    author_df['Author_ForeName'].fillna('None', inplace=True)
    author_df['Author_LastName'].fillna('None', inplace=True)
    author_df['Author_APA_Name'].fillna('None', inplace=True)

    author_df['Author_Affiliate'] = author_df['Author_Affiliate'].str.replace("\n", "")
    author_df['Author_Affiliate'] = author_df['Author_Affiliate'].str.replace("\t", "")
    author_df['Author_ForeName'] = author_df['Author_ForeName'].str.replace("\n", "")
    author_df['Author_ForeName'] = author_df['Author_ForeName'].str.replace("\t", "")
    author_df['Author_LastName'] = author_df['Author_LastName'].str.replace("\n", "")
    author_df['Author_LastName'] = author_df['Author_LastName'].str.replace("\t", "")
    author_df['Author_APA_Name'] = author_df['Author_APA_Name'].str.replace("\n", "")
    author_df['Author_APA_Name'] = author_df['Author_APA_Name'].str.replace("\t", "")

    author_df["Authors_full_name"] = author_df["Author_LastName"] + " " + author_df["Author_ForeName"] 

    def extract_country_or_city(workplace_str):
        country = ['Aruba', 'Afghanistan', 'Angola', 'Anguilla', 'Åland Islands', 'Albania', 'Andorra', 'United Arab Emirates', 'Argentina', 'Armenia', 'American Samoa', 'Antarctica', 'Antigua and Barbuda',
    'Australia', 'Austria', 'Azerbaijan', 'Burundi', 'Belgium', 'Benin', 'Sint Eustatius and Saba Bonaire', 'Burkina Faso', 'Bangladesh', 'Bulgaria', 'Bahrain', 'Bahamas', 'Bosnia and Herzegovina', 'Saint Barthélemy', 'Belarus', 'Belize',
    'Bermuda', 'Plurinational State of Bolivia', 'Brazil', 'Barbados', 'Brunei Darussalam', 'Bhutan', 'Bouvet Island', 'Botswana', 'Central African Republic', 'Canada', 'Cocos (Keeling) Islands', 'Switzerland', 'Chile', 'China',
    "Côte d'Ivoire", 'Cameroon', 'The Democratic Republic of the Congo', 'Congo', 'Cook Islands', 'Colombia', 'Comoros', 'Cabo Verde', 'Costa Rica', 'Cuba', 'Curaçao', 'Christmas Island', 'Cayman Islands', 'Cyprus', 'Czechia', 'Germany',
    'Djibouti', 'Dominica', 'Denmark', 'Dominican Republic', 'Algeria', 'Ecuador', 'Egypt', 'Eritrea', 'Western Sahara', 'Spain', 'Estonia', 'Ethiopia', 'Finland', 'Fiji', 'Falkland Islands (Malvinas)', 'France', 'Faroe Islands', 'Federated States of Micronesia',
    'Gabon', 'United Kingdom', 'Georgia', 'Guernsey', 'Ghana', 'Gibraltar', 'Guinea', 'Guadeloupe', 'Gambia',
    'Guinea-Bissau', 'Equatorial Guinea', 'Greece', 'Grenada', 'Greenland', 'Guatemala', 'French Guiana', 'Guam', 'Guyana', 'Hong Kong', 'Heard Island and McDonald Islands', 'Honduras',
    'Croatia', 'Haiti', 'Hungary', 'Indonesia', 'Isle of Man', 'India', 'British Indian Ocean Territory', 'Ireland', 'Islamic Republic of Iran', 'Iraq', 'Iceland', 'Israel', 'Italy', 'Jamaica', 'Jersey', 'Jordan', 'Japan', 'Kazakhstan', 'Kenya',
    'Kyrgyzstan', 'Cambodia', 'Kiribati', 'Saint Kitts and Nevis', 'Republic of Korea', 'Kuwait', "Lao People's Democratic Republic", 'Lebanon', 'Liberia', 'Libya', 'Saint Lucia',
    'Liechtenstein', 'Sri Lanka', 'Lesotho', 'Lithuania', 'Luxembourg', 'Latvia', 'Macao', 'Saint Martin (French part)', 'Morocco', 'Monaco', 'Republic of Moldova', 'Madagascar', 'Maldives', 'Mexico', 'Marshall Islands', 'North Macedonia',
    'Mali', 'Malta', 'Myanmar', 'Montenegro', 'Mongolia', 'Northern Mariana Islands', 'Mozambique', 'Mauritania', 'Montserrat', 'Martinique', 'Mauritius', 'Malawi', 'Malaysia',
    'Mayotte', 'Namibia', 'New Caledonia', 'Niger', 'Norfolk Island', 'Nigeria', 'Nicaragua', 'Niue', 'Netherlands', 'Norway', 'Nepal', 'Nauru', 'New Zealand',
    'Oman', 'Pakistan', 'Panama', 'Pitcairn', 'Peru', 'Philippines', 'Palau', 'Papua New Guinea', 'Poland', 'Puerto Rico', "Democratic People's Republic of Korea",
    'Portugal', 'Paraguay', 'State of Palestine', 'French Polynesia', 'Qatar', 'Réunion', 'Romania', 'Russia', 'Rwanda', 'Saudi Arabia', 'Sudan', 'Senegal', 'Singapore', 'South Georgia and the South Sandwich Islands',
    'Ascension and Tristan da Cunha Saint Helena', 'Svalbard and Jan Mayen', 'Solomon Islands', 'Sierra Leone', 'El Salvador', 'San Marino', 'Somalia', 'Saint Pierre and Miquelon',
    'Serbia', 'South Sudan', 'Sao Tome and Principe', 'Suriname', 'Slovakia', 'Slovenia', 'Sweden', 'Eswatini', 'Sint Maarten (Dutch part)', 'Seychelles', 'Syrian A', 'Iran',
    'Korea', 'US', 'UK', 'United States', 'Taiwan']
        nation = None
        if workplace_str:
            for c in country:
                if c in workplace_str:
                    nation = c

        if nation:
            return nation
        else:
            return 'N/A'

    author_df['Country'] = author_df['Author_Affiliate'].apply(extract_country_or_city)
    author_df['Country'] = author_df['Country'].replace('US', 'USA')
    author_df['Country'] = author_df['Country'].replace('United Kingdom', 'UK')
    author_df['Country'] = author_df['Country'].replace('United States', 'USA')
    author_df['Country'] = author_df['Country'].replace('United States of America', 'USA')
    author_df['Country'] = author_df['Country'].replace('Korea', 'Republic of Korea')
    author_df['PMID'] = author_df['PMID'].astype(int)

    author_df = author_df[author_df['PMID'].isin(ranked_paper['PMID'].values)]
    author_df['Author_id_pre'] = author_df.groupby(by=['Authors_full_name', 'Country']).ngroup()


    def fill_id(row):
        id = str(row)
        new_id = 'AU' + '0' * (7 - len(id)) + id
        return new_id
    author_df['Author_id'] = author_df['Author_id_pre'].apply(fill_id)
    author_df.drop(columns=['Author_id_pre'])

    # to_csv
    csv_file_path = '/home/trannguyen/airflow/authors.csv'
    author_df.drop(columns=['Author_id_pre']).to_csv(csv_file_path, index=False)
    print(author_df.head(5))

    # Upload the local file to GCS
    destination_blob_name = 'authors.csv'
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(csv_file_path)

    # Bigquery info
    project_id = 'theta-voyager-402017'
    dataset_id = 'khtn_bigdata'
    table_id = 'author'

    # Get the BigQuery dataset and table
    dataset_ref = bq_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    # table = bq_client.get_table(table_ref)

    # Specify the GCS URI of the CSV file
    gcs_uri = 'gs://khtn/authors.csv'

    # Load data from GCS into the BigQuery table
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.skip_leading_rows = 1  # Skip the header row if present
    # job_config.autodetect = True  # Automatically detect schema
    load_job = bq_client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

    load_job.result()  # Wait for the job to complete

with DAG(
    "KHTN",
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        "depends_on_past": False,
        # "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "schedule_interval": '@monthly',
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function, # or list of functions
        # 'on_success_callback': some_other_function, # or list of functions
        # 'on_retry_callback': another_function, # or list of functions
        # 'sla_miss_callback': yet_another_function, # or list of functions
        # 'trigger_rule': 'all_success'
    },
    description="KHTN",
    schedule=timedelta(days=30.44),
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=["project"],

) as dag:
    
    t1 = PythonOperator(
        task_id='crawl_data_and_upload_to_gcs',
        python_callable=crawl_data,
    )

    t2 = PythonOperator(
        task_id='clean_data',
        python_callable=cleaning_crawled_data,
    )

    t3 = PythonOperator(
        task_id='create_ranked_paper_table',
        python_callable=get_paper_rank,
    )

    t4 = PythonOperator(
        task_id='create_keyword_table',
        python_callable=get_keyword_year,
    )

    t5 = PythonOperator(
        task_id='create_author_table',
        python_callable=get_author,
    )

    # t1 >> t2 >> [t3, t4, t5]

    t1.set_downstream(t2)
    t2.set_downstream(t3)
    t3.set_downstream(t4)
    t4.set_downstream(t5)