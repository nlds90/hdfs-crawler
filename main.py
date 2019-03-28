from pyarrow import hdfs
from pyarrow import csv
import requests
import os

HASBRAIN_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ3b3Jrc3BhY2VJZCI6IjVhZDg1ODI0YjNjZWMzNDE1MzA0YWViNiIsImlhdCI6MTU1MzEwMTM0NX0.SzJ4BLuob2RwYbMKl1Wi1TNZ4H8F6_EPLI3r_RLZ_ok"
HASBRAIN_URL = "http://localhost:3000/apollo"
HDFS_URL = "hdfs://localhost:8020"
fs = hdfs.connect(HDFS_URL)

def scan():
    for dataset in fs.ls("/"):
        if fs.isdir(dataset):
            print("CREATE DATASET", dataset)
            dataset_id = create_dataset(dataset)

            if (dataset_id):
                index_dataset(dataset_id, dataset)

def create_dataset(dataset_path):
    query = '''
        mutation datasetUpsertFromStorage($record: DatasetRecordInputType!, $token: String!, $upsert: DatasetUpsertInputType!) {
            datasetUpsertFromStorage(token: $token, upsert: $upsert, record: $record) {
                id
            }
        }
    '''

    variables = {
        "token": HASBRAIN_TOKEN,
        "upsert": { "path": dataset_path },
        "record": { "title": dataset_path }
    }

    r = graphql(query, variables)
    dataset = r.json()
    return dataset.get("data", {}).get("datasetUpsertFromStorage", {}).get("id");

def graphql(query, variables):
    payload = {
        "query": query,
        "variables": variables
    }

    return requests.post(HASBRAIN_URL, json=payload)

def index_dataset(dataset_id, dataset):
    for file in fs.ls(dataset):
        if fs.isfile(file):
            index_file(dataset_id, file)
        else:
            index_dir(dataset_id, file)

def index_file(dataset_id, file):
    try:
        file_name, file_ext = os.path.splitext(file)
        if file_ext == ".csv":
            print("INDEX CSV", file)
            index_csv(dataset_id, file)
        elif file_ext == ".parquet":
            print("INDEX PARQUET", file)
            index_parquet(dataset_id, file)
        else:
            print("UNSUPPORTED FILE", file)
            print('============')
    except Exception as e:
        print('ERROR:', e)
        print('============')

def index_dir(dataset_id, directory):
    try:
        print("INDEX DIR", directory)
        index_parquet(dataset_id, directory)
    except Exception as e:
        print('ERROR:', e)
        print('============')
        index_dataset(dataset_id, directory)

def index_csv(dataset_id, file):
    with fs.open(file) as csv_file:
        table = csv.read_csv(csv_file)
        csv_header = [{ "name": column.name, "description": str(column.type) } for column in table.columns]
        create_file({ "datasetId": dataset_id, "pathFromRoot": file, "csvHeader": csv_header })

def create_file(record):
    query = '''
        mutation fileUpsertFromStorage($record: FileRecordInputType!, $token: String!, $upsert: FileUpsertInputType!) {
            fileUpsertFromStorage(token: $token, upsert: $upsert, record: $record) {
                id
                name
            }
        }
    '''
    variables = {
        "token": HASBRAIN_TOKEN,
        "upsert": { "datasetId": record.get("datasetId"), "pathFromRoot": record.get("pathFromRoot") },
        "record": { "csvHeader": record.get("csvHeader"), "metadata": record.get("metadata") }
    }

    return graphql(query, variables)

def index_parquet(dataset_id, file):
    table = fs.read_parquet(file)
    parquet_header = [{ "name": column.name, "description": str(column.type) } for column in table.columns]
    create_file({ "datasetId": dataset_id, "pathFromRoot": file, "metadata": parquet_header })

scan()

