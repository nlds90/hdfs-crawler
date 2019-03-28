from pyarrow import hdfs
from pyarrow import csv
from pyarrow import parquet
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
    print("GRAPHQL DATASET", dataset)
    return dataset.get("data").get("datasetUpsertFromStorage").get("id");

def graphql(query, variables):
    payload = {
        "query": query,
        "variables": variables
    }

    return requests.post(HASBRAIN_URL, json=payload)

def index_dataset(dataset_id, dataset):
    for file in fs.ls(dataset):
        if fs.isfile(file):
            file_name, file_ext = os.path.splitext(file)
            if file_ext == ".csv":
                print("INDEX CSV", file)
                index_csv(dataset_id, file)
            elif file_ext == ".parquet":
                print("INDEX PARQUET", file)
                index_parquet(dataset_id, file)
            else:
                print("Unsupported File!")
        else:
            print("TRY TO READ PARQUET DIR")
            try:
                index_parquet_dir(dataset_id, file)
            except:
                index_dataset(dataset_id, file)


def index_csv(dataset_id, file):
    try:
        with fs.open(file) as csv_file:
            table = csv.read_csv(csv_file)
            csvHeader = [{ "name": column.name } for column in table.columns]
            create_file({ "datasetId": dataset_id, "pathFromRoot": file, "csvHeader": csvHeader })
    except Exception as e:
        print(e)

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
        "record": { "csvHeader": record.get("csvHeader") }
    }

    r = graphql(query, variables)
    print("GRAPHQL FILE", r.json())

def getTypeFromPhysicalType(dtype):
    type_map = {
        'INT64': 'INTEGER',
        'INT32': 'INTEGER',
        'INT96': 'INTEGER',
        'INT8': 'INTEGER',
        'FLOAT': 'DECIMAL',
        'DOUBLE': 'DECIMAL',
        'FLOAT': 'DECIMAL',
        'BOOLEAN': 'BOOLEAN',
        'DATE': 'DATE',
    }
    known_type = type_map.get(str(dtype))
    return known_type if known_type else 'STRING'

def getTypeFromLogicalType(dtype):
    type_map = {
       # 'UTF8': 'STRING',
        'UINT_8': 'INTEGER',
        'UINT_16': 'INTEGER',
        'UINT_32': 'INTEGER',
        'UINT_64': 'INTEGER',
        'INT_8': 'INTEGER',
        'INT_16': 'INTEGER',
        'INT_32': 'INTEGER',
        'INT_64': 'INTEGER',
        'DECIMAL': 'DECIMAL',
        'DATE': 'DATE',
    }
    known_type = type_map.get(str(dtype))
    return known_type if known_type else 'STRING'

def getType(physical_type, logical_type):
    if (logical_type == 'NONE'):
        return getTypeFromPhysicalType(physical_type)
    else:
        return getTypeFromLogicalType(logical_type)

def index_parquet_dir(dataset_id, dir):
    dataset = parquet.ParquetDataset(dir)
    metadata = {
        'num_rows': dataset.metadata.num_rows,
        'num_row_groups': dataset.metadata.num_row_groups,
        'serialized_size': dataset.metadata.serialized_size
    }
    print(metadata)
    parquet_header = []
    for item in dataset.schema:
        parquet_header.append({
            'name': item.name,
            'type': getType(item.physical_type, item.logical_type)
        })
        # physical_type logical_type
    print(parquet_header)

def index_parquet(dataset_id, file):
    parquet_file = parquet.ParquetFile(file)
    metadata = {
        'num_rows': parquet_file.metadata.num_rows,
        'num_row_groups': parquet_file.metadata.num_row_groups,
        'serialized_size': parquet_file.metadata.serialized_size
    }
    print(metadata)
    parquet_header = []
    for item in parquet_file.schema:
        parquet_header.append({
            'name': item.name,
            'type': getType(item.physical_type, item.logical_type)
        })
        # physical_type logical_type
    print(parquet_header)

scan()

