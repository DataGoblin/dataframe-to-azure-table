import pandas as pd
import math
from time import sleep
from azure.data.tables import TableClient, TableTransactionError
from azure.core.exceptions import ResourceExistsError

connection_string = 'your_connection_string'
table_name = 'TestTable'

# load your own parquet file to test or drop into a script. 
df = pd.read_parquet('test.parquet').drop_duplicates()

# creating the partition and row keys.
# Partition key can only be one value when performing batch operations
# RowKey needs to be unique.
df.insert(0, 'PartitionKey', 'MyPartition')
df.insert(1, 'RowKey', df.index)


data = df.to_dict(orient='records')

# batch upserts records from dataframe complying with the limit of 50
for x in range(math.ceil(len(data) / 50)):

    rows = x*50
    operations = []
    entity_count = 0

    for index, row in enumerate(data[rows:rows+50]):
        entity_count = entity_count + 1
        operations.append(("upsert", row, {"mode": "replace"}))

        if entity_count == len(data[rows:rows+50]):
            entity_count = 0
        with TableClient.from_connection_string(conn_str=connection_string, table_name=table_name) as table_client:  
            try:
                table_client.submit_transaction(operations)
            except TableTransactionError as e:
                print("There was an error with the transaction operation")
                print(e)
