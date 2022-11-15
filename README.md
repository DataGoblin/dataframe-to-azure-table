# dataframe-to-azure-table
Concept of how to batch upsert dataframe to azure table storage

It takes your current Dataframe and batch upserts the rows into a azure table. Only tested on a dataset of less than 10 thousand rows. 

Batch operations only work when uploading data with the same partition key
