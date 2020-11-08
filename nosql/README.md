## Data modelling with Cassandra Query Language (CQL). NoSQL/Apache Cassandra training.
### Database schema and ETL pipeline for Sparkify.

The project involves creating a NoSQL database for ***Sparkify***.   

A startup called ***Sparkify*** wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analysis team is particularly interested in understanding what songs users are listening to.
The aim of the project was to act as a data engineer to create an Apache Cassandra database which can create queries on song play data to answer the questions. 

#### Data 
The data was suplied in form a directory of CSV files partitioned by date. An example raw data file is provided in the even_data folder and the screenshot of the denormalized dataset provided in the `nosql_modelling.ipynb` notebook.  

#### Apache Cassandra Tables Modelling and ETL Pipeline

The data modelling process is described in the `nosql_modelling.ipynb` notebook. 
The main steps are:
1. Creation of the denormalised dataset combining several csv files.
2. Creating Cassandra instance.
3. Creating and setting up the keyspace.
4. Creating Apache Cassandra tables corresponding to specific business queries, setting up correct partition key and clustering columns.
5. Inserting relevent data into the tables.
6. Running `SELECT` statetements in the CQL and printing the output of the query in a tabular form.
7. Droping the tables and closing the connection.

