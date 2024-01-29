
<h2> Code for Vasco Series- Mage AI Overview </h2> 


<h3> Before building the pipeline </h3>


Make sure you have the following GCP project, bucket, bigquery API, retrieve credentials from authored service account, save them as a json on a secure location.


Create [HMAC keys](https://console.cloud.google.com/storage/settings;tab=interoperability) for your bucket, required for Duckdb connection. 



<h3> Our pipeline consists of the following:</h3> 

1. [gcs_parquet_files](gcs_parquet_files.py) (Sensor) - check if there are parquet files under a google cloud storage bucket. If there are we want to return the oldest one as a global variable, following a FIFO logic. If conditions are met for the ingestion, the Sensor will return True, thereby allowing the rest of the pipeline to continue. 
2. [load_parquet_gcs](load_parquet_gcs.py) (Loader) - load from the parquet file previously assigned. 
3. [cleaning_data](cleaning_data.py) (Custom) - perform some dataframe transformations
4. [bq_export](bq_export.py) (Data Exporter) - load cleaned dataframe into a bigquery table.
5. [move_ingest_file](move_ingest_file.py) (Data Exporter) - Copy ingested file from the initial location, to the destination folder, delete the file from the watch folder location.   

<h3> Global Variables </h3>


![](/img/global_variables.png)


<h3> Pipeline Tree View </h3>


![](/img/MageAI_demo.png)






