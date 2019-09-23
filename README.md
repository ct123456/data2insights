# data2insights


# Healthcare

## Pipeline

### Current Steps
* Transform raw data to parquet
* Create subsets for exploration
* Clean
   * Transform Zip9 to Zip5+4
   * Fix corrupt data (e.g., 3 digit zip codes -> add leading zeros)

### Coming Soon!
* Split NPI into separate data sets (HCP, HCO)
* Aggregate and join data into entities (HCP, HCO, Zipcode)



## Build
Zip files to include in spark job

`bash scripts/build.sh`

## Execute
`spark-submit --py-files builds/dependencies.zip --master spark://<ip of master node>:7077 --jars jars/postgresql-42.2.8.jar ingestion/write_npi_to_db.py`