# Lydia Technical Test

## Requirements

- Python 3.10 or higher (I used 3.11).
- Docker.
- make.

## Guide

To start the project simply run:

```bash
make run
```

This will start the required containers, and as soon as it's done, you will be able to access the airflow UI [here](http://localhost:8080/).
To authenticate use the default credentials (user: airflow; password: airflow).

If you check the history, you'll find that I tried using prefect, in order to be able to run it locally, no docker needed, however I ran into some issues with the Airbyte cache, it keeps a. lock on the db file, so I went to airflow, since I have a lot of experience with it, and the PythonOperator helps in closing any lingering connections and/or process

Once on the home page, you will find one dag, the bitcoin_elt_pipeline, it consists of 5 tasks:
- extract_and_load
- verify_data
- transform
- test 
- visualize_data

### The extraction

The python task uses airbyte to fetch coingecko `market_chart` data ([documentation here](https://docs.airbyte.com/integrations/sources/coingecko-coins)).
The last seven days of data is retrieved.
Data is stored in a local cache, as a duckdb database file in the table `market_chart`.
I wanted to use my own cache instead of the default one in order to use the same db for all the tasks, and share it in a volume.


### Verifying the data

This python task reads the raw data as written by Airbyte, and checks that the table exists, and that the prices column has at least one element in it.

### Transforming the data
The python tasks loads extracted data from cached raw data to load it into the crypto_data duckdb database.

Inside `market_chart`, the column prices is an array of unix timestamp and usd price. The dbt model takes the data as is, and extracts the timestamp and price from each element in the list of prices, and the resulting table is then used to compute daily aggregates and stores it in a table called `daily_candlestick`. In this table I used the Date as a unique_key since we only have one currency and one row per day, if we had multiple currencies or multiple data points per day, another row_Id policy might be warranted.


### Testing

I put in place some tests within the model, as well as tests to ensure the validity of the data, the tests are run during the `dbt build` command and `dbt test`

### Visualization

This task reads the data from `daily_candlestick` table and plots a candlestick graph showing the progression of the price during the last 7 days.

## Improvements

### Data transformation and query optimization
- Implement intermediate models instead of using CTEs in one big query, and implement tests for each of the intermediate models.
- Implement indexing, (using the date as an index in this case for example).
- Implement clustering, for example, in the case of multiple currencies, cluster by currency.
- I implemented daily partitionning, but since we only have one record per day it's not warranted.
- Even though DuckDB is a powerful processing engine, I would recommend using a more suitable solution to store data permanently, for example, after each process copy the raw data in object storage, the final data in a data warehouse, and only leave the last 7 days for example in DuckDb for quick access.

### 1. CI/CD
- Implement a CI/CD pipeline with automated builds, testing, and deployment
- Add code quality checks including linting (ex: pylint) and formatting (ex: black)
- Add unit and end to end tests
- Automated dependency vulnerability scanning

### 2. Data Quality & Monitoring
- Enhanced dbt tests with oackages like `dbt_expectations` for data validation
- Data freshness monitoring and alerting
- Add data profiling and anomaly detection using tools like `dbt_expectations`n `dbt macro` and python's `GX`

### 3. Error Handling & Logging
- Structured logging with consistent format across all components, instead of just prints
- Centralized logging, error tracking and alerting
- Use a comprehensive performance monitoring stack

### 4. Data Governance
- Create data retention policies
- Add data lineage documentation
- Implement data versioning
- Add schema evolution strategy

### 5. Security
- Dependency vulnerability scanning and patching
- Secrets scanning and rotation
- Role-based access controls (RBAC)


