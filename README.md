# Data Caterer - Data Generator

## Overview

Generator data for databases, files, JMS or HTTP request through a YAML based input and executed via Spark.
  
Full docs can be found [here](https://pflooky.github.io/data-caterer-docs/).

## Flow

![Data Caterer high level design](design/high-level-design.png "High level design")

## Generate data
### Quickest start
1. `mkdir /tmp/datagen`
2. `docker run -v /tmp/datagen:/opt/app/data-caterer pflookyy/data-caterer:0.1`
3. `head /tmp/datagen/sample/json/account-gen/part-0000*`

### Quick start
1. Run [App.scala](app/src/main/scala/com/github/pflooky/datagen/App.scala)
2. Set environment variables `ENABLE_GENERATE_PLAN_AND_TASKS=false;PLAN_FILE_PATH=/plan/account-create-plan.yaml`
3. Check generated data under [here](app/src/test/resources/sample/json)

### Manually create data
1. Create plan like [here](app/src/main/resources/plan/customer-create-plan.yaml)
2. Create tasks like [here](app/src/main/resources/task/postgres/postgres-customer-task.yaml)
3. Run job from [here](app/src/main/scala/com/github/pflooky/datagen/App.scala)
   1. Alter [application.conf](app/src/main/resources/application.conf)
      1. Set plan file path to run via environment variable [PLAN_FILE_PATH](app/src/main/resources/application.conf)
      2. Set task folder path via environment variable [TASK_FOLDER_PATH](app/src/main/resources/application.conf)

## Configuration/Customisation
### Configuration

| Config                          | Default Value | Description                                                                                             |
|---------------------------------|---------------|---------------------------------------------------------------------------------------------------------|
| BASE_FOLDER_PATH                | /tmp          | Defines base folder pathway to be used for plan and task files                                          |
| PLAN_FILE_PATH                  | <empty>       | Defines plan file path                                                                                  |
| TASK_FOLDER_PATH                | <empty>       | Defines folder path where all task files can be found                                                   |
| ENABLE_GENERATE_DATA            | true          | Enable/disable data generation                                                                          |
| ENABLE_GENERATE_PLAN_AND_TASKS  | true          | Enable/disable plan and task auto generation based off data source connections                          |
| ENABLE_RECORD_TRACKING          | true          | Enable/disable which data records have been generated for any data source                               |
| ENABLE_DELETE_GENERATED_RECORDS | false         | Delete all generated records based off record tracking (if ENABLE_RECORD_TRACKING has been set to true) |

### Datagen plan

[Sample plan](app/src/test/resources/sample/plan/customer-create-plan.yaml)

<details><summary>Detailed summary</summary><br>

```yaml
name: "customer_create_plan"
description: "Create customers in JDBC and Cassandra"
tasks:
  #list of tasks to execute
  - name: "jdbc_customer_accounts_table_create"
    #Name of the data source with configuration as defined in application.conf
    dataSourceName: "postgres"
  - name: "parquet_transaction_file"
    dataSourceName: "parquet"
  - name: "cassandra_customer_status_table_create"
    dataSourceName: "cassandra"
    #Can disable tasks, enabled by default
    enabled: false
  - name: "cassandra_customer_transactions_table_create"
    dataSourceName: "cassandra"
    enabled: false

sinkOptions:
  #Define a static seed if you want consistent data produced
  seed: "1"
  #Define any foreign keys that should match across data tasks
  foreignKeys:
    #The foreign key name with naming convention [dataSourceName].[schema].[column name]
    "postgres.accounts.account_number":
      #List of columns to match with same naming convention
      - "parquet.transactions.account_id"
```

</details>

### Datagen Task

[Sample task](app/src/test/resources/sample/task/postgres/postgres-transaction-task.yaml)

<details><summary>Detailed summary</summary><br>

Simple sample

```yaml
name: "jdbc_customer_accounts_table_create"
steps:
  #Define one or more steps within a task
  - name: "accounts"
    type: "postgres"
    count:
      #Number of records to generate
      total: 10
    #Define any Spark options to pass when pushing data
    options:
      dbtable: "account.accounts"
    schema:
      fields:
        - name: "account_number"
          #Data type of column: string, int, double, date
          type: "string"
          generator:
            #Type of data generator: regex, random, oneOf
            type: "regex"
            #Options to set per type of generator
            options:
              regex: "ACC1[0-9]{5,10}"
              seed: 1 #Can set the random seed at column level
        - name: "account_status"
          type: "string"
          generator:
            type: "oneOf"
            options:
              #List of potential values
              oneOf:
                - "open"
                - "closed"
        - name: "open_date"
          type: "date"
          generator:
            type: "random"
            #`options` is optional, will revert to defaults if not defined
            options:
              minValue: "2020-01-01" #Default: now() - 5 days
              maxValue: "2022-12-31" #Default: now()
        - name: "created_by"
          type: "string"
          generator:
            type: "random"
            options:
              minLength: 10  #Default: 1
              maxLength: 100 #Default: 20
        - name: "customer_id"
          type: "int"
          generator:
            type: "random"
            options:
              minValue: 0    #Default: 0
              maxValue: 100  #Default: 1
```

With multiple records per key (i.e. have X number of transactions per account)

```yaml
name: "parquet_transaction_file"
steps:
  - name: "transactions"
    type: "parquet"
    options:
      path: "/tmp/sample/parquet/transactions"
    count:
      #Number of records per column to generate
      perColumn:
        #Can be based on multiple columns
        columnNames:
          - "account_id"
        #Can define simple count of records
        count: 10
        #Or define generator for number of records (has to be int generator)
        generator:
          type: "random"
          options:
            minValue: 1
            maxValue: 10
...
```

</details>

### Datagen input

#### Supported data sinks

Data Caterer is able to support the following data sinks:

1. Database
   1. JDBC
   2. Cassandra
   3. ElasticSearch
2. HTTP
3. Files (local or remote like S3)
   1. CSV
   2. Parquet
   3. ORC
   4. Delta
   5. JSON
4. JMS
   1. Solace

#### Supported use cases

1. Insert into single data sink
2. Insert into multiple data sinks
   1. Foreign keys associated between data sources
   2. Number of records per column value
3. Set random seed at column level
4. Generate real looking data (via DataFaker) and edge cases
   1. Names, addresses, places etc.
   2. Edge cases for each data type (e.g. newline character in string, maximum integer, NaN, 0)
   3. Nullability
5. Send events progressively
6. Automatically insert data into data source
   1. Read metadata from data source and insert for all sub data sources (e.g. tables)
   2. Get statistics from existing data in data source if exists
7. Track and delete generated data
8. Extract data profiling and metadata from given data sources
   1. Calculate the total number of combinations

## Improvements

- UI to see dashboard of metadata and data generated
- Read in schema files (such as protobuf, openapi) and convert to tasks
  - Ability to convert sample data into task
  - Read from metadata sources like amundsen, datahub, etc.
- Pass in data attributes to HTTP URL as parameters
- Auto generate regex and/or faker expressions
- Track each data generation run along with statistics
- Fine grain control on delete certain run of data
- Demo for each type of data source
  - Demonstrate what modifications are needed for different use cases
  - Preloaded/preconfigured datasets within docker images
- ML model to assist in metadata gathering (either via API or self-hosted)
  - Regex and SQL generation
  - Foreign key detection across datasets
  - Documentation for the datasets
  - Via API could be problem as sensitive data could be shared
  - Via self-hosted requires large image (10+ Gb)
- Allow for delete from Queue or API
  - Ability to define a queue or endpoint that can delete the corresponding records
- Postgres data type related errors
  - spark converts to wrong data type when reading from postgres so fails to write back to postgres
      open_date_interval INTERVAL,
      ERROR: column "open_date_interval" is of type interval but expression is of type character varying
      open_id UUID,
      balance MONEY,
      payload_json JSONB

## Challenges

- How to apply foreign keys across datasets
- Providing functions for data generators
- Setting out the Plan -> Task -> Step model
- How to process the data in batches
- Data cleanup after run
  - Save data into parquet files. Can read and delete when needed
  - Have option to delete directly
  - Have to do in particular order due to foreign keys
- Relationships/constraints between fields
  - e.g. if transaction has type purchase, then it is a debit
  - if country is Australia, then country code should be AU
  - could be one to one, one to many, many to many mapping
- Predict the type of string expression to use from DataFaker
  - Utilise the metadata for the field
- Having intermediate fields and not including them into the output
  - Allow for SQL expressions
- Issues with spark streaming to write real-time data
  - Using rate format, have to manage the connection to the data source yourself
  - Connection per batch, stopped working for Solace after 125 messages (5 per second)
- Generating regex pattern given data samples
- Database generated columns values
  - Auto increment
  - On update current_timestamp
  - Omit generating columns (only if they are not used as foreign keys)

## Resources

[Spark test data generator](https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala)
