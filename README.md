# Data Caterer - Data Generation and Validation

![Data Catering](misc/banner/logo_landscape_banner.svg)

## Overview

Generator data for databases, files, JMS or HTTP request through a YAML based input and executed via Spark.
  
Full docs can be found [**here**](https://pflooky.github.io/data-caterer-docs/).

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

## Configuration/Customisation

### Supported data sources

Data Caterer is able to support the following data sources:

1. Database
   1. JDBC 
      1. Postgres
      2. MySQL
   2. Cassandra
   3. ElasticSearch (soon)
2. HTTP
3. Files (local or remote like S3)
   1. CSV
   2. Parquet
   3. ORC
   4. Delta (soon)
   5. JSON
4. JMS
   1. Solace
5. Kafka

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
- Metadata storage and referencing
  - How will it interact with a data dictionary?
  - Updated schema/metadata

## UI

- UI for no/low code solution
- Run as same image
  - Option to execute jobs separately
    - Interface through YAML files?
- Pages
  - Data sources
  - Generation
  - Validation

## Resources

[Spark test data generator](https://github.com/apache/spark/blob/master/sql/catalyst/src/test/scala/org/apache/spark/sql/RandomDataGenerator.scala)

### Java 17 VM Options

```shell
--add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED
```