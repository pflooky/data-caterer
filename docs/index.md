# Spartagen - Data Generator

## Overview
Ability to generate production like data based on any source/target system whether it be a CSV file, database table, etc.
It can also be manually altered to produce data the way you want leveraging [datafaker](https://www.datafaker.net/documentation/getting-started/).

## Generate data
### Quick start
1. Run [App.scala](../app/src/main/scala/com/github/pflooky/datagen/App.scala)
2. Check generated data under [here](../app/src/test/resources/csv/transactions)

### Manually create data
1. Create plan like [here](../app/src/main/resources/plan/customer-create-plan.yaml)
2. Create tasks like [here](../app/src/main/resources/task/postgres/postgres-customer-task.yaml)
3. Run job from [here](../app/src/main/scala/com/github/pflooky/datagen/App.scala)
   1. Alter [application.conf](../app/src/main/resources/application.conf) if you have any data source/target configurations
      1. Set plan file path to run via environment variable [PLAN_FILE_PATH](../app/src/main/resources/application.conf)
      2. Set task folder path via environment variable [TASK_FOLDER_PATH](../app/src/main/resources/application.conf)

### Advanced use cases
#### Foreign keys across data sets
If you have a use case where you require a columns value to match in another data set, this can be achieved in the plan definition.
For example, if I have the column `account_number` in a data source named `customer-postgres` and column `account_id` in `transaction-cassandra`,
```yaml
sinkOptions:
  foreignKeys:
    #The foreign key name with naming convention [dataSourceName].[schema].[column name]
    "customer-postgres.accounts.account_number":
      #List of columns to match with same naming convention
      - "transaction-cassandra.transactions.account_id"
```
  
You can define any number of foreign key relationships as you want.

#### Generating JSON data