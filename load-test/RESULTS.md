# Load Test Results

Load tests from running on M1 Macbook with 16Gb memory.

## Large plan

- Large plan has two data source: CSV and JSON
- One foreign key is defined between the account_id in the CSV and JSON
- 100,000 records are generated for the JSON file
  - 200,000 records, 2 records per account_id, are generated for the CSV file

### Result

Run 1 (foreign key, no unique): 26s
Run 2 (foreign key, with unique): 45s
Run 3 (cache after unique): 45s
Run 4 (additional field with unique): 60s

## Dvd Rental

- 1000 records per table
- Many foreign keys
- Many primary keys (singular and composite)

### Result

Run 1: 202s
Run 2 (with cache before zipWithIndex, shuffle partitions = 10): 147s
Run 3 (same as 2 with disable count): 122s
Run 4 (same as 3 run in docker): 22s

## Postgres Multiple Tables

- Write to balances and transactions
- 1,000,000 in balances
- 2,000,000 in transactions, 5 transactions per 200,000 accounts
- Link account_number between balances and transactions

### Result

Run 1 (no primary keys defined): 166s
Run 2 (shuffle partitions from 10 to 3): 149s
Run 3 (batch size 1,000,000): 105s
Run 4 (shuffle partitions from 3 to 1): 109s