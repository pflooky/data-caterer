# Load Test Results

Load tests from running on M1 Macbook with 16Gb memory.

## Large plan

- Large plan has two data source: CSV and JSON
- One foreign key is defined between the account_id in the CSV and JSON
- 100,000 records are generated for the JSON file
  - 200,000 records, 2 records per account_id, are generated for the CSV file

## Result

Run 1 (foreign key, no unique): 26s
Run 2 (foreign key, with unique): 45s
Run 3 (cache after unique): 45s
Run 4 (additional field with unique): 60s
