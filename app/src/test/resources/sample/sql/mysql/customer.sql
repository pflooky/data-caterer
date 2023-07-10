CREATE DATABASE customer;
USE customer;
CREATE SCHEMA IF NOT EXISTS account;

CREATE TABLE IF NOT EXISTS account.accounts
(
    id                      SERIAL PRIMARY KEY,
    account_number          VARCHAR(20) NOT NULL,
    account_status          VARCHAR(10),
    created_by              TEXT,
    created_by_fixed_length CHAR(10),
    customer_id_int         INT UNIQUE,
    customer_id_smallint    SMALLINT,
    customer_id_bigint      BIGINT,
    customer_id_decimal     DECIMAL,
    customer_id_real        REAL,
    customer_id_double      DOUBLE PRECISION,
    open_date               DATE,
    open_timestamp          TIMESTAMP,
    last_opened_time        TIME,
    payload_bytes           BLOB
);
-- spark converts to wrong data type when reading from postgres so fails to write back to postgres
--     open_date_interval INTERVAL,
-- ERROR: column "open_date_interval" is of type interval but expression is of type character varying
--     open_id UUID,
--     balance MONEY,
--     payload_json JSONB

CREATE TABLE IF NOT EXISTS account.balances
(
    id          BIGINT UNSIGNED NOT NULL,
    create_time TIMESTAMP,
    balance     DOUBLE PRECISION,
    PRIMARY KEY (id, create_time),
    CONSTRAINT fk_bal_account_number FOREIGN KEY (id) REFERENCES account.accounts (id)
);

CREATE TABLE IF NOT EXISTS account.transactions
(
    id             BIGINT UNSIGNED NOT NULL,
    create_time    TIMESTAMP,
    transaction_id VARCHAR(20),
    amount         DOUBLE PRECISION,
    PRIMARY KEY (id, create_time, transaction_id),
    CONSTRAINT fk_txn_account_number FOREIGN KEY (id) REFERENCES account.accounts (id)
);

SELECT c.table_schema                                                     AS "schema",
       c.table_name                                                       AS "table",
       c.column_name                                                      AS "column",
       c.data_type                                                        AS source_data_type,
       c.is_nullable,
       c.character_maximum_length,
       c.numeric_precision,
       c.numeric_scale,
       c.column_default,
       CASE WHEN uc.constraint_type != 'PRIMARY' THEN 'YES' ELSE 'NO' END AS is_unique,
       CASE WHEN pc.constraint_type = 'PRIMARY' THEN 'YES' ELSE 'NO' END  AS is_primary_key,
       pc.seq_in_index                                                    AS primary_key_position
FROM information_schema.columns AS c
         LEFT OUTER JOIN (SELECT stat.table_schema,
                                 stat.table_name,
                                 stat.column_name,
                                 stat.index_name AS constraint_type
                          FROM information_schema.statistics stat
                          WHERE index_name != 'PRIMARY') uc
                         ON uc.column_name = c.column_name AND uc.table_schema = c.table_schema AND
                            uc.table_name = c.table_name
         LEFT OUTER JOIN (SELECT stat1.table_schema,
                                 stat1.table_name,
                                 stat1.column_name,
                                 stat1.index_name AS constraint_type,
                                 stat1.seq_in_index
                          FROM information_schema.statistics stat1
                          WHERE index_name = 'PRIMARY') pc
                         ON pc.column_name = c.column_name AND pc.table_schema = c.table_schema AND
                            pc.table_name = c.table_name