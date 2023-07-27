CREATE DATABASE customer;
\c customer
CREATE SCHEMA IF NOT EXISTS account;

CREATE TABLE IF NOT EXISTS account.accounts
(
    id                      BIGSERIAL PRIMARY KEY,
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
    payload_bytes           BYTEA
);

CREATE TABLE IF NOT EXISTS account.balances
(
    account_number VARCHAR(20) UNIQUE NOT NULL,
    create_time    TIMESTAMP,
    balance        DOUBLE PRECISION,
    PRIMARY KEY (account_number, create_time)
);

CREATE TABLE IF NOT EXISTS account.transactions
(
    account_number VARCHAR(20) UNIQUE NOT NULL,
    create_time    TIMESTAMP,
    transaction_id VARCHAR(20),
    amount         DOUBLE PRECISION,
    PRIMARY KEY (account_number, create_time, transaction_id),
    CONSTRAINT fk_txn_account_number FOREIGN KEY (account_number) REFERENCES account.balances (account_number)
);

CREATE TABLE IF NOT EXISTS account.mapping
(
    key   TEXT,
    value TEXT
);
