CREATE DATABASE customer;
\c customer
CREATE SCHEMA IF NOT EXISTS account;
CREATE TABLE IF NOT EXISTS account.accounts (
    account_number VARCHAR(20) NOT NULL,
    account_status VARCHAR(10),
    open_date DATE,
    created_by VARCHAR(20),
    customer_id INT
);
