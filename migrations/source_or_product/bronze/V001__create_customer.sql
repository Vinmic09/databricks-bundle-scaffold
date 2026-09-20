CREATE TABLE IF NOT EXISTS ${catalog}.bronze.customer
(
    customer_id BIGINT,
    customer_name STRING,
    created_date TIMESTAMP
)
USING DELTA;