{{ config(materialized='incremental') }}

with cte as (
  select * from {{ source('my_test_source', 'olist_customers_dataset') }}

  {% if is_incremental() %}
  where timestamp_column > (select max(timestamp_column) from {{ this }})
  {% endif %}
),



unique_customer_details as (
  select *, row_number() over (partition by customer_id) as r_no
  from cte
),

customer_details as (
  select 
    customer_id,
    customer_zip_code_prefix as customer_zip_code,
    customer_city,
    customer_state as customer_state_code,
    timestamp_column
  from unique_customer_details
  where r_no = 1
)

select * from customer_details
