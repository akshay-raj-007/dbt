{{ config(materialized='incremental') }}

with cte as (
  select * from {{ source('my_test_source', 'olist_order_payments_dataset') }}

  {% if is_incremental() %}
  where timestamp_column > (select max(timestamp_column) from {{ this }})
  {% endif %}


),



user_data as(
    select order_id,
        payment_type,
        payment_value,
        timestamp_column,
        row_number() over(partition by order_id) as r_no
    from cte
),
 
cleaned_data as(
  select * from user_data
  where r_no=1
) 

select * from cleaned_data