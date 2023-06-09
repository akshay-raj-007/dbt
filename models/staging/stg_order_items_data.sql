{{ config(materialized='incremental') }}

with cte as (
  select * from {{ source('my_test_source', 'olist_order_items_dataset') }}

  {% if is_incremental() %}
  where timestamp_column > (select max(timestamp_column) from {{ this }})
  {% endif %}
),



user_data as (
    select order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date as shipping_date,
        price,
        timestamp_column,
        row_number() over(partition by order_id) as r_no
    from cte
),
cleaned_data as(
  select order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_date,
        price,
        timestamp_column
  from user_data
  where r_no=1
)

select * from cleaned_data