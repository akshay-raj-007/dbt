-- Calculating monthly orders. To find which month does the maximum sales occur throughout the data
{{ config(materialized='incremental') }}


with cte as (
  select * from {{source('my_test_source', 'olist_orders_dataset')}}

  {% if is_incremental() %}
  where timestamp_column > (select max(timestamp_column) from {{ this }})
  {% endif %}

),


unique_orders_dataset as(
    with lete as(
      select *, row_number() over(partition by order_id) as r_no
      from cte 
    )
	  
    select * from lete
    where r_no=1
)

select * from unique_orders_dataset




