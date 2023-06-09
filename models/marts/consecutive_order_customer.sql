{{ config(materialized='table') }}


with cte as (
    select * from {{ ref('stg_orders_dataset')}}
   
),
consecutive_ordered_customer_details as(
    select s.customer_id 
    from cte as s
    inner join cte as b
    ON s.customer_id = b.customer_id
    AND  s.order_purchase_timestamp::date = (b.order_purchase_timestamp::date - interval '1 day' )
)

select * from consecutive_ordered_customer_details
