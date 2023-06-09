with cte as (
    select count(order_id) as total_order_count, to_char(order_purchase_timestamp, 'Month') AS month
    from {{ ref('stg_orders_dataset') }}
    group by 2
    order by 2
)

select * from cte