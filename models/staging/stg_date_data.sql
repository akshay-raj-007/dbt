{{ config(materialized='incremental') }}

with cte as(
    select * from {{ source('my_test_source', 'date_dataset')}}

    {% if is_incremental() %}
    where date_a > (select max(date_a) from {{ this }})
    {% endif %}
)
select * from cte


