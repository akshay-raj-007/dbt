{{ config(materialized='incremental') }}

with cte as (
  select * from {{ source('my_test_source', 'olist_order_reviews_dataset') }}
  {% if is_incremental() %}
  where timestamp_column > (select (max(timestamp_column)) from {{ this }} )
  {% endif %}

),

user_data as (
    select *, row_number()  over(partition by review_id) as r_no
    from cte

),

unique_review as(
    select review_id,
        order_id,
        review_score,
        review_creation_date,
        timestamp_column
    from user_data
    where r_no=1
)

select * from unique_review