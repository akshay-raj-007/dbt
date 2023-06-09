{{ config(materialized='incremental') }}


with cte as(
    select * from {{ source('my_test_source', 'product_category_name_translation')}}
    {% if is_incremental() %}
    where timestamp_column > (select max(timestamp_column) from {{ this }})
    {% endif %}
),
 cleaned_data as (
  select *, row_number() over(partition by product_category_name) as r_no
  from cte
 )





select * from cleaned_data
where r_no=1