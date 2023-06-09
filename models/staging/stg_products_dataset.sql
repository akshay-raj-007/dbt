{{ config(materialized='incremental') }}


with cte as(
    select * from {{ source('my_test_source', 'olist_products_dataset')}}

    {% if is_incremental() %}
    where timestamp_column > (select max(timestamp_column) from {{this}})
    {% endif %}
),




cleaned_product_data as(
  
      select product_id, product_category_name,timestamp_column,row_number() over(partition by product_id) as r_no
      from cte
  ),
    


cleaned_data as (
  select product_id, product_category_name,timestamp_column
  from cleaned_product_data
  where r_no=1

)
select * from cleaned_data
