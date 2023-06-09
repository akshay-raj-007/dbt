{{ config(materialized='incremental') }}

with cte as(
    select * from {{ source('my_test_source', 'stg_incremental_table')}}

      {% if is_incremental() %}
      where timestamp_column > (select max(timestamp_column) from {{ this }})
      {% endif %}
  )



select * from cte



