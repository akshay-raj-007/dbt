with cte as(
    select count(ooid.order_id) as total_orders, ooid.product_id , opd.product_category_name
	from {{ ref('stg_order_items_data')}} as ooid 
	left join {{ ref('stg_products_dataset')}} as opd 
	on ooid.product_id =opd.product_id 
	group by ooid.product_id, opd.product_category_name
    ),

product_count as(
    select s.product_category_name_english ,cte.total_orders  from cte
    left join {{ ref('stg_category_name_translation')}} as s
    on cte.product_category_name=s.product_category_name
    where s.product_category_name is not null
    order by cte.total_orders desc
)

select product_category_name_english, sum(total_orders) as total_order_count
from product_count
group by product_category_name_english
order by 2 desc
