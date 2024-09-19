INSERT INTO dds.dm_products(product_id, product_name, product_price, active_from, active_to, restaurant_id)
with cte as (
select
	json_array_elements((object_value::JSON->>'order_items')::JSON)->>'id' as product_id,
	json_array_elements((object_value::JSON->>'order_items')::JSON)->>'name' as product_name,
	(json_array_elements((object_value::JSON->>'order_items')::JSON)->>'price')::numeric as product_price,
	update_ts as active_from,
	'2099-12-31 00:00:00.000'::timestamptz as active_to,
	((object_value::JSON)->>'restaurant')::JSON->>'id' as restaurant_id
from stg.ordersystem_orders 
)
select distinct 
	product_id, 
	product_name, 
	product_price, 
	min(cte.active_from) as active_from, 
	cte.active_to, 
	dr.id as restaurant_id
from cte
inner join dds.dm_restaurants dr on cte.restaurant_id = dr.restaurant_id 
group by
	product_id, 
	product_name, 
	product_price, 
	cte.active_to, 
	dr.id
on conflict (product_id) do update
set
	product_name = excluded.product_name,
	product_price = excluded.product_price,
	active_from = excluded.active_from,
	active_to = excluded.active_to,
	restaurant_id = excluded.restaurant_id
	
	
	 