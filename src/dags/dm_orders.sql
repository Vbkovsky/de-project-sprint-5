insert into dds.dm_orders (order_key, order_status, restaurant_id, timestamp_id, user_id)
with cte as
(
select distinct
	(object_value::JSON)->>'_id' as order_key,
	(object_value::JSON)->>'final_status' as order_status,
	((object_value::JSON)->>'restaurant')::JSON->>'id' as restaurant_id,
	((object_value::JSON)->>'date')::timestamp as ts,
	((object_value::JSON)->>'user')::JSON->>'id' as user_id	
from stg.ordersystem_orders
)
select 
	order_key,
	order_status,
	min(dr.id) as restaurant_id,
	min(dt.id) as timestamp_id,
	min(du.id) as user_id	
from cte
inner join dds.dm_restaurants dr on cte.restaurant_id = dr.restaurant_id
inner join dds.dm_timestamps dt on cte.ts = dt.ts
inner join dds.dm_users du on cte.user_id = du.user_id 
group by
	order_key,
	order_status
on conflict(order_key) do update
set
	order_status = excluded.order_status,
	restaurant_id = excluded.restaurant_id,
	timestamp_id = excluded.timestamp_id,
	user_id = excluded.user_id








