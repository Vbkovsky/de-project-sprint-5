INSERT INTO dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to)
select 
	id,
	(object_value::json)->>'_id' as restaurant_id,
	(object_value::json)->>'name' as restaurant_name,
	update_ts as active_from,
	'2099-12-31' as active_to
from stg.ordersystem_restaurants or2 
on conflict (id) do update
set
	restaurant_id = excluded.restaurant_id,
	restaurant_name = excluded.restaurant_name,
	active_from = excluded.active_from,
	active_to = excluded.active_to