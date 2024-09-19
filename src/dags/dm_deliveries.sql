insert into dds.dm_deliveries (delivery_id, order_id, courier_id, rate, sum, tip_sum)
select 
	delivery_id,
	do2.id as order_id,
	dc.id as courier_id,
	rate,
	sum,
	tip_sum
from stg.deliveries d 
inner join dds.dm_couriers dc on d.courier_id = dc.courier_id 
inner join dds.dm_orders do2 on d.order_id = do2.order_key 
on conflict(delivery_id) do update
set
	order_id = excluded.order_id,
	courier_id = excluded.courier_id,
	rate = excluded.rate,
	sum = excluded.sum,
	tip_sum = excluded.tip_sum








