insert into dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant)
with cte as (
select 
	(event_value::JSON)->>'order_id' as order_id,
	json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'product_id' as product_id,
	(json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'quantity')::numeric as count,
	(json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'price')::numeric as price,
	(json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'product_cost')::numeric as total_sum,
	(json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'bonus_payment')::numeric as bonus_payment,
	(json_array_elements((event_value::JSON->>'product_payments')::JSON)->>'bonus_grant')::numeric as bonus_grant
from stg.bonussystem_events be  
where event_type = 'bonus_transaction'
)
select  
	dp.id as product_id,
	do2.id as order_id,
	count,
	price,
	total_sum,
	bonus_payment,
	bonus_grant
from cte
inner join dds.dm_orders do2 on cte.order_id = do2.order_key 
inner join dds.dm_products dp on cte.product_id = dp.product_id 
on conflict (order_id,product_id) do update 
set 
	count = excluded.count,
	price = excluded.price,
	total_sum = excluded.total_sum,
	bonus_payment = excluded.bonus_payment,
	bonus_grant = excluded.bonus_grant