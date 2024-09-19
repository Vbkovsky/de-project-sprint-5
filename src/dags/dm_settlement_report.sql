insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
with cte as(
select 
	do2.restaurant_id,
	restaurant_name,
	date as settlement_date,
	order_id,
	count,
	total_sum,
	bonus_payment,
	bonus_grant
from dds.dm_orders do2 
inner join dds.fct_product_sales fps on do2.id = fps.order_id 
inner join dds.dm_timestamps dt on do2.timestamp_id = dt.id 
inner join dds.dm_restaurants dr on do2.restaurant_id = dr.id
where order_status = 'CLOSED'
)
select 
	restaurant_id,
	restaurant_name,
	settlement_date,
	count(distinct order_id) as orders_count,
	sum(total_sum) as orders_total_sum,
	sum(bonus_payment) as orders_bonus_payment_sum,
	sum(bonus_grant) as orders_bonus_granted_sum,
	sum(total_sum) * 0.5 as order_processing_fee,
	sum(total_sum) * 0.5 - sum(bonus_payment) as restaurant_reward_sum
from cte
group by
	restaurant_id,
	restaurant_name,
	settlement_date
on conflict (restaurant_id, settlement_date) do update
set
	restaurant_name = excluded.restaurant_name,
	orders_count = excluded.orders_count,
	orders_total_sum = excluded.orders_total_sum,
	orders_bonus_payment_sum = excluded.orders_bonus_payment_sum,
	orders_bonus_granted_sum = excluded.orders_bonus_granted_sum,
	order_processing_fee = excluded.order_processing_fee,
	restaurant_reward_sum = excluded.restaurant_reward_sum