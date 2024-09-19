INSERT INTO cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_rewards_sum)
with cte as
(
select 
	dd.courier_id,
	courier_name,
	dt.year as settlement_year,
	dt.month as settlement_month,
	count(distinct dd.order_id) as orders_count,
	sum(dd.sum) as orders_total_sum,
	sum(dd.sum) * 0.25 as order_processing_fee,
	avg(rate) as rate_avg,
	sum(dd.tip_sum) as courier_tips_sum
from dds.dm_deliveries dd 
inner join dds.dm_couriers dc on dd.courier_id = dc.id 
inner join dds.dm_orders do2 on dd.order_id = do2.id
inner join dds.dm_timestamps dt on dt.id = do2.timestamp_id 
where dt.date >= date_trunc('month', date '{{ ds }}' - interval '1 month') 
	and dt.date < date_trunc('month', date '{{ ds }}' + interval '1 month')
group by
	dd.courier_id,
	courier_name,
	dt.year,
	dt.month 
),
cte2 as (
select *,
	case 
		when rate_avg < 4 then GREATEST(100, orders_total_sum * 0.05)
		when rate_avg < 4.5 then GREATEST(150, orders_total_sum * 0.07)
		when rate_avg < 4.9 then GREATEST(175, orders_total_sum * 0.08)
		else GREATEST(200, orders_total_sum * 0.1)
	end as courier_order_sum
from cte)
select 
	courier_id,
	courier_name,
	settlement_year,
	settlement_month,
	orders_count,
	orders_total_sum,
	rate_avg,
	order_processing_fee,
	courier_order_sum,
	courier_tips_sum,
	courier_order_sum + courier_tips_sum * 0.95 as courier_rewards_sum
from cte2
on conflict(settlement_year, settlement_month, courier_id) do update 
set 
	courier_name = excluded.courier_name,
	orders_count = excluded.orders_count,
	orders_total_sum = excluded.orders_total_sum,
	rate_avg = excluded.rate_avg,
	order_processing_fee = excluded.order_processing_fee,
	courier_order_sum = excluded.courier_order_sum,
	courier_tips_sum = excluded.courier_tips_sum,
	courier_rewards_sum = excluded.courier_rewards_sum
