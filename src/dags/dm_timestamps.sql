
   
INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time)
with cte as
(
select *, json_array_elements((object_value::JSON->>'statuses')::JSON)->>'status' AS status,
	json_array_elements((object_value::JSON->>'statuses')::JSON)->>'dttm' AS status_dttm
FROM stg.ordersystem_orders
)
select distinct
	status_dttm::timestamptz as ts,
	EXTRACT(YEAR FROM status_dttm::timestamp) AS year,
    EXTRACT(MONTH FROM status_dttm::timestamp) AS month,
    EXTRACT(DAY FROM status_dttm::timestamp) AS day,
    status_dttm::date AS date,
    status_dttm::time AS time
from cte
where status in ('CLOSED', 'CANCELLED')
on conflict (ts) do update
set
	year = excluded.year,
	month = excluded.month,
	day = excluded.day,
	date = excluded.date,
	time = excluded.time
