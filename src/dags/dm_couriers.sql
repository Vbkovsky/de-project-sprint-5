insert into dds.dm_couriers (courier_id, courier_name)
select distinct
    _id AS courier_id,
    name AS courier_name
FROM
    stg.couriers
on conflict (courier_id) do update
set 
	courier_name = excluded.courier_name;
