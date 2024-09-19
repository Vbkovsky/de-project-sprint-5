insert into dds.dm_users (id, user_id, user_login, user_name)
select distinct
    id,
    (object_value::json)->>'_id' AS user_id,
    (object_value::json)->>'login' AS user_login,
    (object_value::json)->>'name' AS user_name
FROM
    stg.ordersystem_users
on conflict (id) do update
set 
	user_id = excluded.user_id,
	user_login = excluded.user_login,
	user_name = excluded.user_name;