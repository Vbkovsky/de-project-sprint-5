-- bonussystem_users
CREATE TABLE stg.bonussystem_users (
	id integer NOT NULL,
	order_user_id text NOT NULL
);

-- bonussystem_ranks
CREATE TABLE stg.bonussystem_ranks (
	id integer NOT NULL,
	"name" varchar(2048) NOT NULL,
	bonus_percent numeric(19, 5) DEFAULT 0 NOT NULL,
	min_payment_threshold numeric(19, 5) DEFAULT 0 NOT NULL
);

-- bonussystem_events
CREATE TABLE stg.bonussystem_events (
	id integer NOT NULL,
	event_ts timestamp NOT NULL,
	event_type varchar NOT NULL,
	event_value text NOT NULL
);

CREATE INDEX idx_bonussystem_events__event_ts ON stg.bonussystem_events USING btree (event_ts);

-- ordersystem_orders
CREATE TABLE stg.ordersystem_orders (
	id serial NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL
);

ALTER TABLE stg.ordersystem_orders
ADD CONSTRAINT ordersystem_orders_object_id_uindex UNIQUE (object_id);

-- ordersystem_restaurants
CREATE TABLE stg.ordersystem_restaurants (
	id serial NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL
);

ALTER TABLE stg.ordersystem_restaurants
ADD CONSTRAINT ordersystem_restaurants_object_id_uindex UNIQUE (object_id);

-- ordersystem_users
CREATE TABLE stg.ordersystem_users (
	id serial NOT NULL,
	object_id varchar NOT NULL,
	object_value text NOT NULL,
	update_ts timestamp NOT NULL
);

ALTER TABLE stg.ordersystem_users
ADD CONSTRAINT ordersystem_users_object_id_uindex UNIQUE (object_id);

-- couriers
drop table if exists stg.couriers;
create table stg.couriers (
	id serial not null,
	_id varchar not null,
	name varchar not null
);

ALTER TABLE stg.couriers
ADD CONSTRAINT couriers_id_uindex UNIQUE (_id);

-- deliveries
drop table if exists stg.deliveries;
create table stg.deliveries (
	id serial not null,
	order_id varchar not null,
	order_ts timestamptz not null,
	delivery_id varchar not null,
	courier_id varchar not null,
	address varchar not null,
	delivery_ts timestamptz not null,
	rate integer not null,
	sum numeric(14,2) not null,
	tip_sum numeric(14,2) not null
);

ALTER TABLE stg.deliveries
ADD CONSTRAINT deliveries_delivery_id_uindex UNIQUE (delivery_id);