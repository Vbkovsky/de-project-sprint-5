-- dm_users
drop table if exists dds.dm_users;
create table dds.dm_users
(
	id serial primary key,
	user_id varchar not null,
	user_name varchar not null,
	user_login varchar not null
);

-- dm_restaurants
drop table if exists dds.dm_restaurants;
create table dds.dm_restaurants
(
	id serial primary key,
	restaurant_id varchar not null,
	restaurant_name varchar not null,
	active_from timestamp not null,
	active_to timestamp not null
);

-- dm_products
drop table if exists dds.dm_products;
create table dds.dm_products
(
	id serial primary key,
	restaurant_id integer not null,
	product_id varchar not null,
	product_name varchar not null,
	product_price NUMERIC(14, 2) DEFAULT 0 CHECK (product_price >= 0 AND product_price <= 1000000000000) not null,
	active_from timestamp not null,
	active_to timestamp not null
);

ALTER TABLE dds.dm_products
ADD CONSTRAINT unique_product_id UNIQUE (product_id);

ALTER TABLE dds.dm_products
ADD CONSTRAINT dm_products_restaurant_id_fkey
FOREIGN KEY (restaurant_id)
REFERENCES dds.dm_restaurants;

-- dm_timestamps
drop table if exists dds.dm_timestamps;
create table dds.dm_timestamps
(
	id serial primary key,
	ts timestamp not null,
	year smallint check(year >= 2022 and year < 2500) not null,
	month smallint check(month >= 1 and month <= 12) not null,
	day smallint  check (day >= 1 and day <= 31) not null,
	time time not null,
	date date not null
);

ALTER TABLE dds.dm_timestamps ADD CONSTRAINT unique_ts UNIQUE (ts);

-- dm_orders
drop table if exists dds.dm_orders;
create table dds.dm_orders
(
	id serial primary key,
	order_key varchar not null,
	order_status varchar not null,
	user_id integer not null,
	restaurant_id integer not null,
	timestamp_id integer not null
);

ALTER TABLE dds.dm_orders
ADD CONSTRAINT unique_order_key UNIQUE (order_key);

alter table dds.dm_orders
add constraint dm_orders_user_id_fkey
foreign key (user_id)
references dds.dm_users,

add constraint dm_orders_restaurant_id_fkey
foreign key (restaurant_id)
references dds.dm_restaurants,

add constraint dm_orders_timestamp_id_fkey
foreign key (timestamp_id)
references dds.dm_timestamps;

-- fct_product_sales
drop table if exists dds.fct_product_sales;
create table dds.fct_product_sales
(
	id serial primary key,
	product_id integer not null,
	order_id integer not null,
	count integer not null default 0 check(count >= 0),
	price numeric(14,2) not null default 0 check(price >= 0),
	total_sum numeric(14,2) not null default 0 check(total_sum >= 0),
	bonus_payment numeric(14,2) not null default 0 check(bonus_payment >= 0),
	bonus_grant numeric(14,2) not null default 0 check(bonus_grant >= 0),
	constraint fct_product_sales_product_id_fkey foreign key (product_id) references dds.dm_products,
	constraint fct_product_sales_order_id_fkey foreign key (order_id) references dds.dm_orders
);

ALTER TABLE dds.fct_product_sales ADD CONSTRAINT unique_product_id_order_id UNIQUE (product_id, order_id);


