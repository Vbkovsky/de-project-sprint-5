-- dm_settlement_report
drop table if exists cdm.dm_settlement_report;
create table cdm.dm_settlement_report (
	id serial NOT NULL primary key,
	restaurant_id varchar NOT NULL,
	restaurant_name varchar NOT NULL,
	settlement_date date NOT NULL,
	orders_count integer NOT NULL,
	orders_total_sum numeric(14, 2) NOT NULL,
	orders_bonus_payment_sum numeric(14, 2) NOT NULL,
	orders_bonus_granted_sum numeric(14, 2) NOT NULL,
	order_processing_fee numeric(14, 2) NOT NULL,
	restaurant_reward_sum numeric(14, 2) NOT NULL
);

ALTER TABLE cdm.dm_settlement_report 
ADD CONSTRAINT dm_settlement_report_settlement_date_check
CHECK (settlement_date >= '2022-01-01' AND settlement_date < '2500-01-01');

-- dm_courier_ledger 
drop table if exists cdm.dm_courier_ledger;
create table cdm.dm_courier_ledger (
	id serial NOT NULL primary key,
	courier_id integer not null,
	courier_name varchar not null,
	settlement_year integer not null,
	settlement_month integer not null,
	orders_count integer not null DEFAULT 0 check (orders_count >= 0),
	orders_total_sum numeric(14, 2) not null DEFAULT 0 check (orders_total_sum >= 0),
	rate_avg numeric (14, 2) not null DEFAULT 0 check (rate_avg >= 0),
	order_processing_fee numeric (14, 2) not null DEFAULT 0 check (order_processing_fee >= 0),
	courier_order_sum numeric (14, 2) not null DEFAULT 0 check (courier_order_sum >= 0),
	courier_tips_sum numeric (14, 2) not null DEFAULT 0 check (courier_tips_sum >= 0),
	courier_rewards_sum numeric (14, 2) not null DEFAULT 0 check (courier_rewards_sum >= 0)	
);

ALTER TABLE cdm.dm_courier_ledger
ADD CONSTRAINT unique_id_month_year_combination UNIQUE (courier_id, settlement_year, settlement_month);