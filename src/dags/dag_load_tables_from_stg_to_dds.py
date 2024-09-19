from airflow import DAG
#from airflow.providers.postgres.operators.postgres import PostgresOperator
#from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator as PostgresOperator
#import pandas as pd
import pendulum
#from sqlalchemy.orm import sessionmaker

DWH_conn_id = 'PG_WAREHOUSE_CONNECTION'

# Define the DAG
with DAG(
        'load_tables_from_stg_to_dds',
        schedule='1 * * * *',
        description='Loading tables from STG to DDS',
        catchup=False,
        max_active_runs=1,
        start_date=pendulum.datetime(2024, 4, 26, tz="UTC")
) as dag:
    update_dm_users = PostgresOperator(
        task_id='update_dm_users',
        postgres_conn_id=DWH_conn_id,
        sql="dm_users.sql")
    
    update_dm_restaurants = PostgresOperator(
        task_id='update_dm_restaurants',
        postgres_conn_id=DWH_conn_id,
        sql="dm_restaurants.sql")
    
    update_dm_timestamps = PostgresOperator(
        task_id='update_dm_timestamps',
        postgres_conn_id=DWH_conn_id,
        sql="dm_timestamps.sql")
    
    update_dm_products = PostgresOperator(
        task_id='update_dm_products',
        postgres_conn_id=DWH_conn_id,
        sql="dm_products.sql")
    
    update_dm_orders = PostgresOperator(
        task_id='update_dm_orders',
        postgres_conn_id=DWH_conn_id,
        sql="dm_orders.sql")
    
    update_fct_product_sales = PostgresOperator(
        task_id='update_fct_product_sales',
        postgres_conn_id=DWH_conn_id,
        sql="fct_product_sales.sql")
    
    update_dm_settlement_report = PostgresOperator(
        task_id='update_dm_settlement_report',
        postgres_conn_id=DWH_conn_id,
        sql="dm_settlement_report.sql")
    
    update_dm_couriers = PostgresOperator(
        task_id='update_dm_couriers',
        postgres_conn_id=DWH_conn_id,
        sql="dm_couriers.sql")
    
    update_dm_deliveries = PostgresOperator(
        task_id='update_dm_deliveries',
        postgres_conn_id=DWH_conn_id,
        sql="dm_deliveries.sql")
    
    update_dm_courier_ledger = PostgresOperator(
        task_id='update_dm_courier_ledger',
        postgres_conn_id=DWH_conn_id,
        sql="dm_courier_ledger.sql")






