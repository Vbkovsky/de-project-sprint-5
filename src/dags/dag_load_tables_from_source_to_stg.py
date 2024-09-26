import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator as PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models.variable import Variable
from datetime import datetime, timedelta
import pandas as pd
import pendulum
from sqlalchemy.orm import sessionmaker

source_conn_id = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
DWH_conn_id = 'PG_WAREHOUSE_CONNECTION'

def load_table_from_source_to_dwh(source_table, dwh_scheme, dwh_table):
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    dwh_hook = PostgresHook(postgres_conn_id=DWH_conn_id)

    source_engine = source_hook.get_sqlalchemy_engine()
    dwh_engine = dwh_hook.get_sqlalchemy_engine()

    # Load data from source DB to DataFrame
    source_df = pd.read_sql(f"SELECT * FROM {source_table};", source_engine)
    print('Number of rows:', len(source_df))

    # Load data from DataFrame to DWH
    source_df.to_sql(dwh_table, dwh_engine, schema=dwh_scheme, if_exists='replace', index=False)
    print('Data inserted into DWH successfully')

    # Close SQLAlchemy engines to release resources
    source_engine.dispose()
    dwh_engine.dispose()

def update_bonussystem_events():
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    dwh_hook = PostgresHook(postgres_conn_id=DWH_conn_id)

    source_engine = source_hook.get_sqlalchemy_engine()
    dwh_engine = dwh_hook.get_sqlalchemy_engine()

    Session = sessionmaker(bind=dwh_engine)
    session = Session()

    # Getting the max_id from the workflow_settings
    settings_query = """
    SELECT workflow_settings 
    FROM stg.srv_wf_settings 
    WHERE workflow_key = 'load_tables_from_source_to_stg';
    """
    settings_result = pd.read_sql(settings_query, dwh_engine)
    
    if not settings_result.empty:
        settings_value = settings_result.iloc[0, 0]
        if isinstance(settings_value, str):
            settings_json = json.loads(settings_value)
        elif isinstance(settings_value, dict):
            settings_json = settings_value
        else:
            raise TypeError("Unexpected type for workflow_settings value")
        max_id = settings_json.get("max_id", 0)
    else:
        max_id = 0  # default if there is no entry yet
    print(f"Max ID: {max_id}")

    # Loading new values to df
    new_values_query = f"SELECT * FROM outbox WHERE id > {max_id};"
    new_values_df = pd.read_sql(new_values_query, source_engine)
    print(f"New values loaded: {len(new_values_df)} rows")

    if not new_values_df.empty:
        # Storing new values and updating max value
        with session.begin():
            new_values_df.to_sql('bonussystem_events', dwh_engine, schema='stg', if_exists='append', index=False)
            new_max_id_query = "SELECT max(id) FROM stg.bonussystem_events;"
            new_max_id = pd.read_sql(new_max_id_query, dwh_engine).iloc[0, 0]
            print(f"New Max ID: {new_max_id}")

            # Convert new_max_id to a standard Python integer
            new_max_id = int(new_max_id)

            # Prepare the new workflow_settings value
            new_workflow_settings = json.dumps({"max_id": f"{new_max_id}"})

            # Insert or update the srv_wf_settings table
            if settings_result.empty:
                # Insert new row
                insert_query = f"""
                    INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings)
                    VALUES ('load_tables_from_source_to_stg', '{new_workflow_settings}');
                """
                with dwh_engine.begin() as connection:
                    connection.execute(insert_query)
            else:
                # Update existing row
                update_query = f"""
                    UPDATE stg.srv_wf_settings
                    SET workflow_settings = '{new_workflow_settings}'
                    WHERE workflow_key = 'load_tables_from_source_to_stg';
                """
                with dwh_engine.begin() as connection:
                    connection.execute(update_query)

    session.close()
    source_engine.dispose()
    dwh_engine.dispose()

# Data for courier api connection
api_key = "25c27781-8fde-4b30-a22e-524044a7580f"
nickname = 'vbkovsky'
cohort = '27'
api_endpoint = "d5d04q7d963eapoepsqr.apigw.yandexcloud.net"

headers = {
    "X-API-KEY": api_key,
    "X-Nickname": nickname,
    "X-Cohort": cohort
}

def get_api_data(api_endpoint, headers, method_url, sort_field, dwh_table, dwh_scheme):
    offset = 0
    result_df = []
    response_df = []
    params = {
        'sort_field':sort_field,
        'sort_direction':'asc',
        'offset':offset
    }
    if method_url == '/deliveries':
        seven_days_ago = datetime.now() - timedelta(days=7)
        formatted_date = seven_days_ago.strftime("%Y-%m-%d %H:%M:%S")
        params['formatted_date'] = formatted_date

    while len(response_df) > 0 or offset == 0:
        r = requests.get('https://' + api_endpoint + method_url, params=params, headers=headers)
        response_df = json.loads(r.content)
        result_df += response_df
        offset += 50
    
    dwh_hook = PostgresHook(postgres_conn_id=DWH_conn_id)
    dwh_engine = dwh_hook.get_sqlalchemy_engine()

    pd.DataFrame(result_df).to_sql(dwh_table, dwh_engine, schema=dwh_scheme, if_exists='replace', index=False)
    dwh_engine.dispose()

# Define the DAG
with DAG(
        'load_tables_from_source_to_stg',
        schedule='1 * * * *',
        description='Loading tables from source DB to DWH',
        catchup=False,
        max_active_runs=1,
        start_date=pendulum.datetime(2024, 5, 30, tz="UTC")
) as dag:

    load_ranks_table_from_source_to_dwh_task = PythonOperator(
        task_id='load_ranks_table_from_source_to_dwh_task',
        python_callable=load_table_from_source_to_dwh,
        op_kwargs={'source_table': 'ranks', 'dwh_scheme': 'stg', 'dwh_table':'bonussystem_ranks'}
    )

    load_users_table_from_source_to_dwh_task = PythonOperator(
        task_id='load_users_table_from_source_to_dwh_task',
        python_callable=load_table_from_source_to_dwh,
        op_kwargs={'source_table': 'users', 'dwh_scheme': 'stg', 'dwh_table':'bonussystem_users'}
    )

    events_load = PythonOperator(
        task_id='events_load',
        python_callable=update_bonussystem_events
    )

    load_couriers_table_from_source_to_dwh_task = PythonOperator(
        task_id='load_couriers_table_from_source_to_dwh_task',
        python_callable=get_api_data,
        op_kwargs={'api_endpoint': api_endpoint , 'headers': headers, 'method_url': '/couriers', 'sort_field':'_id', 'dwh_table': 'couriers', 'dwh_scheme': 'stg'}
    )

    load_deliveries_table_from_source_to_dwh_task = PythonOperator(
        task_id='load_deliveries_table_from_source_to_dwh_task',
        python_callable=get_api_data,
        op_kwargs={'api_endpoint': api_endpoint , 'headers': headers, 'method_url': '/deliveries', 'sort_field':'delivery_ts', 'dwh_table': 'deliveries', 'dwh_scheme': 'stg'}
    )


    load_ranks_table_from_source_to_dwh_task >> load_users_table_from_source_to_dwh_task >> events_load >> load_couriers_table_from_source_to_dwh_task >> load_deliveries_table_from_source_to_dwh_task


