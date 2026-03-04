from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
import pandas as pd
import json
import os
from sqlalchemy import create_engine, text

# --- CONFIGURATION & ENVIRONMENT ---
# Using environment variables to avoid hardcoding sensitive credentials.
# These should be defined in your docker-compose.yml or .env file.
MYSQL_URI = os.environ.get('MYSQL_CONN')
POSTGRES_URI = os.environ.get('PG_CONN')
DBT_PROJECT_DIR = os.environ.get('DBT_ROOT', '/opt/airflow/dbt')

TABLES_TO_INGEST = [
    'ordenes', 'cambios_estados_ordenes', 'remito_movimientos',
    'instrumentos_clientes', 'clientes', 'users', 'estados_ordenes',
    'modelos', 'marcas', 'tipos_instrumentos', 'remitos_prueba', 'notas_ingreso_prueba'
]

def run_append_only_ingestion():
    """
    Extracts from MySQL and loads into Postgres using Append-only.
    Includes a self-healing check to ensure the technical metadata column exists.
    """
    if not MYSQL_URI or not POSTGRES_URI:
        raise ValueError("Database connection strings not found in environment variables.")

    mysql_engine = create_engine(MYSQL_URI)
    pg_engine = create_engine(POSTGRES_URI)
    
    with pg_engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS audit;"))

    for table_name in TABLES_TO_INGEST:
        print(f"Ingesting table: {table_name}")
        
        df = pd.read_sql(f"SELECT * FROM {table_name}", mysql_engine)
        df['_ingested_at'] = datetime.now()
        
        # --- SELF-HEALING BLOCK: Check and Add Column ---
        # We verify if '_ingested_at' exists in the target table to avoid ProgrammingErrors
        with pg_engine.begin() as pg_conn:
            # Check if table exists first
            check_table_sql = text(f"""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'raw' AND table_name = '{table_name}'
                );
            """)
            table_exists = pg_conn.execute(check_table_sql).scalar()

            if table_exists:
                # If table exists, ensure the metadata column exists
                add_col_sql = text(f"ALTER TABLE raw.{table_name} ADD COLUMN IF NOT EXISTS _ingested_at TIMESTAMP;")
                pg_conn.execute(add_col_sql)
            
            # Load: if_exists='append' now works because the schema is guaranteed
            df.to_sql(
                name=table_name, 
                con=pg_conn, 
                schema='raw', 
                if_exists='append', 
                index=False
            )
        print(f"Successfully persisted {len(df)} rows for {table_name}.")

def parse_dbt_execution_results():
    """
    Parses the dbt run_results.json artifact to extract test failures and 
    warnings, persisting them into the audit schema for long-term tracking.
    """
    results_path = os.path.join(DBT_PROJECT_DIR, 'target/run_results.json')
    
    if not os.path.exists(results_path):
        print("No dbt artifact found. Skipping audit parsing.")
        return

    with open(results_path, 'r') as f:
        data = json.load(f)
    
    audit_records = []
    for result in data['results']:
        if result['status'] in ['warn', 'fail']:
            # Normalize test name for easier reporting in BI tools
            node_id = result['unique_id'].split('.')[-1]
            
            audit_records.append({
                'test_id': node_id,
                'status': result['status'],
                'error_message': result['message'],
                'executed_at': data['metadata']['generated_at']
            })
    
    if audit_records:
        df = pd.DataFrame(audit_records)
        engine = create_engine(POSTGRES_URI)
        # Persistent log of quality breaches
        df.to_sql(
            'audit_log', 
            engine, 
            schema='audit', 
            if_exists='append', 
            index=False
        )

# --- DAG DEFINITION ---
with DAG(
    dag_id='iso_17025_lab_observability',
    start_date=datetime(2025, 1, 1),
    schedule='@daily', 
    catchup=False,
    tags=['production', 'compliance', 'iso17025'],
    doc_md="""
    ### ISO 17025 Data Observability Pipeline
    This DAG manages the movement of laboratory data, enforcing an **Append-only** ingestion pattern to maintain data integrity and audit trails.
    """
) as dag:

    ingest_raw = PythonOperator(
        task_id='ingest_legacy_data',
        python_callable=run_append_only_ingestion
    )
    
    check_freshness = BashOperator(
        task_id='dbt_source_freshness',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt source freshness"
    )

    dbt_build = BashOperator(
        task_id='dbt_transformation_build',
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt build"
    )

    # Note: trigger_rule='all_done' ensures we record failures even if dbt_build fails
    persist_audit = PythonOperator(
        task_id='persist_test_audit',
        python_callable=parse_dbt_execution_results,
        trigger_rule='all_done' 
    )

    ingest_raw >> check_freshness >> dbt_build >> persist_audit