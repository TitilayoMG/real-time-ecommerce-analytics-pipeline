from airflow.providers.postgres.hooks.postgres import PostgresHook
import subprocess
import logging

log = logging.getLogger(__name__)

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"

def run_dbt_layers():
    models = ["bronze", "silver", "gold"]

    for model in models:
        command = f"dbt run --select {model} --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}"
    
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True,
            cwd="/opt/airflow/dbt"
        )

        log.info(result.stdout)

        if result.stderr:
            logging.warning(result.stderr)

        if result.returncode != 0:
            raise Exception(f"dbt run failed for {model}")


def check_data_quality():
    postgres = PostgresHook(postgres_conn_id="redshift_conn")

    checks = {
        "duplicate_transaction": """
            SELECT COUNT(*)
            FROM (
                SELECT transaction
                FROM stream_schema.fct_order
                GROUP BY transaction
                HAVING COUNT(*) > 1
            ) t
        """,

        "invalid_country": """
            SELECT COUNT(*)
            FROM stream_schema.dim_customer
            WHERE country NOT IN ('US', 'UK', 'Canada', 'France', 'Denmark', 'Australia')
        """,

        "negative_quantity": """
            SELECT COUNT(*)
            FROM stream_schema.fct_order
            WHERE quantity < 0
        """
    }

    for check_name, query in checks.items():
        result = postgres.get_first(query)[0]
        log.info(f"{query}: {result}")

        if result > 0:
            raise ValueError(
                f"Data quality failed: {check_name} has {result} bad rows"
            )