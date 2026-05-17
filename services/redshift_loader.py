import logging
import psycopg2
from airflow.hooks.base import BaseHook
from airflow.models import Variable

log = logging.getLogger(__name__)


def load_data_from_s3_to_redshift(**context):
    """
    Single Airflow task:
    1. Connect to Redshift
    2. Create table if not exists
    3. Optionally clear table (prevents duplicates)
    4. COPY data from S3 into Redshift
    """

    conn = None

    try:
        # ----------------------------
        # 1. CONNECT TO REDSHIFT
        # ----------------------------
        log.info("Step 1: Connecting to Redshift...")

        conn_info = BaseHook.get_connection("redshift_conn")

        conn = psycopg2.connect(
            dbname=conn_info.schema,
            host=conn_info.host,
            port=conn_info.port,
            user=conn_info.login,
            password=conn_info.password,
            sslmode="require"
        )
       
        log.info("Connected to Redshift successfully")

        # ----------------------------
        # 2. CREATE TABLE IF NOT EXISTS
        # ----------------------------
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS stream_store_raw (
            transaction VARCHAR(20),
            order_date DATE,
            customer_id INTEGER,
            customer_name VARCHAR(100),
            country VARCHAR(50),
            product_id INTEGER,
            product_category VARCHAR(50),
            quantity INTEGER,
            price NUMERIC(10,2),
            payment_method VARCHAR(50),
            order_status VARCHAR(20)
        );
        """


        log.info("Step 2: Creating table if not exists...")

        with conn.cursor() as cur:
            cur.execute(create_table_sql)
            conn.commit()

        print("✔ Table ensured")
        log.info("Table ensured successfully")

        # ----------------------------
        # 4. COPY FROM S3 TO REDSHIFT
        # ----------------------------
        s3_path = Variable.get("s3_path")
        iam_role = Variable.get("iam_role")

        copy_sql = f"""
        COPY stream_store_raw
        FROM '{s3_path}'
        IAM_ROLE '{iam_role}'
        FORMAT AS CSV
        IGNOREHEADER 1
        DELIMITER ','
        TIMEFORMAT 'auto'
        REGION 'ap-south-1';
        """
        log.info("Step 3: Copying data from S3 to Redshift...")

        with conn.cursor() as cur:
            cur.execute(copy_sql)
            conn.commit()

        print("✔ COPY completed successfully from S3 to Redshift")
        log.info("COPY completed successfully")

    except Exception as e:
        log.error(f"Pipeline failed: {e}")

        if conn:
            conn.rollback()
        print(f"❌ Pipeline failed: {e}")
        raise

    finally:
        if conn:
            conn.close()
            log.info("Redshift connection closed") # LOPKmrzjb132%%