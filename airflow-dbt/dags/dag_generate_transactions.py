from airflow.sdk import DAG, task
from datetime import timedelta
import pendulum
from faker import Faker
import random
import uuid
from include.telegram_sender import send_failed_message, send_success_message

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': pendulum.datetime(2023, 1, 1, tz="Asia/Jakarta"),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'generate_retail_transactions',
    default_args=default_args,
    description='Generate dummy retail transactions to PostgreSQL',
    schedule='55 * * * *',
    catchup=False,
    is_paused_upon_creation=True,
    on_failure_callback=send_failed_message,
    on_success_callback=send_success_message,
    tags=['lion_parcel', 'data_generator'],
) as dag:

    @task
    def inject_data():
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        
        # Connect to DB using Airflow Connection
        hook = PostgresHook(postgres_conn_id='postgres_default')
        conn = hook.get_conn()
        conn.autocommit = True
        cur = conn.cursor()

        # 1. Create table and trigger if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS retail_transactions (
            "id" VARCHAR(50) PRIMARY KEY,
            "customerId" VARCHAR(50),
            "lastStatus" VARCHAR(50),
            "posOrigin" VARCHAR(100),
            "posDestination" VARCHAR(100),
            "createdAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            "updatedAt" TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            "deletedAt" TIMESTAMPTZ NULL
        );
        """
        cur.execute(create_table_sql)

        create_func_sql = """
        CREATE OR REPLACE FUNCTION set_updated_at()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW."updatedAt" = CURRENT_TIMESTAMP;
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
        """
        cur.execute(create_func_sql)

        # Drop trigger if exists to avoid errors, then create
        cur.execute("DROP TRIGGER IF EXISTS trigger_update_timestamp ON retail_transactions;")
        
        create_trigger_sql = """
        CREATE TRIGGER trigger_update_timestamp
        BEFORE UPDATE ON retail_transactions
        FOR EACH ROW
        EXECUTE FUNCTION set_updated_at();
        """
        cur.execute(create_trigger_sql)

        fake = Faker('id_ID')
        statuses = ['BOOKED', 'PROCESSING', 'IN_TRANSIT', 'OUT_FOR_DELIVERY', 'DELIVERED', 'CANCELLED']
        
        # 2. Insert new data (simulate 10-50 new transactions)
        num_inserts = random.randint(10, 50)
        insert_count = 0
        for _ in range(num_inserts):
            t_id = f"TRX-{uuid.uuid4().hex[:8].upper()}"
            cust_id = f"CUST-{random.randint(1000, 9999)}"
            status = 'BOOKED' # new transactions start here
            origin = fake.city_name()
            dest = fake.city_name()
            
            cur.execute(
                """
                INSERT INTO retail_transactions ("id", "customerId", "lastStatus", "posOrigin", "posDestination")
                VALUES (%s, %s, %s, %s, %s)
                """,
                (t_id, cust_id, status, origin, dest)
            )
            insert_count += 1
            
        # 3. Update existing data (Progress through the funnel)
        cur.execute("SELECT \"id\", \"lastStatus\" FROM retail_transactions WHERE \"lastStatus\" NOT IN ('DELIVERED', 'CANCELLED') AND \"deletedAt\" IS NULL")
        active_txs = cur.fetchall()
        
        update_count = 0
        cancel_count = 0
        for tx in active_txs:
            if random.random() < 0.5: # 50% chance to update
                if random.random() < 0.10: # 10% chance to be cancelled
                    cur.execute(
                        "UPDATE retail_transactions SET \"lastStatus\" = 'CANCELLED' WHERE \"id\" = %s",
                        (tx[0],)
                    )
                    cancel_count += 1
                else:
                    # Transition logic
                    current_status = tx[1]
                    if current_status == 'BOOKED':
                        new_status = 'PROCESSING'
                    elif current_status == 'PROCESSING':
                        new_status = 'IN_TRANSIT'
                    elif current_status == 'IN_TRANSIT':
                        new_status = 'OUT_FOR_DELIVERY'
                    else:
                        new_status = 'DELIVERED'
                        
                    cur.execute(
                        "UPDATE retail_transactions SET \"lastStatus\" = %s WHERE \"id\" = %s",
                        (new_status, tx[0])
                    )
                    update_count += 1
                
        # 4. Soft delete data (mark deletedAt for some DELIVERED transactions as voided/archived)
        cur.execute("SELECT \"id\" FROM retail_transactions WHERE \"lastStatus\" = 'DELIVERED' AND \"deletedAt\" IS NULL")
        done_txs = cur.fetchall()
        
        delete_count = 0
        for tx in done_txs:
            if random.random() < 0.15: # 15% chance to soft-delete
                cur.execute(
                    "UPDATE retail_transactions SET \"deletedAt\" = CURRENT_TIMESTAMP WHERE \"id\" = %s",
                    (tx[0],)
                )
                delete_count += 1

        cur.close()
        conn.close()

        print(f"Injected {insert_count} new transactions.")
        print(f"Updated {update_count} active transactions.")
        print(f"Cancelled {cancel_count} transactions.")
        print(f"Soft deleted {delete_count} done transactions.")

    inject_data()
