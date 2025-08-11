import oracledb
import random
from faker import Faker
from datetime import datetime, timedelta
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"customer_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize Faker for generating realistic data
fake = Faker()

# Database connection parameters (update with your Oracle DB details)
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'your_host:your_port/your_service_name',
    'timeout': 30  # Connection timeout in seconds
}

def update_and_delete_records():
    try:
        # Establish Oracle connection with timeout
        logger.info("Connecting to database...")
        connection = oracledb.connect(**db_config)
        cursor = connection.cursor()
        logger.info("Connected successfully")

        # Check for table locks
        logger.info("Checking for table locks...")
        cursor.execute("""
            SELECT COUNT(*) 
            FROM v$lock l
            JOIN dba_objects o ON l.id1 = o.object_id
            WHERE o.object_name = 'CUSTOMER_INFORMATION'
        """)
        lock_count = cursor.fetchone()[0]
        if lock_count > 0:
            logger.warning(f"Found {lock_count} locks on customer_information table")

        # 1. Update 100 random records in batches of 10
        logger.info("Selecting 100 customer IDs for update...")
        cursor.execute("""
            SELECT customer_id, credit_score, account_status, investment_objective
            FROM customer_information 
            WHERE ROWNUM <= 100
        """)
        original_records = cursor.fetchall()
        customer_ids = [row[0] for row in original_records]

        if len(customer_ids) < 100:
            logger.warning(f"Only found {len(customer_ids)} records to update")

        update_sql = """
        UPDATE customer_information
        SET 
            credit_score = :1,
            account_status = :2,
            investment_objective = :3,
            updated_date = :4,
            updated_by = :5
        WHERE customer_id = :6
        """

        batch_size = 10
        updated_count = 0
        
        for i in range(0, len(customer_ids), batch_size):
            batch = []
            logger.info(f"Preparing update batch {i//batch_size + 1}...")
            
            for j in range(i, min(i + batch_size, len(customer_ids))):
                cid = customer_ids[j]
                old_record = original_records[j]
                new_credit_score = random.randint(300, 850)
                new_status = random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED', 'CLOSED'])
                new_investment_objective = random.choice(['GROWTH', 'INCOME', 'PRESERVATION', 'SPECULATIVE'])

                # Log update details
                logger.info(f"Updating Customer ID: {cid}")
                logger.info(f"Old Values: Credit Score={old_record[1]}, Status={old_record[2]}, Investment Objective={old_record[3]}")
                logger.info(f"New Values: Credit Score={new_credit_score}, Status={new_status}, Investment Objective={new_investment_objective}")

                batch.append([
                    new_credit_score,
                    new_status,
                    new_investment_objective,
                    datetime.now(),
                    fake.user_name(),
                    cid
                ])

            # Execute batch update
            logger.info(f"Executing update for {len(batch)} records...")
            cursor.executemany(update_sql, batch)
            connection.commit()
            updated_count += len(batch)
            logger.info(f"Batch committed, total updated: {updated_count}")

        logger.info(f"Successfully updated {updated_count} records")

        # 2. Delete 10 random records
        logger.info("Selecting 10 customer IDs for deletion...")
        cursor.execute("SELECT customer_id FROM customer_information WHERE ROWNUM <= 10")
        delete_customer_ids = [row[0] for row in cursor.fetchall()]

        if len(delete_customer_ids) < 10:
            logger.warning(f"Only found {len(delete_customer_ids)} records to delete")

        delete_sql = """
        DELETE FROM customer_information
        WHERE customer_id = :1
        """

        logger.info("Deleting records...")
        for cid in delete_customer_ids:
            logger.info(f"Deleting Customer ID: {cid}")
            cursor.execute(delete_sql, (cid,))
            connection.commit()
        
        logger.info(f"Successfully deleted {len(delete_customer_ids)} records")

    except oracledb.DatabaseError as e:
        error, = e.args
        logger.error(f"Database error: {error.code} - {error.message}")
        connection.rollback()
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        logger.info("Database connection closed")

if __name__ == "__main__":
    update_and_delete_records()
