import oracledb
import random
from faker import Faker
from datetime import datetime, timedelta
import os

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
    # Create log file with timestamp
    log_file = f"customer_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    try:
        # Establish Oracle connection with timeout
        print("Connecting to database...")
        connection = oracledb.connect(**db_config)
        cursor = connection.cursor()
        print("Connected successfully")

        # Open log file and write header
        with open(log_file, 'w') as f:
            f.write(f"Customer Update/Delete Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # Check for table locks
            print("Checking for table locks...")
            cursor.execute("""
                SELECT COUNT(*) 
                FROM v$lock l
                JOIN dba_objects o ON l.id1 = o.object_id
                WHERE o.object_name = 'CUSTOMER_INFORMATION'
            """)
            lock_count = cursor.fetchone()[0]
            if lock_count > 0:
                f.write(f"WARNING: Found {lock_count} locks on customer_information table\n")
                print(f"WARNING: Found {lock_count} locks on customer_information table")

            # 1. Update 100 random records in batches of 10
            print("Selecting 100 customer IDs for update...")
            cursor.execute("""
                SELECT customer_id, credit_score, account_status, investment_objective
                FROM customer_information 
                WHERE ROWNUM <= 100
            """)
            original_records = cursor.fetchall()
            customer_ids = [row[0] for row in original_records]

            if len(customer_ids) < 100:
                warning = f"WARNING: Only found {len(customer_ids)} records to update\n"
                f.write(warning)
                print(warning)

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
            
            f.write("Updated Records:\n")
            f.write("-" * 50 + "\n")
            
            for i in range(0, len(customer_ids), batch_size):
                batch = []
                print(f"Preparing update batch {i//batch_size + 1}...")
                
                for j in range(i, min(i + batch_size, len(customer_ids))):
                    cid = customer_ids[j]
                    old_record = original_records[j]
                    new_credit_score = random.randint(300, 850)
                    new_status = random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED', 'CLOSED'])
                    new_investment_objective = random.choice(['GROWTH', 'INCOME', 'PRESERVATION', 'SPECULATIVE'])

                    # Log update details to file
                    f.write(f"Customer ID: {cid}\n")
                    f.write(f"Old Values:\n")
                    f.write(f"  Credit Score: {old_record[1]}\n")
                    f.write(f"  Status: {old_record[2]}\n")
                    f.write(f"  Investment Objective: {old_record[3]}\n")
                    f.write(f"New Values:\n")
                    f.write(f"  Credit Score: {new_credit_score}\n")
                    f.write(f"  Status: {new_status}\n")
                    f.write(f"  Investment Objective: {new_investment_objective}\n")
                    f.write("-" * 50 + "\n")

                    batch.append([
                        new_credit_score,
                        new_status,
                        new_investment_objective,
                        datetime.now(),
                        fake.user_name(),
                        cid
                    ])

                # Execute batch update
                print(f"Executing update for {len(batch)} records...")
                cursor.executemany(update_sql, batch)
                connection.commit()
                updated_count += len(batch)
                f.write(f"Batch committed, total updated: {updated_count}\n")
                print(f"Batch committed, total updated: {updated_count}")

            f.write(f"\nTotal records updated: {updated_count}\n\n")
            print(f"Successfully updated {updated_count} records")

            # 2. Delete 10 random records
            print("Selecting 10 customer IDs for deletion...")
            cursor.execute("SELECT customer_id FROM customer_information WHERE ROWNUM <= 10")
            delete_customer_ids = [row[0] for row in cursor.fetchall()]

            if len(delete_customer_ids) < 10:
                warning = f"WARNING: Only found {len(delete_customer_ids)} records to delete\n"
                f.write(warning)
                print(warning)

            delete_sql = """
            DELETE FROM customer_information
            WHERE customer_id = :1
            """

            f.write("Deleted Records:\n")
            f.write("-" * 50 + "\n")
            
            for cid in delete_customer_ids:
                f.write(f"Customer ID: {cid}\n")
                f.write("-" * 50 + "\n")
                print(f"Deleting Customer ID: {cid}")
                cursor.execute(delete_sql, (cid,))
                connection.commit()
            
            f.write(f"\nTotal records deleted: {len(delete_customer_ids)}\n")
            print(f"Successfully deleted {len(delete_customer_ids)} records")

    except oracledb.DatabaseError as e:
        error, = e.args
        error_msg = f"Database error: {error.code} - {error.message}\n"
        print(error_msg)
        with open(log_file, 'a') as f:
            f.write(error_msg)
        connection.rollback()
    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}\n"
        print(error_msg)
        with open(log_file, 'a') as f:
            f.write(error_msg)
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print(f"Database connection closed. Log file created: {log_file}")
        with open(log_file, 'a') as f:
            f.write("Database connection closed\n")

if __name__ == "__main__":
    update_and_delete_records()
