import oracledb
import random
from faker import Faker
from datetime import datetime
import os

# Initialize Faker for generating realistic data
fake = Faker()

# Database connection parameters (update with your Oracle DB details)
db_config = {
    'user': 'your_username',
    'password': 'your_password',
    'dsn': 'your_host:your_port/your_service_name'
}

def update_and_delete_records():
    # Create log file with timestamp
    log_file = f"transaction_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    
    try:
        # Establish Oracle connection
        connection = oracledb.connect(**db_config)
        cursor = connection.cursor()

        # Open log file
        with open(log_file, 'w') as f:
            f.write(f"Transaction Update/Delete Log - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

            # 1. Update 100 random records
            # First, get 100 random transaction_ids with relevant fields for logging
            cursor.execute("""
                SELECT transaction_id, amount, transaction_status, notes
                FROM cash_transaction_history 
                WHERE ROWNUM <= 100
            """)
            original_records = cursor.fetchall()
            transaction_ids = [row[0] for row in original_records]

            update_sql = """
            UPDATE cash_transaction_history
            SET 
                amount = :1,
                transaction_status = :2,
                notes = :3,
                updated_date = :4,
                updated_by = :5
            WHERE transaction_id = :6
            """

            update_records = []
            f.write("Updated Records:\n")
            f.write("-" * 50 + "\n")
            
            for i, tid in enumerate(transaction_ids):
                old_record = original_records[i]
                new_amount = round(random.uniform(100.00, 1000000.00), 2)
                new_status = random.choice(['PENDING', 'COMPLETED', 'CANCELLED'])
                new_notes = fake.sentence(nb_words=10)
                
                # Log update details
                f.write(f"Transaction ID: {tid}\n")
                f.write(f"Old Values:\n")
                f.write(f"  Amount: {old_record[1]}\n")
                f.write(f"  Status: {old_record[2]}\n")
                f.write(f"  Notes: {old_record[3]}\n")
                f.write(f"New Values:\n")
                f.write(f"  Amount: {new_amount}\n")
                f.write(f"  Status: {new_status}\n")
                f.write(f"  Notes: {new_notes}\n")
                f.write("-" * 50 + "\n")

                update_records.append([
                    new_amount,  # New amount
                    new_status,  # New status
                    new_notes,  # New notes
                    datetime.now(),  # Updated date
                    fake.user_name(),  # Updated by
                    tid  # Transaction ID
                ])

            # Execute updates
            cursor.executemany(update_sql, update_records)
            connection.commit()
            print(f"Successfully updated {len(update_records)} records")
            f.write(f"\nTotal records updated: {len(update_records)}\n\n")

            # 2. Delete 10 random records
            # Get 10 different random transaction_ids
            cursor.execute("SELECT transaction_id FROM cash_transaction_history WHERE ROWNUM <= 10")
            delete_transaction_ids = [row[0] for row in cursor.fetchall()]

            delete_sql = """
            DELETE FROM cash_transaction_history
            WHERE transaction_id = :1
            """

            f.write("Deleted Records:\n")
            f.write("-" * 50 + "\n")
            for tid in delete_transaction_ids:
                f.write(f"Transaction ID: {tid}\n")
                f.write("-" * 50 + "\n")

            # Execute deletes
            cursor.executemany(delete_sql, [(tid,) for tid in delete_transaction_ids])
            connection.commit()
            print(f"Successfully deleted {len(delete_transaction_ids)} records")
            f.write(f"\nTotal records deleted: {len(delete_transaction_ids)}\n")

    except oracledb.Error as e:
        print(f"Database error: {e}")
        with open(log_file, 'a') as f:
            f.write(f"\nError occurred: {e}\n")
        connection.rollback()
    finally:
        cursor.close()
        connection.close()
        print(f"Log file created: {log_file}")

if __name__ == "__main__":
    update_and_delete_records()
