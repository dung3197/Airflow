import psycopg2
from psycopg2.extras import execute_batch

def call_pg_function_partition(rows):
    conn = psycopg2.connect(
        host="postgres-host",
        port=5432,
        database="test_map",
        user="test_map",
        password="your_password"
    )

    conn.autocommit = False
    cur = conn.cursor()

    sql = """
        SELECT public.safe_insert_back_account_map(
            %s, %s, %s, %s
        )
    """

    batch = []

    for r in rows:
        batch.append((
            r["c_customer_code"],
            r["c_account_code"],
            r["op_type"],
            r["current_ts"]
        ))

        if len(batch) >= 5000:
            execute_batch(cur, sql, batch, page_size=5000)
            conn.commit()
            batch.clear()

    if batch:
        execute_batch(cur, sql, batch, page_size=5000)
        conn.commit()

    cur.close()
    conn.close()


new_rows_df.foreachPartition(call_pg_function_partition)
