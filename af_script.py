import time
import requests

# --- Configuration ---
AIRFLOW_BASE_URL = "http://localhost:8080/api/v1"  # change this
USERNAME = "admin"
PASSWORD = "admin"
CHECK_INTERVAL = 300  # seconds

def get_running_dagruns():
    url = f"{AIRFLOW_BASE_URL}/dags?only_active=true"
    resp = requests.get(url, auth=(USERNAME, PASSWORD))
    resp.raise_for_status()
    dag_ids = [dag["dag_id"] for dag in resp.json().get("dags", [])]

    running_dagruns = []

    for dag_id in dag_ids:
        dagruns_url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns?state=running&limit=100"
        resp = requests.get(dagruns_url, auth=(USERNAME, PASSWORD))
        resp.raise_for_status()
        dag_runs = resp.json().get("dag_runs", [])
        for run in dag_runs:
            running_dagruns.append((dag_id, run["dag_run_id"]))
    return running_dagruns

def mark_dagrun_failed(dag_id, dag_run_id):
    url = f"{AIRFLOW_BASE_URL}/dags/{dag_id}/dagRuns/{dag_run_id}"
    payload = {
        "state": "failed"
    }
    resp = requests.patch(url, json=payload, auth=(USERNAME, PASSWORD))
    if resp.ok:
        print(f"✅ Marked DAG run {dag_id}:{dag_run_id} as failed.")
    else:
        print(f"❌ Failed to mark {dag_id}:{dag_run_id} as failed: {resp.text}")

def monitor_loop():
    while True:
        print("🔄 Checking for running DAGs...")
        try:
            running_dagruns = get_running_dagruns()
            if not running_dagruns:
                print("✅ No running DAGs found.")
            for dag_id, run_id in running_dagruns:
                print(f"⚠️ DAG run in running state: {dag_id}:{run_id}")
                mark_dagrun_failed(dag_id, run_id)
        except Exception as e:
            print(f"❌ Error during check: {e}")
        time.sleep(CHECK_INTERVAL)

# --- Start Monitoring ---
monitor_loop()











from pyspark.sql.functions import col

def get_final_values(ogg_df, op, business_fields):
    if op == "I" or op == "U" or op == "R":
        mapping = {f"after.{f}": f for f in business_fields}
    elif op == "D":
        mapping = {f"before.{f}": f for f in business_fields}
    else:
        return ogg_df

    # Build select expressions for all fields
    select_exprs = [col(src).alias(dest) for src, dest in mapping.items()]

    # Keep original columns if needed
    other_cols = [c for c in ogg_df.columns if not (c.startswith("before.") or c.startswith("after."))]
    select_exprs.extend([col(c) for c in other_cols])

    result_df = ogg_df.select(*select_exprs)
    print("Finish get final values")
    return result_df
