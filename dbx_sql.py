"""Execute SQL statements against Databricks warehouse with polling."""
import json, sys, subprocess, time

DBX_TOKEN = "dapib860acdc3182474716e54de41dfd0be6"
DBX_HOST = "https://adb-1169784117228619.19.azuredatabricks.net"
DBX_WAREHOUSE = "a7e9ada5cd37e1c7"

def _curl(method, url, data=None):
    cmd = ["curl", "-s", "-X", method, url,
           "-H", f"Authorization: Bearer {DBX_TOKEN}",
           "-H", "Content-Type: application/json"]
    if data:
        cmd += ["-d", data]
    r = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace")
    return json.loads(r.stdout)

def run_sql(stmt, label="query"):
    payload = json.dumps({
        "warehouse_id": DBX_WAREHOUSE,
        "statement": stmt,
        "wait_timeout": "50s"
    })
    r = _curl("POST", f"{DBX_HOST}/api/2.0/sql/statements", payload)

    if "error_code" in r:
        print(f"FAILED [{label}]: {r['error_code']} — {r.get('message','')[:300]}")
        return None

    stmt_id = r.get("statement_id")
    status = r.get("status", {}).get("state", "???")

    # Poll if PENDING or RUNNING
    while status in ("PENDING", "RUNNING"):
        time.sleep(3)
        r = _curl("GET", f"{DBX_HOST}/api/2.0/sql/statements/{stmt_id}")
        status = r.get("status", {}).get("state", "???")

    if status != "SUCCEEDED":
        err = r.get("status", {}).get("error", {}).get("message", "no message")
        print(f"FAILED [{label}]: {status} — {err[:300]}")
        return None

    data = r.get("result", {}).get("data_array", [])
    cols = [c["name"] for c in r.get("manifest", {}).get("schema", {}).get("columns", [])]
    print(f"OK [{label}]: {len(data)} rows")
    return {"columns": cols, "data": data}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python dbx_sql.py 'SQL statement' [label]")
        sys.exit(1)
    result = run_sql(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else "query")
    if result and result["data"]:
        print(f"  Columns: {result['columns']}")
        for row in result["data"][:5]:
            print(f"  {row}")
