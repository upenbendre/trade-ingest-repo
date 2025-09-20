from datetime import datetime, timedelta
import os
import time
import json
import logging
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOG = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": True,
    "email": ["sre-team@example.com"],
}

def _validate_manifest(**context):
    conf = context.get("dag_run").conf or {}
    manifest_path = conf.get("manifest_path")
    correlation_id = conf.get("correlation_id")
    if not manifest_path or not correlation_id:
        raise ValueError("manifest_path and correlation_id are required in dag_run.conf")
    # Optionally: validate manifest JSON schema by fetching the file from S3 (omitted)
    context["ti"].xcom_push(key="manifest_path", value=manifest_path)
    context["ti"].xcom_push(key="correlation_id", value=correlation_id)
    LOG.info("Validated manifest_path=%s correlation_id=%s", manifest_path, correlation_id)
    return True

def _upload_jar_to_flink(**context):
    """
    Upload a local Flink jar to the Flink JobManager via REST API /jars/upload.
    Expects environment variable FLINK_JAR_FILE to point to the jar on the Airflow worker filesystem
    and uses Airflow connection 'flink_api' to determine base_url and auth.
    Returns jar_id (basename of uploaded file) and pushes it to XCom.
    """
    upload_enabled = os.environ.get("UPLOAD_JAR", "false").lower() == "true"
    if not upload_enabled:
        LOG.info("Jar upload disabled (UPLOAD_JAR != 'true'), skipping upload step.")
        return None

    jar_file = os.environ.get("FLINK_JAR_FILE")
    if not jar_file or not os.path.exists(jar_file):
        raise ValueError("FLINK_JAR_FILE must point to an existing jar file on the worker when UPLOAD_JAR=true")

    conn = BaseHook.get_connection("flink_api")
    flink_base = conn.host
    if conn.port:
        flink_base = f"{flink_base}:{conn.port}"
    # Determine schema from connection (conn.schema often used for http/https scheme)
    scheme = conn.schema or "http"
    upload_url = f"{scheme}://{flink_base}/jars/upload"

    LOG.info("Uploading jar %s to Flink upload endpoint %s", jar_file, upload_url)

    # Use requests directly to post multipart file upload; include basic auth if present in connection
    auth = None
    if conn.login and conn.password:
        auth = (conn.login, conn.password)

    with open(jar_file, "rb") as fh:
        files = {"jarfile": (os.path.basename(jar_file), fh, "application/java-archive")}
        resp = requests.post(upload_url, files=files, auth=auth, timeout=120)
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to upload jar to Flink: {resp.status_code} {resp.text}")
    body = resp.json()
    # Flink returns 'filename' like "/tmp/flink-web-.../job.jar" - use basename as jarid
    filename = body.get("filename") or body.get("fileName") or ""
    jar_id = os.path.basename(filename) if filename else None

    if not jar_id:
        # some Flink versions return {"status":"success","data":{"filename":"..."}}
        data = body.get("data") or {}
        filename = data.get("filename") or data.get("fileName")
        jar_id = os.path.basename(filename) if filename else None

    if not jar_id:
        raise Exception(f"Could not determine jar id from Flink upload response: {body}")

    LOG.info("Uploaded jar, determined jar_id=%s", jar_id)
    context["ti"].xcom_push(key="flink_jar_id", value=jar_id)
    return jar_id

def _submit_via_ingest_controller(manifest_path, correlation_id, http_conn_id="ingest_controller_api"):
    hook = HttpHook(method="POST", http_conn_id=http_conn_id)
    payload = {"manifest_path": manifest_path, "correlation_id": correlation_id}
    resp = hook.run(endpoint="/submit-bulk", data=json.dumps(payload), headers={"Content-Type": "application/json"})
    if resp.status_code not in (200, 201):
        raise Exception(f"Failed to submit bulk job to ingest controller: {resp.status_code} {resp.text}")
    body = resp.json()
    job_id = body.get("job_id") or body.get("jobId")
    if not job_id:
        raise Exception("ingest_controller_api did not return job_id")
    return job_id

def _submit_via_flink_rest(manifest_path, correlation_id, jar_id, http_conn_id="flink_api", entry_class=None, program_args_override=None):
    """
    Submit job using Flink REST API /jars/{jarid}/run
    """
    if not jar_id:
        raise ValueError("jar_id is required for direct Flink REST submission")

    conn = BaseHook.get_connection(http_conn_id)
    host = conn.host
    if conn.port:
        host = f"{host}:{conn.port}"
    scheme = conn.schema or "http"
    run_url = f"{scheme}://{host}/jars/{jar_id}/run"

    run_conf = {}
    if entry_class:
        run_conf["entryClass"] = entry_class

    # build program args that the Flink job will see
    if program_args_override:
        run_conf["programArgs"] = program_args_override
    else:
        # Flink expects programArgs as a single string
        run_conf["programArgs"] = f'--manifest_path "{manifest_path}" --correlation_id "{correlation_id}"'

    LOG.info("Submitting Flink jar %s to %s with args %s", jar_id, run_url, run_conf.get("programArgs"))

    auth = None
    if conn.login and conn.password:
        auth = (conn.login, conn.password)

    resp = requests.post(run_url, json=run_conf, auth=auth, timeout=30)
    if resp.status_code not in (200, 202):
        raise Exception(f"Failed to run Flink jar: {resp.status_code} {resp.text}")
    body = resp.json()
    # extract job id (varies by Flink version)
    job_id = body.get("jobid") or body.get("jobId") or (body.get("jobs") and body["jobs"][0].get("id"))
    if not job_id and isinstance(body.get("result"), dict):
        job_id = body["result"].get("jobid") or body["result"].get("jobId")
    if not job_id:
        raise Exception(f"Could not determine job_id from Flink response: {body}")
    return job_id

def _trigger_bulk_job(**context):
    ti = context["ti"]
    manifest_path = ti.xcom_pull(key="manifest_path")
    correlation_id = ti.xcom_pull(key="correlation_id")

    use_ingest_controller = os.environ.get("USE_INGEST_CONTROLLER", "true").lower() == "true"
    use_direct_flink = os.environ.get("USE_DIRECT_FLINK", "false").lower() == "true"

    # Optionally upload jar first (if enabled)
    jar_id = None
    upload_enabled = os.environ.get("UPLOAD_JAR", "false").lower() == "true"
    if upload_enabled:
        jar_id = ti.xcom_pull(key="flink_jar_id")
        if not jar_id:
            jar_id = _upload_jar_to_flink(**context)  # pushes to xcom internally

    if use_ingest_controller:
        job_id = _submit_via_ingest_controller(manifest_path, correlation_id, http_conn_id="ingest_controller_api")
    elif use_direct_flink:
        # require jar_id either from env or earlier upload
        jar_id = jar_id or os.environ.get("FLINK_JAR_ID")
        if not jar_id:
            raise ValueError("FLINK_JAR_ID must be provided when USE_DIRECT_FLINK=true and no upload was performed")
        job_id = _submit_via_flink_rest(manifest_path, correlation_id, jar_id, http_conn_id="flink_api")
    else:
        raise ValueError("No submission method selected. Set USE_INGEST_CONTROLLER or USE_DIRECT_FLINK in env.")

    LOG.info("Submitted bulk job; job_id=%s", job_id)
    ti.xcom_push(key="job_id", value=job_id)
    return job_id

def _poll_job_status(job_id, http_conn_id="flink_api", timeout_minutes=120, poll_interval=15):
    conn = BaseHook.get_connection(http_conn_id)
    host = conn.host
    if conn.port:
        host = f"{host}:{conn.port}"
    scheme = conn.schema or "http"
    status_url = f"{scheme}://{host}/jobs/{job_id}"

    auth = None
    if conn.login and conn.password:
        auth = (conn.login, conn.password)

    deadline = time.time() + timeout_minutes * 60
    while time.time() < deadline:
        resp = requests.get(status_url, auth=auth, timeout=30)
        if resp.status_code == 200:
            body = resp.json()
            state = body.get("state") or body.get("status") or (body.get("job", {}) and body["job"].get("status"))
            LOG.info("Polled Flink job %s state=%s", job_id, state)
            if state and state.upper() in ("FINISHED", "COMPLETED", "SUCCESS"):
                return body
            if state and state.upper() in ("FAILED", "CANCELED", "ERROR"):
                raise Exception(f"Flink job {job_id} failed: {body}")
        elif resp.status_code in (404, 410):
            LOG.info("Flink job %s not found yet (status %s). Retrying...", job_id, resp.status_code)
        else:
            LOG.warning("Unexpected response polling Flink job %s: %s %s", job_id, resp.status_code, resp.text)
        time.sleep(poll_interval)
    raise TimeoutError(f"Flink job {job_id} did not finish within {timeout_minutes} minutes")

def _monitor_job(**context):
    ti = context["ti"]
    job_id = ti.xcom_pull(key="job_id")
    if not job_id:
        raise ValueError("No job_id found in XComs to monitor")
    result = _poll_job_status(job_id, http_conn_id="flink_api", timeout_minutes=int(os.environ.get("BULK_JOB_TIMEOUT_MIN", "120")), poll_interval=int(os.environ.get("BULK_JOB_POLL_SEC", "15")))
    ti.xcom_push(key="job_result", value=result)
    return result

def _finalize(**context):
    ti = context["ti"]
    correlation_id = ti.xcom_pull(key="correlation_id")
    job_result = ti.xcom_pull(key="job_result")
    # Write simple audit row into Yugabyte/Postgres via PostgresHook (connection id: yugabyte_postgres)
    try:
        pg = PostgresHook(postgres_conn_id="yugabyte_postgres")
        sql = """
        INSERT INTO ingest_audit (correlation_id, job_id, status, details, created_at)
        VALUES (%s, %s, %s, %s, NOW())
        """
        job_id = ti.xcom_pull(key="job_id")
        status = None
        if isinstance(job_result, dict):
            # try common fields
            status = job_result.get("state") or job_result.get("status") or json.dumps(job_result)
        else:
            status = str(job_result)
        pg.run(sql, parameters=(correlation_id, job_id, status, json.dumps(job_result)))
        LOG.info("Wrote audit record for correlation_id=%s", correlation_id)
    except Exception as e:
        LOG.exception("Failed to write audit record to Yugabyte: %s", e)
        # do not fail finalize step to avoid DAG being marked failed after success; instead warn
    return True

with DAG(
    dag_id="bulk_ingest",
    default_args=DEFAULT_ARGS,
    description="Bulk ingest DAG - accepts manifest_path and correlation_id in dag_run.conf; uploads jar (optional), submits Flink job and polls for completion",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=["ingest", "bulk"],
) as dag:

    validate_manifest = PythonOperator(
        task_id="validate_manifest",
        python_callable=_validate_manifest,
        provide_context=True,
    )

    upload_jar = PythonOperator(
        task_id="upload_jar_to_flink",
        python_callable=_upload_jar_to_flink,
        provide_context=True,
    )

    trigger_bulk = PythonOperator(
        task_id="trigger_bulk_job",
        python_callable=_trigger_bulk_job,
        provide_context=True,
    )

    monitor = PythonOperator(
        task_id="monitor_job",
        python_callable=_monitor_job,
        provide_context=True,
    )

    finalize = PythonOperator(
        task_id="finalize",
        python_callable=_finalize,
        provide_context=True,
    )

    # DAG structure: validate -> (upload_jar optional) -> trigger -> monitor -> finalize
    # We keep upload_jar as a branch that always runs; it will early-return if UPLOAD_JAR!=true
    validate_manifest >> upload_jar >> trigger_bulk >> monitor >> finalize
