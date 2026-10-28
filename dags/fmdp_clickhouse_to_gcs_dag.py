"""FMDP: export local ClickHouse raw/golden snapshot to GCS (Airflow 3.3).

This DAG is intended to execute *inside local Linux Airflow Docker*, not Windows
PowerShell. Host connectivity to local ClickHouse is via host.docker.internal:8123.
ADC must be mounted read-only into the TASK-EXECUTING container.

Manifest is written LAST; VM watcher treats it as the snapshot commit marker.
"""
import csv
import hashlib
import os
import tempfile
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.sdk import Asset, dag, task
from airflow.timetables.trigger import CronTriggerTimetable

TABLES = (
    'golden.securities', 'golden.adjustment_events', 'golden.bars_intraday',
    'golden.price_daily', 'raw.analyst_grades', 'raw.balance_sheet',
    'raw.cash_flow', 'raw.enterprise_values', 'raw.executive_compensation',
    'raw.financial_estimate', 'raw.financial_ratio', 'raw.income_statement',
    'raw.market_cap', 'raw.price_volume_daily',
    'raw.price_volume_unadjusted_daily', 'raw.profile',
)

EOD_READY = Asset("x-fmdp://local/daily-eod-ready")

@dag(
    dag_id='fmdp_clickhouse_snapshot_to_gcs',
    start_date=pendulum.datetime(2026, 10, 27, tz='Asia/Shanghai'),
    schedule=[EOD_READY],  # Manual testing; enable schedule AFTER full end-to-end validation
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=10)},
    tags=['fmdp', 'clickhouse', 'gcs'],
    description='Full snapshot of 16 local ClickHouse tables, manifest uploaded last.',
)
def fmdp_clickhouse_snapshot_to_gcs():

    @task(execution_timeout=timedelta(hours=3))
    def export_and_publish():
        import requests
        from google.cloud import storage
        from google.cloud.storage.retry import DEFAULT_RETRY

        endpoint = os.environ.get('FMDP_CLICKHOUSE_URL', 'http://host.docker.internal:8123').rstrip('/')
        username = os.environ.get('FMDP_CLICKHOUSE_USER', 'default')
        password = os.environ.get('FMDP_CLICKHOUSE_PASSWORD', '')
        bucket = os.environ.get('FMDP_GCS_BUCKET', 'financial-market-data-2026-10-25')
        # A new ID for each attempt: incomplete snapshots remain ignored (no manifest).
        snapshot = pendulum.now('Asia/Shanghai').format('YYYYMMDD-HHmmss')
        prefix = f'snapshots/clickhouse/{snapshot}'
        gcs_client = storage.Client()
        bucket_ref = gcs_client.bucket(bucket)
        session = requests.Session()
        session.auth = (username, password)

        def query_scalar(sql):
            response = session.post(endpoint, data=sql.encode('utf-8'), timeout=(15, 120))
            response.raise_for_status()
            return int(response.text.strip())

        def put_file(local_file, object_name):
            digest = hashlib.sha256()
            with open(local_file, 'rb') as file:
                for block in iter(lambda: file.read(1024 * 1024), b''):
                    digest.update(block)
            blob = bucket_ref.blob(object_name)
            blob.metadata = {'sha256': digest.hexdigest(), 'snapshot': snapshot}
            # Create-only prevents accidentally mutating a completed snapshot.
            # The request timeout is distinct from total retry time.
            # A transient network disconnect previously exhausted the 120s default
            # retry budget during raw.income_statement upload.
            try:
                blob.upload_from_filename(
                    str(local_file),
                    checksum='auto',
                    if_generation_match=0,  # create-only makes retries safe
                    timeout=(30, 180),
                    retry=DEFAULT_RETRY.with_timeout(900),
                )
            except Exception:
                # The server may have committed the upload but the response was lost.
                # Trust a pre-existing object only if both digest and size agree.
                existing = bucket_ref.get_blob(object_name)
                expected_size = Path(local_file).stat().st_size
                if (existing is None or existing.size != expected_size
                        or (existing.metadata or {}).get('sha256') != digest.hexdigest()):
                    raise
                print(f'  Upload response lost, but verified existing object: {object_name}')
            blob.reload(retry=DEFAULT_RETRY.with_timeout(600))
            size = Path(local_file).stat().st_size
            if blob.size != size:
                raise RuntimeError(f'Cloud object size mismatch for {object_name}: {blob.size} != {size}')
            return digest.hexdigest()

        print(f'FMDP snapshot {snapshot} => gs://{bucket}/{prefix}/')
        print('Ensure ALL local ingestion/write jobs completed before this snapshot starts.')
        manifest = []
        # One task / one temp directory: no cross-task filesystem or XCom coupling.
        with tempfile.TemporaryDirectory(prefix='fmdp-snapshot-') as tmp:
            for i, table in enumerate(TABLES, 1):
                before = query_scalar(f'SELECT count() FROM {table}')
                local_file = Path(tmp) / f'{table}.parquet'
                print(f'[{i}/16] {table}: {before} rows')
                if before:
                    # Stream binary Parquet directly to a file; do NOT use PowerShell redirection.
                    with session.post(endpoint,
                                      data=f'SELECT * FROM {table} FORMAT Parquet'.encode('utf-8'),
                                      stream=True, timeout=(15, 1800)) as response:
                        response.raise_for_status()
                        with open(local_file, 'wb') as out:
                            for chunk in response.iter_content(chunk_size=1024 * 1024):
                                if chunk:
                                    out.write(chunk)
                    if local_file.stat().st_size == 0:
                        raise RuntimeError(f'Empty Parquet export for nonempty table {table}')
                after = query_scalar(f'SELECT count() FROM {table}')
                if after != before:
                    raise RuntimeError(f'Concurrent insert/delete in {table}: before={before}, after={after}')
                if before:
                    put_file(local_file, f'{prefix}/{table}.parquet')
                manifest.append((table, before))
                print(f'  Uploaded {table}: {before} rows')

            manifest_path = Path(tmp) / 'manifest.tsv'
            with open(manifest_path, 'w', newline='', encoding='ascii') as fp:
                writer = csv.writer(fp, delimiter='\t', lineterminator='\n')
                writer.writerow(('table', 'count'))
                writer.writerows(manifest)
            # COMMIT SIGNAL: the VM must not consider a snapshot ready before this.
            put_file(manifest_path, f'{prefix}/manifest.tsv')
            print(f'SUCCESS snapshot ready: {snapshot} (16 tables)')
        return snapshot

    export_and_publish()


fmdp_clickhouse_snapshot_to_gcs()
