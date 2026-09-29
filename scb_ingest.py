"""SCB JSON-stat Bronze ingestion shared by three Databricks notebook tasks.

Each call checks published source periods and existing Delta raw responses.
SparkSession is passed explicitly by the calling notebook.
"""

import json
from datetime import datetime, timezone
from functools import reduce
from operator import mul
from zoneinfo import ZoneInfo

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType
)

SCB_BASE = "https://statistikdatabasen.scb.se/api/v2/tables"

def http_session():
    """Create an HTTP session that retries transient API failures."""
    session = requests.Session()
    retry = Retry(
        total=5, connect=5, read=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session

def response_json(session, url, params=None):
    """Request a JSON document and raise an error for unsuccessful responses."""
    response = session.get(url, params=params, timeout=(10, 90))
    response.raise_for_status()
    return response.json()

def dimension_codes(payload, dimension_id):
    """Return JSON-stat category codes in their declared value-array order."""
    index = payload["dimension"][dimension_id]["category"]["index"]
    return [code for code, _ in sorted(index.items(), key=lambda item: item[1])] if isinstance(index, dict) else index


def validate_jsonstat(payload, expected_regions, expected_times, expected_codes):
    """Verify SCB dimensions and value count, returning the number of values."""
    ids = payload["id"]
    assert set(expected_codes).issubset(ids) and "Region" in ids and "Tid" in ids
    assert set(dimension_codes(payload, "Region")) == set(expected_regions)
    assert set(dimension_codes(payload, "Tid")) == set(expected_times)
    for dimension_id, codes in expected_codes.items():
        assert set(dimension_codes(payload, dimension_id)) == set(codes), dimension_id
    expected_count = reduce(mul, payload["size"], 1)
    assert len(payload["value"]) == expected_count
    assert all(value is not None for value in payload["value"]), "SCB returned missing values"
    return expected_count


SCB_SCHEMA = StructType([
    StructField("source", StringType(), False),
    StructField("table_id", StringType(), False),
    StructField("batch_no", IntegerType(), False),
    StructField("fetched_at_utc", TimestampType(), False),
    StructField("payload_json", StringType(), False),
])



def ingest_scb(spark, table_id, content_code, filters, start_year, cadence,
               end_year=None, catalog="db_labb2"):
    # Existing multi-period 2023-25 batches remain valid; new periods are separate responses.
    target_table = f"{catalog}.bronze.scb_{table_id.lower()}"
    required = {"ContentsCode": [content_code], **filters}
    today = datetime.now(ZoneInfo("Europe/Stockholm")).date()
    with http_session() as session:
        reference = response_json(session, f"{SCB_BASE}/TAB638/metadata", {"lang": "sv"})
        kommun = sorted(code for code in dimension_codes(reference, "Region")
                        if len(code) == 4 and code.isdigit())
        if len(kommun) != 290:
            raise ValueError("TAB638 metadata must contain 290 municipality codes")
        metadata = response_json(session, f"{SCB_BASE}/{table_id}/metadata", {"lang": "sv"})
        for dim, codes in required.items():
            if not set(codes) <= set(dimension_codes(metadata, dim)):
                raise ValueError(f"{target_table}: missing {dim} codes")
        if not set(kommun) <= set(dimension_codes(metadata, "Region")):
            raise ValueError(f"{target_table}: municipality mismatch")
        import re
        available_periods = dimension_codes(metadata, "Tid")
        if cadence == "year":
            periods = sorted(p for p in available_periods
                             if re.fullmatch(r"[0-9]{4}", p)
                             and start_year <= int(p) <= min(end_year or today.year, today.year))
        else:
            periods = sorted(p for p in available_periods
                             if re.fullmatch(r"[0-9]{4}M(0[1-9]|1[0-2])", p)
                             and start_year <= int(p[:4])
                             and (int(p[:4]), int(p[-2:])) <= (today.year, today.month))
        if not periods:
            print(target_table, "no published target periods")
            return
        existing_tables = {r.tableName for r in spark.sql(f"SHOW TABLES IN {catalog}.bronze").collect()}
        def keys():
            if f"scb_{table_id.lower()}" not in existing_tables:
                return set()
            found=set()
            for row in spark.table(target_table).select("source","table_id","batch_no","payload_json").collect():
                batch_no=row.batch_no
                if row.source!="SCB" or row.table_id!=table_id or batch_no not in range(1,13):
                    raise ValueError(f"{target_table}: invalid saved batch {batch_no}")
                payload=json.loads(row.payload_json)
                times=dimension_codes(payload,"Tid")
                if not times or len(times)!=len(set(times)):
                    raise ValueError(f"{target_table}: invalid saved periods")
                expected_region=kommun[(batch_no-1)*25:batch_no*25]
                validate_jsonstat(payload, expected_region, times, required)
                for period in times:
                    key=(batch_no,period)
                    if key in found:
                        raise ValueError(f"{target_table}: overlapping batch/period {key}")
                    found.add(key)
            return found
        saved=keys()
        expected={(batch_no,p) for p in periods for batch_no in range(1,13)}
        missing=sorted(expected-saved,key=lambda item:(item[1],item[0]))
        print(target_table, len(periods), "published periods;", len(missing), "missing batches")
        for batch_no, period in missing:
            batch=kommun[(batch_no-1)*25:batch_no*25]
            params={"lang":"sv","valueCodes[ContentsCode]":content_code,
                    "valueCodes[Region]":",".join(batch),"valueCodes[Tid]":period,
                    **{f"valueCodes[{k}]":",".join(v) for k,v in filters.items()}}
            payload=response_json(session,f"{SCB_BASE}/{table_id}/data",params)
            validate_jsonstat(payload,batch,[period],required)
            row=[("SCB",table_id,batch_no,datetime.now(timezone.utc),
                  json.dumps(payload,ensure_ascii=False,separators=(",",":")))]
            (spark.createDataFrame(row,SCB_SCHEMA).write.format("delta")
                  .mode("append").saveAsTable(target_table))
            existing_tables.add(f"scb_{table_id.lower()}")
            print(target_table, period, "batch", batch_no, "saved")
        if not expected <= keys():
            raise ValueError(f"{target_table}: incomplete after ingestion")
        print(target_table, "complete;", len(missing), "data API requests")
