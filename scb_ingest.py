"""SCB JSON-stat Bronze ingestion shared by three Databricks notebook tasks.

Each call checks published source periods and existing Delta raw responses.
SparkSession is passed explicitly by the calling notebook.
HTTP requests are attempted once; configure retries on Databricks tasks.
"""

import json
from datetime import datetime, timezone
from functools import reduce
from operator import mul
from zoneinfo import ZoneInfo

import requests
from pyspark.sql.types import (
    IntegerType, StringType, StructField, StructType, TimestampType
)

SCB_BASE = "https://statistikdatabasen.scb.se/api/v2/tables"

def http_session():
    """Create a session without HTTP-level retries; Jobs owns task retries."""
    return requests.Session()


def response_json(session, url, params=None, *, context="API request"):
    """Request once and raise a contextual failure for Databricks/ADF."""
    try:
        response = session.get(url, params=params, timeout=(10, 90))
        response.raise_for_status()
    except requests.RequestException as exc:
        message = (
            f"{context}: API request failed; "
            f"{type(exc).__name__}: {exc}; url={url}; "
            "HTTP-level retries are disabled; retries are managed by Databricks task settings."
        )
        print(f"[API FAILED] {message}", flush=True)
        raise RuntimeError(message) from exc
    try:
        return response.json()
    except ValueError as exc:
        message = (
            f"{context}: HTTP {response.status_code}, but response is not valid JSON; "
            f"{exc}; url={url}"
        )
        print(f"[API FAILED] {message}", flush=True)
        raise RuntimeError(message) from exc


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
        reference = response_json(session, f"{SCB_BASE}/TAB638/metadata", {"lang": "sv"},
                                  context=f"SCB reference metadata TAB638; target={table_id}")
        kommun = sorted(code for code in dimension_codes(reference, "Region")
                        if len(code) == 4 and code.isdigit())
        if len(kommun) != 290:
            raise ValueError("TAB638 metadata must contain 290 municipality codes")
        metadata = response_json(session, f"{SCB_BASE}/{table_id}/metadata", {"lang": "sv"},
                                 context=f"SCB table={table_id}; metadata; target={target_table}")
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
            context = (f"SCB table={table_id}; period={period}; batch={batch_no}; "
                       f"target={target_table}")
            payload=response_json(session,f"{SCB_BASE}/{table_id}/data",params,
                                  context=context)
            try:
                validate_jsonstat(payload,batch,[period],required)
            except (AssertionError, KeyError, TypeError, ValueError) as exc:
                raise RuntimeError(
                    f"{context}: JSON-stat validation failed; "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            row=[("SCB",table_id,batch_no,datetime.now(timezone.utc),
                  json.dumps(payload,ensure_ascii=False,separators=(",",":")))]
            (spark.createDataFrame(row,SCB_SCHEMA).write.format("delta")
                  .mode("append").saveAsTable(target_table))
            existing_tables.add(f"scb_{table_id.lower()}")
            print(target_table, period, "batch", batch_no, "saved")
        if not expected <= keys():
            raise ValueError(f"{target_table}: incomplete after ingestion")
        print(target_table, "complete;", len(missing), "data API requests")
