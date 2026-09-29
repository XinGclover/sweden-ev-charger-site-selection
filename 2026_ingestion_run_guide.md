# Databricks 2026 incremental ingestion

The package also contains scb_ingest.py. Place this Python file beside the three SCB
Bronze notebooks in the same Databricks Git folder before running them. Each SCB notebook
imports the module and passes its notebook Spark session explicitly. Commit the module
and notebooks together. The electricity notebook remains independent.

The package contains four Bronze notebooks (population, registrations, vehicles, electricity) plus updated Silver and Gold notebooks. Keep the existing five Bronze managed Delta tables in db_labb2. Keep kommun_price_area_2024.csv beside Gold.

## Verified on 29 September 2026

SCB metadata shows TAB3277 published through August 2026. TAB5557 ends in 2025, TAB1278 ends in 2025, and TAB638 ends in 2024. A direct data request for TAB3277 August 2026 returned a valid 25 municipality, two fuel response (50 values). The price API returned a complete 96 interval response for 28 September 2026 in SE3. The other 2026 dates and areas were not individually checked.

## First Databricks run

1. Import the six notebooks. Run Bronze population and vehicles: if old history is complete, each should make zero SCB data calls (metadata is still fetched).
2. In Bronze electricity's last cell, temporarily set PRICE_START and PRICE_END to date(2026, 9, 28). Run it twice: the first run should add four area responses; the second should add none.
3. Restore its default start (2025-01-01) and end (today, Stockholm time) for the full catch-up. If Bronze contains no 2026 prices, this is over a thousand calls; monitor compute costs. An interrupted run resumes at missing day and area keys.
4. Bronze registrations will add each published missing 2026 month in 12 municipality batches. Complete its run before Silver. It can resume an interrupted month at the missing batch.
5. Once all Bronze tasks finish, run silver_databricks_2026 and gold_databricks_2026. Silver requires every period to have all 12 SCB batches and every electricity date through the latest ingested day to have SE1–SE4. Do not run Silver immediately after only the one-day electricity trial.
6. Configure a Job with four independent Bronze tasks, Silver dependent on all four, and Gold dependent on Silver. Retire the earlier fixed-period Silver and Gold Job tasks.

The existing 2023–2025 multi-period SCB raw batches remain in place. New periods are stored separately in the same raw tables and checked by batch and period within the JSON-stat response. Electricity checks date and area. Already saved responses are not automatically refreshed if a source later revises them.

Gold retains the complete 2025 charger-ranking comparison. It adds 2026 observations to granular facts; incomplete 2026 electricity does not become a full-year annual average. A 2026 ranking needs a deliberate year-to-date comparison or a completed year.
