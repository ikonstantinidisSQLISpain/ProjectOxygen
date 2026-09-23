import pyspark.pipelines as dp
import pyspark.sql.functions as F
from bronze_constants import CATALOG, TARGET_SCHEMA, KNOWN_SCHEMAS_HISTORY, EXLCUDE_HISTORY_COLUMNS
CATALOG, TARGET_SCHEMA = CATALOG(spark), TARGET_SCHEMA(spark)


def pipe_maker(table):

    dp.create_streaming_table(
        name=f"{CATALOG}.{TARGET_SCHEMA}.{table}_history",
        schema=KNOWN_SCHEMAS_HISTORY[table],
        table_properties={"quality": "bronze"},
    )
    dp.create_auto_cdc_from_snapshot_flow(
        target=f"{CATALOG}.{TARGET_SCHEMA}.history_{table}",
        source=f"{CATALOG}.{TARGET_SCHEMA}.base_{table}",
        keys=["id"],
        # Whoz's own last-modified time on the record, not when we happened to ingest it —
        # protects against an older dated export landing after a newer one (e.g. backfill).
        #sequence_by=F.col("snapshot_ts"),
        stored_as_scd_type="2",
        except_column_list=EXLCUDE_HISTORY_COLUMNS[table]
    )

    return None

"""
for table in KNOWN_SCHEMAS_HISTORY.keys():
    pipe_maker(table)
"""


