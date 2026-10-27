
import common.raw_processing.raw_reader as rr
import common.raw_processing.payload_to_table as pt
import common.raw_processing.snapshot_processor as sp
import common.silver_processing.basic_filtering_and_quarantine as bfq
import common.silver_processing.raw_to_silver as rts
import common.gold_processing.dimensions_and_facts as daf
import common.gold_processing.dimension_date as dd
import common.gold_processing.silver_to_gold as stg
import common.utils as ut


PLATFORM_TABLES = {
    "analytic": ["bu", "department", "entity", "service_line", "site", "society"],
    "whoz": ["accreditation", "certification", "profile", "skill", "talent", "user"],
    "perso": ["collab_status", "leave", "workers"]
}
CATALOG = ut.get_conf(spark, "catalog")
#rr.create_quarantine_table(spark, CATALOG)
#bfq.create_quarantine_table_sanitize(spark, CATALOG)
for platform, tables in PLATFORM_TABLES.items():
    for table in tables:
        rr.raw_bronze_pipe_maker(spark, CATALOG, platform, table, True)
        rts.silver_pipe_maker(spark, CATALOG, platform, table)

daf.dimension_organization_pipe_maker(spark, CATALOG)
daf.dimension_site_pipe_maker(spark, CATALOG)
dd.make_dim_date_pipe_maker(CATALOG)
stg.worker_process_pipe_maker_2(spark, CATALOG)