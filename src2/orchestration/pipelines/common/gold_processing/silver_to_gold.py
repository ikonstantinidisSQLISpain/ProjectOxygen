
import pyspark.pipelines as dp
import pyspark.sql.functions as F
import pyspark.sql.types as ty
import common.utils as ut
import common.constants as c
import common.raw_processing.payload_to_table as pt
import common.raw_processing.snapshot_processor as sp
import common.silver_processing.basic_filtering_and_quarantine as bfq
import common.gold_processing.dimensions_and_facts as daf
from pathlib import Path



def worker_profile_process(worker_df, collab_df, leave_df, user_df, talent_df, profile_df, keep_history = False):

    if not keep_history:
        worker_df = ut.max_filter(worker_df, ["id"], c.SNAPSHOT_COL)
        collab_df = ut.max_filter(collab_df, ["uid"], c.SNAPSHOT_COL)
        leave_df = ut.max_filter(leave_df, ["uid"], c.SNAPSHOT_COL)
        user_df = ut.max_filter(user_df, ["id"], c.SNAPSHOT_COL)
        talent_df = ut.max_filter(talent_df, ["id"], c.SNAPSHOT_COL)
        profile_df = ut.max_filter(profile_df, ["id"], c.SNAPSHOT_COL)

    wc_m_cols = ut.get_matching_cols(worker_df, collab_df) # department_id	++	service_line_id	++	site_id	++	matricule
    worker = ut.custom_join(worker_df, collab_df, "worker", "collab", worker_df["id"] == collab_df["uid"], "inner", keep_history)
    worker = worker.drop("uid", *[f"collab_{col}" for col in wc_m_cols])
    renames = dict()
    for col in wc_m_cols:
        renames[f"worker_{col}"] = col
    worker = worker.withColumnsRenamed(renames)


    wl_m_cols = ut.get_matching_cols(worker, leave_df) # display_name	++	matricule
    worker = ut.custom_join(worker, leave_df, "worker", "leave", worker["id"] == leave_df["uid"], "left", keep_history)
    worker = worker.drop("uid", *[f"leave_{col}" for col in wl_m_cols])
    renames = dict()
    for col in wl_m_cols:
        renames[f"worker_{col}"] = col
    worker = worker.withColumnsRenamed(renames)


    wu_m_cols = ut.get_matching_cols(worker, user_df) # id
    worker_user = ut.custom_join(worker, user_df, "worker", "user", worker["mail"] == user_df["username"], "left", keep_history) # left, to keep the workers without whoz profile
    # Here, workers id is renamed to worker_id, and user id, to user_id, no need to do any rename.
    worker_user = worker_user.drop("username")


    wut_m_cols = ut.get_matching_cols(worker_user, talent_df) # removed	++	createdDate	++	lastConnectionDate	++	language	++	createdBy	++	lastModifiedBy	++	gender	++	lastModifiedDate
    # We dont want to remove any of this columns and we rather keep what do they reference to
    wut = ut.custom_join(worker_user, talent_df, "user", "talent", worker_user["user_id"] == talent_df["userId"], "left", keep_history)
    wut = wut.drop("userId")
    # Need to rename id to talent_id
    wut = wut.withColumnRenamed("id", "talent_id")


    wutp_m_cols = ut.get_matching_cols(wut, profile_df) # status	++	DaysSince	++	permissionScope	++	federationId	++	qualificationIds	++	customFields
    # No need to rename, we keep both
    wutp = ut.custom_join(wut, profile_df, "talent", "profile", wut["talent_id"] == profile_df["talentId"], "left", keep_history)
    wutp = wutp.drop("talentId")
    wutp = wutp.withColumnRenamed("id", "profile_id")

    
    ndf = ut.drop_variant_cols(wutp) # Required for the dashboards
    return ndf


def worker_process_pipe_maker_2(sparkSession, catalog, enable_quarantine=False):

    platform_whoz = ut.get_platform(sparkSession, "whoz")
    platform_perso = ut.get_platform(sparkSession, "perso")
    target_quality = ut.get_quality(sparkSession, "gold")
    read_quality = ut.get_quality(sparkSession, "silver")

    read_schema_whoz = ut.schema_name_builder(read_quality, platform_whoz)
    read_schema_perso = ut.schema_name_builder(read_quality, platform_perso)
    target_schema = ut.schema_name_builder(target_quality, platform_perso)

    read_table_worker = ut.table_name_builder("cleaned", "workers")
    read_table_cs = ut.table_name_builder("cleaned", "collab_status")
    read_table_leave = ut.table_name_builder("cleaned", "leave")
    read_table_user = ut.table_name_builder("cleaned", "user")
    read_table_talent = ut.table_name_builder("cleaned", "talent")
    read_table_profile = ut.table_name_builder("cleaned", "profile")
    target_table = ut.table_name_builder("collab", "referential")
    target_table_history = ut.table_name_builder("history", "profile")

    read_path_worker = f"{catalog}.{read_schema_perso}.{read_table_worker}"
    read_path_cs = f"{catalog}.{read_schema_perso}.{read_table_cs}"
    read_path_leave = f"{catalog}.{read_schema_perso}.{read_table_leave}"
    read_path_user = f"{catalog}.{read_schema_whoz}.{read_table_user}"
    read_path_talent = f"{catalog}.{read_schema_whoz}.{read_table_talent}"
    read_path_profile = f"{catalog}.{read_schema_whoz}.{read_table_profile}"
    target_path = f"{catalog}.{target_schema}.{target_table}"
    target_path_history = f"{catalog}.{target_schema}.{target_table_history}"

    @dp.table(
        name=target_path_history,
        comment="Workers data. Fact table. Historical",
        table_properties={
            "quality": "gold"
        }
    )
    def f():
        worker = sparkSession.read.table(read_path_worker)
        cs = sparkSession.read.table(read_path_cs)
        lea = sparkSession.read.table(read_path_leave)
        use = sparkSession.read.table(read_path_user)
        pro = sparkSession.read.table(read_path_profile)
        tal = sparkSession.read.table(read_path_talent)

        ndf = worker_profile_process(worker, cs, lea, use, tal, pro, True)
        return ndf


    @dp.table(
        name=target_path,
        comment="Workers data. Fact table.",
        table_properties={
            "quality": "gold"
        }
    )
    def f2():
        worker_profile = sparkSession.read.table(target_path_history)
        ndf = ut.max_filter(worker_profile, ["worker_id"], c.SNAPSHOT_COL)
        return ndf

    if enable_quarantine:
        quarantine_schema = ut.get_quarantine_schema(spark)
        quarantine_table = ut.get_quarantine_table(spark, "sanitize")
        quarantine_path = f"{catalog}.{quarantine_schema}.{quarantine_table}"
        quarantine_queries = [
            "worker_service_line_id != collab_service_line_id",
            "worker_department_id != collab_service_line_id"
        ]
        pk_cols = ["id"]
        @dp.append_flow(
            target=quarantine_path,
            name=f"flow_{target_path.replace('.','_')}_{quarantine_path.replace('.','_')}"
        )
        def f3():
            df = spark.read.table(target_path)
            return ut.make_quarantine_table(df, quarantine_queries, pk_cols, target_path)

    return None







