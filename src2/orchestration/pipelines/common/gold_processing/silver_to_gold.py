
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
    wut_ = ut.custom_join(worker_user, talent_df, "user", "talent", worker_user["connectedTalents_id"] == talent_df["id"], "left", keep_history)
    wut_ = wut_.drop("connectedTalents_id")
    # Need to rename id to talent_id
    wut_ = wut_.withColumnRenamed("id", "talent_id")

    # A user may have multiple talents, so this may duplicate the worker, and obviusly the user id


    wutp_m_cols = ut.get_matching_cols(wut_, profile_df) # status	++	DaysSince	++	permissionScope	++	federationId	++	qualificationIds	++	customFields
    # No need to rename, we keep both
    wutp = ut.custom_join(wut_, profile_df, "talent", "profile", wut_["talent_id"] == profile_df["talentId"], "left", keep_history)
    wutp = wutp.drop("talentId")
    wutp = wutp.withColumnRenamed("id", "profile_id")

    # After this joins, a talent may (though shouldnt) have multiple profiles, and we know we have the same worker with multiple talents so we filter by profile completionRate

    wutp = ut.max_filter(wutp, ["worker_id", c.SNAPSHOT_COL], "completionRate")

    
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
    read_table_profile_positions = ut.table_name_builder("profile", "positions")
    target_table = ut.table_name_builder("collab", "referential")
    target_table_history = ut.table_name_builder("history", "profile")

    read_path_worker = f"{catalog}.{read_schema_perso}.{read_table_worker}"
    read_path_cs = f"{catalog}.{read_schema_perso}.{read_table_cs}"
    read_path_leave = f"{catalog}.{read_schema_perso}.{read_table_leave}"
    read_path_user = f"{catalog}.{read_schema_whoz}.{read_table_user}"
    read_path_talent = f"{catalog}.{read_schema_whoz}.{read_table_talent}"
    read_path_profile = f"{catalog}.{read_schema_whoz}.{read_table_profile}"
    read_path_profile_positions = f"{catalog}.{read_schema_whoz}.{read_table_profile_positions}"
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
        profile_positions = sparkSession.read.table(read_path_profile_positions)
        worker_profile = ut.max_filter(worker_profile, ["worker_id"], c.SNAPSHOT_COL).drop(c.SNAPSHOT_COL)
        profile_positions = ut.max_filter(profile_positions, ["profile_id", "position_id"], c.SNAPSHOT_COL).drop(c.SNAPSHOT_COL)

        worker_profile = worker_profile.withColumnRenamed("profile_id", "id")
        ndf = ut.add_last_mission_col_to_profile_df(worker_profile, profile_positions)
        ndf = ndf.withColumnRenamed("id", "profile_id")
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



def dimension_organization_pipe_maker(sparkSession, catalog):

    platform_ana = ut.get_platform(sparkSession, "analytic")
    gold_quality = ut.get_quality(sparkSession, "gold")
    silver_quality = ut.get_quality(sparkSession, "silver")

    read_schema_analytic = ut.schema_name_builder(silver_quality, platform_ana)
    target_schema = ut.schema_name_builder(gold_quality, platform_ana)

    read_table_dep = ut.table_name_builder("cleaned", "department")
    read_table_sl = ut.table_name_builder("cleaned", "service_line")
    target_table = ut.table_name_builder("dim", "organization")

    read_table_path_dep = f"{catalog}.{read_schema_analytic}.{read_table_dep}"
    read_table_path_sl = f"{catalog}.{read_schema_analytic}.{read_table_sl}"
    target_table_path = f"{catalog}.{target_schema}.{target_table}"

    @dp.table(
        name=target_table_path,
        comment="Dimension Organization",
        table_properties={
            "quality": "gold"
        }
    )
    def f():
        dep_df = spark.read.table(read_table_path_dep)
        sl_df = spark.read.table(read_table_path_sl)
        dep_df = ut.max_filter(dep_df, ["id"], c.SNAPSHOT_COL).drop(c.SNAPSHOT_COL)
        sl_df = ut.max_filter(sl_df, ["id"], c.SNAPSHOT_COL).drop(c.SNAPSHOT_COL)
        ndf = ut.make_department_service_line_zones(sparkSession, dep_df, sl_df)
        ndf = ndf.drop("associated_service_line", "associated_practice", "service_line_associated_zone", "id", "practiceName", "dz_department_name")

        cols_renames = {
            "department_associated_zone": "zone_id",
            "name": "zone_name",
            "BUCU": "department_bucu",
            "d_sl_z_3_department_name": "department_name",
            "zone_name": "og_zone_name"
        }

        for col, rename in cols_renames.items():
            ndf = ndf.withColumnRenamed(col, rename)

        ndf = ndf.withColumn(
            "zone_name",
            F.coalesce(F.col("og_zone_name"), F.col("practice_zone_name"))
        )
        ndf = ut.dim_organization_extra_columns(sparkSession, ndf)
        ndf = ut.drop_variant_cols(ndf)
        return ndf

    return None


def dimension_site_pipe_maker(sparkSession, catalog):

    platform_ana = ut.get_platform(sparkSession, "analytic")
    gold_quality = ut.get_quality(sparkSession, "gold")
    silver_quality = ut.get_quality(sparkSession, "silver")

    read_schema_analytic = ut.schema_name_builder(silver_quality, platform_ana)
    target_schema = ut.schema_name_builder(gold_quality, platform_ana)

    read_table_site = ut.table_name_builder("cleaned", "site")
    target_table = ut.table_name_builder("dim", "site")

    read_table_path_site = f"{catalog}.{read_schema_analytic}.{read_table_site}"
    target_table_path = f"{catalog}.{target_schema}.{target_table}"

    @dp.table(
        name=target_table_path,
        comment="Dimension Site",
        table_properties={
            "quality": gold_quality
        }
    )
    def f():
        site_df = spark.read.table(read_table_path_site)
        site_df = ut.max_filter(site_df, ["id"], c.SNAPSHOT_COL).drop(c.SNAPSHOT_COL)
        ndf = ut.map_site(sparkSession, site_df)
        ndf = ut.drop_variant_cols(ndf)
        return ndf

    return None


def common_steps(df, pk_cols, cols_to_select=None):
    ndf = ut.drop_variant_cols(df)
    ndf = ut.max_filter(ndf, pk_cols, c.SNAPSHOT_COL).drop(c.SNAPSHOT_COL)

    if cols_to_select is not None:
        ndf = ndf.select(*cols_to_select)
    return ndf

def fact_tables_pipe_maker(sparkSession, catalog):

    platform_whoz = ut.get_platform(sparkSession, "whoz")
    platform_perso = ut.get_platform(sparkSession, "perso")
    gold_quality = ut.get_quality(sparkSession, "gold")
    silver_quality = ut.get_quality(sparkSession, "silver")

    read_schema_whoz = ut.schema_name_builder(silver_quality, platform_whoz)
    read_schema_perso = ut.schema_name_builder(gold_quality, platform_perso)
    target_schema = ut.schema_name_builder(gold_quality, platform_whoz)

    read_table_cert = ut.table_name_builder("cleaned", "certification")
    read_table_acr = ut.table_name_builder("cleaned", "accreditation")
    read_table_worker = ut.table_name_builder("fact", "workers")
    read_table_edu = ut.table_name_builder("profile", "educations")
    read_table_pos = ut.table_name_builder("profile", "positions")
    read_table_apt = ut.table_name_builder("profile", "aptitudes")
    read_table_pro = ut.table_name_builder("cleaned", "profile")
    read_table_tal = ut.table_name_builder("cleaned", "talent")
    read_table_use = ut.table_name_builder("cleaned", "user")
    read_table_skill = ut.table_name_builder("cleaned", "skill")

    target_table_cert = ut.table_name_builder("fact", "certification")
    target_table_acr = ut.table_name_builder("fact", "accreditation")
    target_table_edu = ut.table_name_builder("fact", "educations")
    target_table_pos = ut.table_name_builder("fact", "positions")
    target_table_apt = ut.table_name_builder("fact", "aptitudes")
    target_table_pro = ut.table_name_builder("fact", "profile")
    target_table_tal = ut.table_name_builder("", "talent")
    target_table_use = ut.table_name_builder("", "user")
    target_table_skill = ut.table_name_builder("dim", "skill")

    read_path_worker = f"{catalog}.{read_schema_perso}.{read_table_worker}"
    read_path_cert = f"{catalog}.{read_schema_whoz}.{read_table_cert}"
    read_path_acr = f"{catalog}.{read_schema_whoz}.{read_table_acr}"
    read_path_edu = f"{catalog}.{read_schema_whoz}.{read_table_edu}"
    read_path_pos = f"{catalog}.{read_schema_whoz}.{read_table_pos}"
    read_path_apt = f"{catalog}.{read_schema_whoz}.{read_table_apt}"
    read_path_pro = f"{catalog}.{read_schema_whoz}.{read_table_pro}"
    read_path_tal = f"{catalog}.{read_schema_whoz}.{read_table_tal}"
    read_path_use = f"{catalog}.{read_schema_whoz}.{read_table_use}"
    read_path_skill = f"{catalog}.{read_schema_whoz}.{read_table_skill}"

    target_path_cert = f"{catalog}.{target_schema}.{target_table_cert}"
    target_path_acr = f"{catalog}.{target_schema}.{target_table_acr}"
    target_path_edu = f"{catalog}.{target_schema}.{target_table_edu}"
    target_path_pos = f"{catalog}.{target_schema}.{target_table_pos}"
    target_path_apt = f"{catalog}.{target_schema}.{target_table_apt}"
    target_path_pro = f"{catalog}.{target_schema}.{target_table_pro}"
    target_path_tal = f"{catalog}.{target_schema}.{target_table_tal}"
    target_path_use = f"{catalog}.{target_schema}.{target_table_use}"
    target_path_skill = f"{catalog}.{target_schema}.{target_table_skill}"

    @dp.table(
        name=target_path_skill,
        comment="Fact Skills",
        table_properties={
            "quality": "gold"
        }
    )
    def f_skill():
        ndf = sparkSession.read.table(read_path_skill)
        cols_to_select = [
            F.col("id"),
            F.col("type"),
            F.col("granularity"),
            F.col("nature"),
            F.col("name_text").alias("name"),
            F.col("description_text").alias("description")
        ]
        ndf = common_steps(ndf, ["id"], cols_to_select)
        return ndf

    @dp.table(
        name=target_path_cert,
        comment="Dim Skills",
        table_properties={
            "quality": "gold"
        }
    )
    def f_certs():
        ndf = sparkSession.read.table(read_path_cert)
        cols_to_select = [
            "id", "profileId",
            "title", "description",
            "endDate", "is_active"
        ]
        ndf = common_steps(ndf, ["id", "profileId"], cols_to_select)
        return ndf

    @dp.table(
        name=target_path_apt,
        comment="Fact Aptitudes",
        table_properties={
            "quality": "gold"
        }
    )
    def f_apt():
        apt = sparkSession.read.table(read_path_apt)
        apt = common_steps(apt, ["profile_id", "aptitude_id"])
        skill = sparkSession.read.table(target_path_skill) # We join with skill to add the skill data and maintain eveything one Join away (consecutive joins when you have a Many to Many relationship give problems)

        ndf = ut.custom_join(apt, skill, "aptitude", "skill", apt["concept_id"] == skill["id"], "left")
        ndf = ndf.withColumnRenamed("description", "skill_description")
        return ndf



    return None

