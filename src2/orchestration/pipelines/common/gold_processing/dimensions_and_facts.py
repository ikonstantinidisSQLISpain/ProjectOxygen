

import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.pipelines as dp
import common.utils as ut




def simple_read_pipe_maker(sparkSession, catalog, platform, table_name, target_type):

    platform_name = ut.get_platform(sparkSession, platform)
    target_quality = ut.get_quality(sparkSession, "gold")
    read_quality = ut.get_quality(sparkSession, "silver")

    read_schema = ut.schema_name_builder(read_quality, platform_name)
    target_schema = ut.schema_name_builder(target_quality, platform_name)

    read_table = ut.table_name_builder("cleaned", table_name)
    target_table = ut.table_name_builder(target_type, table_name)
    read_path = f"{catalog}.{read_schema}.{read_table}"
    target_path = f"{catalog}.{target_schema}.{target_table}"

    @dp.table(
        name=target_path,
        comment=f"{target_type} {table_name}.",
        table_properties={
            "quality": "gold"
        }
    )
    def f():
        return sparkSession.readStream.table(read_path)

    return None

"""
def worker_process(worker_df, collab_status_df, user_df, talent_df, profile_df):

    "We need to add the collab_status status data"
    ndf = ut.custom_join(worker_df, collab_status_df, "worker", "collab", worker_df["id"] == collab_status_df["uid"], "left")
    ndf = ndf.drop(F.col("uid"))

    user_tal = ut.custom_join(
        user_df, talent_df,
        "user", "talent",
        user_df["id"] == talent_df["userId"],
        "left" # This removes all the talents without a user.
    )

    user_tal_pro = ut.custom_join(
        user_tal, profile_df,
        "user_talent", "profile",
        user_tal["talent_id"] == profile_df["talentId"],
        "left" # This removes profiles without talent (though should not happen)
    )

    worker_pro = ut.custom_join(
        ndf, user_tal_pro,
        "worker_2", "profile",
        ndf["mail"] == user_tal_pro["username"],
        "left"  # This keeps worker that are not in Whoz but removes user created by other people
    )

    # Dado que 
    
    return worker_pro

def worker_process_pipe_maker(sparkSession, catalog, enable_quarantine: bool = False):

    platform_whoz = ut.get_platform(sparkSession, "whoz")
    platform_perso = ut.get_platform(sparkSession, "perso")
    target_quality = ut.get_quality(sparkSession, "gold")
    read_quality = ut.get_quality(sparkSession, "silver")

    read_schema_whoz = ut.schema_name_builder(read_quality, platform_whoz)
    read_schema_perso = ut.schema_name_builder(read_quality, platform_perso)
    target_schema = ut.schema_name_builder(target_quality, platform_perso)

    read_table_worker = ut.table_name_builder("cleaned", "workers")
    read_table_cs = ut.table_name_builder("cleaned", "collab_status")
    read_table_user = ut.table_name_builder("cleaned", "user")
    read_table_talent = ut.table_name_builder("cleaned", "talent")
    read_table_profile = ut.table_name_builder("cleaned", "profile")
    target_table = ut.table_name_builder("fact", "workers")

    read_path_worker = f"{catalog}.{read_schema_perso}.{read_table_worker}"
    read_path_cs = f"{catalog}.{read_schema_perso}.{read_table_cs}"
    read_path_user = f"{catalog}.{read_schema_whoz}.{read_table_user}"
    read_path_talent = f"{catalog}.{read_schema_whoz}.{read_table_talent}"
    read_path_profile = f"{catalog}.{read_schema_whoz}.{read_table_profile}"
    target_path = f"{catalog}.{target_schema}.{target_table}"

    @dp.table(
        name=target_path,
        comment="Workers data.",
        table_properties={
            "quality": "gold"
        }
    )
    def f():
        worker = sparkSession.read.table(read_path_worker)
        cs = sparkSession.read.table(read_path_cs)
        u = sparkSession.read.table(read_path_user)
        t = sparkSession.read.table(read_path_talent)
        p = sparkSession.read.table(read_path_profile)
        return worker_process(worker, cs, u, t, p)

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
        def f2():
            df = spark.readStream.table(target_path)
            return ut.make_quarantine_table(df, quarantine_queries, pk_cols, target_path)

    return None
"""


def worker_process_2(worker, collab_status, leave):

    ndf = ut.custom_join(worker, collab_status, "worker", "collab", worker["id"] == collab_status["uid"], "inner")
    ndf = ut.custom_join(
        ndf, leave,
        "worker_2", "leave",
        ndf["id"] == leave["uid"],
        "left"
    )
    ndf = ndf.where("status_name != 'Compte Technique'")

    cols_to_drop = [
        "skill",
        "position",
        "employee_category",
        "year", "month",
        "worker_2_uid",
        "collab_matricule",
        "collab_site_id",
        "collab_service_line_id",
        "collab_department_id",
        "leave_display_name"
    ]
    ndf = ndf.drop(*cols_to_drop)

    cols_to_rename = {
        "worker_2_display_name": "display_name",
        "worker_site_id": "site_id",
        "worker_service_line_id": "service_line_id",
        "worker_department_id": "department_id"
    }

    for og, new_c in cols_to_rename.items():
        ndf = ndf.withColumnRenamed(og, new_c)

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
    target_table = ut.table_name_builder("collab", "referential")

    read_path_worker = f"{catalog}.{read_schema_perso}.{read_table_worker}"
    read_path_cs = f"{catalog}.{read_schema_perso}.{read_table_cs}"
    read_path_leave = f"{catalog}.{read_schema_perso}.{read_table_leave}"
    target_path = f"{catalog}.{target_schema}.{target_table}"

    @dp.table(
        name=target_path,
        comment="Workers data. Fact table.",
        table_properties={
            "quality": "gold"
        }
    )
    def f():
        worker = sparkSession.read.table(read_path_worker)
        cs = sparkSession.read.table(read_path_cs)
        l = sparkSession.read.table(read_path_leave)
        return worker_process_2(worker, cs, l)

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
        def f2():
            df = spark.readStream.table(target_path)
            return ut.make_quarantine_table(df, quarantine_queries, pk_cols, target_path)

    return None

def fact_cert_acr_process(gold_worker_table, cert_or_acr_table, cert_or_acc_alias):

    worker = gold_worker_table.select(F.col("profile_id"))

    joined = ut.custom_join(worker, cert_or_acr_table, "worker", cert_or_acc_alias, worker["profile_id"] == cert_or_acr_table["profileId"], "left")
    # Left filters those profiles without a worker
    return joined

def certifications_accreditation_and_workers_pipe(sparkSession, catalog):

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
        name=target_path_cert,
        comment="Fact Certifications",
        table_properties={
            "quality": "gold"
        }
    )
    def f2():
        
        cert = spark.read.table(read_path_cert)
        variant_cols = ["qualificationIds", "attachment"]
        cert = cert.drop(*variant_cols)
        return cert

    @dp.table(
        name=target_path_acr,
        comment="Fact Accreditation",
        table_properties={
            "quality": gold_quality
        }
    )
    def f3():
        acr = spark.read.table(read_path_acr)
        variant_cols = ["qualificationIds", "attachment"]
        acr = acr.drop(*variant_cols)
        
        return acr

    @dp.table(
        name=target_path_edu,
        comment="Fact Educations",
        table_properties={
            "quality": gold_quality
        }
    )
    def f4():
        edu = spark.read.table(read_path_edu)
        return edu


    @dp.table(
        name=target_path_pos,
        comment="Fact Positions",
        table_properties={
            "quality": gold_quality
        }
    )
    #@dp.expect_or_drop("missing_position", "")
    def f5():
        pos = spark.read.table(read_path_pos)
        return pos

    @dp.table(
        name=target_path_pro,
        comment="Fact Profile",
        table_properties={
            "quality": gold_quality
        }
    )
    @dp.expect_or_drop("talent_not_missing", "talentId IS NOT NULL")
    def f6():
        pro = spark.read.table(read_path_pro)
        pro_pos = spark.read.table(read_path_pos)
        cols_to_drop = [
            "completionDetails",
            "customFields",
            "functionalDomains",
            "headline",
            "links",
            "qualificationIds",
            "schedules",
            "skillRatings",
            "targetFunctionalDomains",
            "targetSkillRatings",
            "resume"
        ]
        pro = pro.drop(*cols_to_drop)
        pro = ut.add_last_mission_col_to_profile_df(pro, pro_pos)
        return pro

    
    @dp.table(
        name=target_path_apt,
        comment="Fact Aptitudes",
        table_properties={
            "quality": gold_quality
        }
    )
    @dp.expect_all_or_drop({
        "missing_aptitude_id": "aptitude_id IS NOT NULL",
        "missing_concept_id": "concept_id IS NOT NULL"
    })
    def f7():
        apt = spark.read.table(read_path_apt).withColumn(
            "proficiency",
            F.when(
                F.col("aptitude_id").isNotNull() | F.col("concept_id").isNotNull(),
                F.coalesce(F.col("proficiency").cast("int"), F.lit(0))
            ).otherwise(F.col("proficiency"))
        ).withColumn(
            "concept_aptitude_id",
            F.coalesce(F.col("concept_id"), F.col("aptitude_id"))
        )
        return apt

    @dp.table(
        name=target_path_tal,
        comment="Fact Talent",
        table_properties={
            "quality": gold_quality
        }
    )
    @dp.expect_or_drop("user_not_missing", "userId IS NOT NULL")
    def f8():
        tal = spark.read.table(read_path_tal)
        cols_to_drop = [
            "nationalities",
            "emails",
            "address",
            "tags",
            "aspirations",
            "sharingDestinations",
            "history",
            "maxWorkingHours",
            "recruitment",
            "qualificationIds",
            "customFields",
            "photo"
        ]
        tal = tal.drop(*cols_to_drop)
        return tal


    @dp.table(
        name=target_path_use,
        comment="Fact User",
        table_properties={
            "quality": gold_quality
        }
    )
    @dp.expect_or_drop("username_not_missing", "username IS NOT NULL")
    def f9():
        users = spark.read.table(read_path_use)
        cols_to_drop = [
            "workspaceRoles",
            "federationRoles",
            "agenticStudioRoles",
            "connectedTalents",
            "keyboardShortcuts"
        ]
        users = users.drop(*cols_to_drop)
        return users

    
    @dp.table(
        name=target_path_skill,
        comment="Dimension Skill",
        table_properties={
            "quality": gold_quality
        }
    )
    def f10():
        skills = spark.read.table(read_path_skill)
        cols_to_drop = [
            "alsoPartOf",
            "coreSkills",
            "name",
            "description",
            "terms",
            "hiddenTerms",
            "wikipediaLink",
            "depiction"
        ]
        skills = skills.drop(*cols_to_drop)
        return skills

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
        ndf = ut.map_site(sparkSession, site_df)
        return ndf

    return None


def make_gold_history_profile_pipe(sparkSession, catalog):

    @dp.table(
        name=f"{catalog}.gold_whoz.history_profile",
        comment="Gold Time series of profile completion and score",
        table_properties={
            "quality": "gold"
        }
    )
    def f():
        return sparkSession.read.table(f"{catalog}.silver_whoz.history_profile")

    return None








