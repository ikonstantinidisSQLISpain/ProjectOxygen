

import pyspark.sql as sql
import pyspark.sql.functions as F
import pyspark.pipelines as dp
import common.utils as ut
import common.constants as c
from pathlib import Path



# For silver accreditation we just need to extract the  aptitudes (skills) and ensure some quality.

def mid_table_factory(sparkSession, read_path, pks, col_to_extract, new_pk_names, extracted_col_name, variant_path):
    
    def f():
        df = sparkSession.readStream.table(read_path)
        return ut.middle_table_extractor(sparkSession, df, pks, col_to_extract, new_pk_names, extracted_col_name, variant_path)
    return f

def silver_quality_pipe_maker(spark, catalog, platform, table_name):
    # Como en la fase silver evitamos agregaciones, extraemos la tabla que contiene la relación, accreditación, profile, skill

    platform_name = ut.get_platform(spark, platform)

    read_quality = ut.get_quality(spark, "bronze")
    read_schema = ut.schema_name_builder(read_quality, platform_name)
    target_quality = ut.get_quality(spark, "silver")
    target_schema = ut.schema_name_builder(target_quality, platform_name)

    read_table = ut.table_name_builder("current", table_name)
    target_table = ut.table_name_builder("cleaned", table_name)
    read_path = f"{catalog}.{read_schema}.{read_table}"
    target_path = f"{catalog}.{target_schema}.{target_table}"

    default_config_file_name = ut.get_conf(spark, "file.default_config")
    table_config_file_name = ut.get_table_metadata_files(spark, "config", platform, table_name)
    table_config_volume = ut.get_table_volume(spark, "config", platform, table_name)
    table_config_path = Path(table_config_volume) / table_config_file_name
    default_config_path = Path(table_config_volume) / default_config_file_name
    table_config = ut.get_table_config(table_config_path, platform, table_name, default_config_path)
    silver_config = table_config.get("silver", None)

    if silver_config is not None:
        cols_to_drop = silver_config.get("cols_to_drop", None)
        drop_metadata = silver_config.get("drop_metadata", True)
        filter_queries = silver_config.get("constraints", None)
        variant_keys_to_extract = silver_config.get("variant_keys_to_extract", None)
        quarantine_queries = silver_config.get("quarantine", None)
        middle_table = silver_config.get("middle_table", None)
        pk_cols = silver_config.get("pk_cols", None)


        @dp.table(
            name=target_path,
            comment=f"{table_name} cleaned and filtered data.",
            table_properties={
                "quality": target_quality
            }
        )
        def f():
            df = spark.readStream.table(read_path)
            if variant_keys_to_extract is not None:
                if not isinstance(variant_keys_to_extract, dict):
                    raise TypeError("variant keys to extract must be a dict. Col: Dict[key, format]")
                for col, keys_format in variant_keys_to_extract.items():
                    df = ut.extract_variant_keys(df, col, keys_format)
                
            if filter_queries is not None:
                df = ut.filter_conditions(df, filter_queries)

            if cols_to_drop is not None:
                df = ut.remove_cols(df, cols_to_drop, drop_metadata)

            if table_name == "talent":
                df = ut.add_seniority(df, "yearsOfExperience")
                df = ut.add_date_status(df, "lastConnectionDate")

            #if table_name == "profile":
                #df = ut.add_bucket_and_status(df, "completionRate")

            if table_name == "collab_status":
                df = ut.classify_status_code(spark, df, "status")

            if table_name == "skill":
                df = ut.extract_all_english_terms(spark, df)

            if table_name == "profile":
                df = ut.add_date_status_prof(df, "lastModifiedDate")

            if table_name in ["certification"]:
                df = ut.add_is_active_col(df, "endDate")

            return df

        if quarantine_queries is not None:
            ut.create_quarantine_table_sanitize(spark, catalog)
            quarantine_schema = ut.get_quarantine_schema(spark)
            quarantine_table = ut.get_quarantine_table(spark, "sanitize")
            quarantine_path = f"{catalog}.{quarantine_schema}.{quarantine_table}"
            if pk_cols is None:
                raise ValueError("PK Columns must be provided in table quality config. It is missing in the json file.")
            @dp.append_flow(
                target=quarantine_path,
                name=f"flow_{target_path.replace('.','_')}_{quarantine_path.replace('.','_')}"
            )
            def f2():
                df = spark.readStream.table(read_path)
                return ut.make_quarantine_table(df, quarantine_queries, pk_cols, read_path)


        if middle_table is not None:

            pks = middle_table.get("pks", None)
            cols_to_extract = middle_table.get("cols_to_extract", None)
            new_pk_names = middle_table.get("pk_new_names", None)
            extracted_cols_names = middle_table.get("extracted_cols_names", None)
            variant_paths = middle_table.get("variant_paths", None)


            if None in [pks, cols_to_extract, new_pk_names, extracted_cols_names]:
                raise ValueError("Missing any of these keys: pks, col_to_extract, pk_new_names, extracted_col_name, extraction_name")

            if isinstance(cols_to_extract, list):
                extraction_names = middle_table.get("extraction_names", None)
                if extraction_names is None:
                    raise ValueError("extraction_names is missing")
                
                if (not isinstance(extracted_cols_names, list) 
                    or not (isinstance(variant_paths, list) or variant_paths is None)
                    or not isinstance(extraction_names, list)
                    ):
                    raise TypeError("If there are multiple cols_to_extract, extracted_cols_names, varian_paths (if provided) and extraction_name must be a list, each one referring to their respective column_to_extract")
                c1 = len(cols_to_extract) != len(extracted_cols_names)
                c2 = len(cols_to_extract) != len(extraction_names)
                c3 = False
                if variant_paths is not None:
                    c3 = len(cols_to_extract) != len(variant_paths)

                if (c1 or c2 or c3):
                    raise ValueError("Len of cols_to_extract, extracted_cols_names and variant_paths (if provided) must match if cols_to_extract is a list.")
                
                for i, col_to_extract in enumerate(cols_to_extract):
                    extraction_name = extraction_names[i]
                    middle_table_name = f"{table_name}_{extraction_name}"
                    middle_table_path = f"{catalog}.{target_schema}.{middle_table_name}"
                    if variant_paths is None:
                        dp.table(
                            name=middle_table_path,
                            comment=f"Multi-valued attribute from {table_name}, attribute {extraction_name}",
                            table_properties={
                                "quality": target_quality
                            }
                        )(
                            mid_table_factory(spark, read_path, pks, col_to_extract, new_pk_names, extracted_cols_names[i], None)
                        )
                    else:
                        dp.table(
                            name=middle_table_path,
                            comment=f"Multi-valued attribute from {table_name}, attribute {extraction_name}",
                            table_properties={
                                "quality": target_quality
                            }
                        )(
                            mid_table_factory(spark, read_path, pks, col_to_extract, new_pk_names, extracted_cols_names[i], variant_paths[i])
                        )
            else:
                extraction_name = middle_table.get("extraction_names", None)
                middle_table_name = f"{table_name}_{extraction_name}"
                middle_table_path = f"{catalog}.{target_schema}.{middle_table_name}"

                @dp.table(
                    name=middle_table_path,
                    comment=f"Multi-valued attribute from {table_name}, attribute {extraction_name}",
                    table_properties={
                        "quality": target_quality
                    }
                )
                def f3():
                    df = spark.readStream.table(read_path)
                    return ut.middle_table_extractor(spark, df, pks, cols_to_extract, new_pk_names, extracted_cols_names, variant_paths)


    return None


def silver_quality_history_profile(spark, catalog):
    # We add the history profile process

    @dp.table(
        name=f"{catalog}.silver_whoz.history_profile",
        comment="History data for profile.",
        table_properties={
            "quality": "silver"
        }
    )
    @dp.expect_or_drop("completionRate_not_null", "completionRate IS NOT NULL")
    @dp.expect_or_drop("completionRate_positive", "completionRate >= 0")
    @dp.expect_or_drop("completionRate_lt_1", "completionRate <= 1")
    def f4():
        workers = spark.read.table(f"{catalog}.bronze_perso.history_workers")
        users = spark.read.table(f"{catalog}.bronze_whoz.history_user")
        talents = spark.read.table(f"{catalog}.bronze_whoz.history_talent")
        profile = spark.read.table(f"{catalog}.bronze_whoz.history_profile")
        collab = spark.read.table(f"{catalog}.bronze_perso.history_collab_status")

        ndf = ut.custom_join(workers, collab, "worker", "collab", workers["id"]==collab["uid"], "left")
        ndf = ut.custom_join(ndf, users, "join1", "user", ndf["mail"] == users["username"], "left")
        ndf = ut.custom_join(ndf, talents, "join2", "talent", ndf["user_id"]==talents["userId"], "left")
        ndf = ut.custom_join(ndf, profile, "join3", "profile", ndf["id"]==profile["talentId"], "left")

        ndf = ndf.where("status != 'Compte Technique'")

        cols_to_drop = [
            "mail",
            "status",
            "collab_snapshot_ts",
            "user_id",
            "username",
            "join2_snapshot_ts",
            "userId", 
            "talent_snapshot_ts",
            "profile_id",
            "talentId",
            "snapshot_ts",
            "join3_id"
        ]
        ndf = ndf.drop(*cols_to_drop).withColumnRenamed("worker_snapshot_ts", c.SNAPSHOT_COL).withColumnRenamed("join1_id", "id")

        return ndf

    return None