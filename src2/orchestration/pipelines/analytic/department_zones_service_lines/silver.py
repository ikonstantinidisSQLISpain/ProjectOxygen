import sys, os
for _p in [os.path.abspath(p) for p in ['', '..', os.path.join('..', '..')]]:
    if os.path.isdir(os.path.join(_p, 'common')) and _p not in sys.path:
        sys.path.insert(0, _p)
        break

import common.utils as ut
import pyspark.pipelines as dp



def create_zones_table(spark, catalog, platform):
    platform_name = ut.get_platform(spark, platform)
    
    read_quality = ut.get_quality(spark, "silver")
    read_schema = ut.schema_name_builder(read_quality, platform_name)
    target_quality = ut.get_quality(spark, "silver")
    target_schema = ut.schema_name_builder(target_quality, platform_name)

    read_table_dep = ut.table_name_builder("cleaned", "department")
    read_table_sl = ut.table_name_builder("cleaned", "service_line")
    target_table = ut.table_name_builder("cleaned", "zones")
    target_table_dslz = ut.table_name_builder("cleaned", "departments_service_lines_zones")
    read_path_dep = f"{catalog}.{read_schema}.{read_table_dep}"
    read_path_sl = f"{catalog}.{read_schema}.{read_table_sl}"
    target_path = f"{catalog}.{target_schema}.{target_table}"
    target_path_dslz = f"{catalog}.{target_schema}.{target_table_dslz}"

    @dp.table(
        name=target_path,
        comment="Known zones.",
        table_properties={
            "quality": target_quality
        }
    )
    def f():
        dep = spark.readStream.table(read_path_dep)
        sl = spark.readStream.table(read_path_sl)

        zones_d = explode_variant_list_column(spark, 
                                              dep, 
                                              "associated_zone"
                                              ).select(
                                                  F.col("associated_zone").alias("zone_id")
                                                  ).distinct()
        
        zones_sl = explode_variant_list_column(spark, 
                                              sl, 
                                              "associated_zone"
                                              ).select(
                                                  F.col("associated_zone").alias("zone_id")
                                                  ).distinct()
        df = zones_d.union(zones_sl).distinct().select("zone_id").alias("id")
        return df


    @dp.table(
        name=target_path_dslz,
        comment="Department, Service Line and Zones relations."
    )
    def f2():
        dep = spark.readStream.table(read_path_dep)
        sl = spark.readStream.table(read_path_sl)
        return None
    
    return None


