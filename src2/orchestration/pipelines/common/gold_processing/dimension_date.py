from pyspark.sql import functions as F
import common.utils as ut
import pyspark.pipelines as dp

# ------------------------------------------------------------
# Configuración
# ------------------------------------------------------------
#CATALOG = ut.get_conf(spark, "catalog")
#TABLE_NAME = f"{CATALOG}.gold_date.dim_date"

START_DATE = "2025-01-01"
END_DATE   = "2050-12-31"

# ------------------------------------------------------------
# Crear rango de fechas
# ------------------------------------------------------------

def make_date_range():
    df = (
        spark.range(1)
        .select(
            F.explode(
                F.sequence(
                    F.to_date(F.lit(START_DATE)),
                    F.to_date(F.lit(END_DATE)),
                    F.expr("INTERVAL 1 DAY")
                )
            ).alias("Date")
        )
    )
    return df

def make_seconds_in_day_range():

    n_seconds = 24*60*60

    seconds = (
        spark.range(n_seconds)
        .select(F.col("id").alias("sec_of_day"))
        .withColumn("hour", F.floor(F.col("sec_of_day") / 3600).cast("int"))
        .withColumn("minute", F.floor((F.col("sec_of_day") % 3600) / 60).cast("int"))
        .withColumn("second", (F.col("sec_of_day") % 60).cast("int"))
        .withColumn(
            "time",
            F.concat_ws(
                ":",
                F.lpad(F.col("hour").cast("string"), 2, "0"),
                F.lpad(F.col("minute").cast("string"), 2, "0"),
                F.lpad(F.col("second").cast("string"), 2, "0"),
            ),
        )
    )

    seconds = (
        seconds
        .withColumn(
            "am_pm",
            F.when(F.col("hour") < 12, F.lit("AM")).otherwise(F.lit("PM"))
        )
        .withColumn(
            "hour_12",
            F.when(F.col("hour") % 12 == 0, F.lit(12))
            .otherwise(F.col("hour") % 12)
            .cast("int")
        )
        .withColumn(
            "time_12",
            F.concat_ws(
                ":",
                F.lpad(F.col("hour_12").cast("string"), 2, "0"),
                F.lpad(F.col("minute").cast("string"), 2, "0"),
                F.lpad(F.col("second").cast("string"), 2, "0"),
            ),
        )
    )

    return seconds

# ------------------------------------------------------------
# Nombres de días y meses en español
# ------------------------------------------------------------
day_names = F.create_map(
    F.lit(1), F.lit("Monday"),
    F.lit(2), F.lit("Tuesday"),
    F.lit(3), F.lit("Wednesday"),
    F.lit(4), F.lit("Thursday"),
    F.lit(5), F.lit("Friday"),
    F.lit(6), F.lit("Saturday"),
    F.lit(7), F.lit("Sunday")
)

month_names = F.create_map(
    F.lit(1),  F.lit("January"),
    F.lit(2),  F.lit("February"),
    F.lit(3),  F.lit("March"),
    F.lit(4),  F.lit("April"),
    F.lit(5),  F.lit("May"),
    F.lit(6),  F.lit("June"),
    F.lit(7),  F.lit("July"),
    F.lit(8),  F.lit("August"),
    F.lit(9),  F.lit("September"),
    F.lit(10), F.lit("October"),
    F.lit(11), F.lit("November"),
    F.lit(12), F.lit("December")
)

# ------------------------------------------------------------
# Construcción de la dimensión calendario
# ------------------------------------------------------------
def make_calendar_df():
    df_calendar = (
        make_date_range()
        .withColumn("Year", F.year("Date"))
        .withColumn("Month", F.month("Date"))
        .withColumn("Day", F.dayofmonth("Date"))

        # Día de la semana: 1 = Lunes ... 7 = Domingo
        .withColumn("DayOfWeek", F.dayofweek("Date"))
        .withColumn(
            "DayName",
            day_names[F.dayofweek("Date")]
        )
        .withColumn(
            "DayShort",
            F.substring(day_names[F.dayofweek("Date")], 1, 3)
        )

        # Mes
        .withColumn(
            "MonthName",
            month_names[F.month("Date")]
        )
        .withColumn(
            "MonthShort",
            F.substring(month_names[F.month("Date")], 1, 3)
        )

        # Trimestre
        .withColumn("Quarter", F.quarter("Date"))
        .withColumn(
            "QuarterYear",
            F.concat(
                F.lit("Q"),
                F.quarter("Date"),
                F.lit("-"),
                F.year("Date")
            )
        )

        # Semana ISO
        .withColumn("WeekNumber", F.weekofyear("Date"))

        # Indicadores relativos a la fecha actual
        .withColumn(
            "IsCurrentMonth",
            (
                (F.year("Date") == F.year(F.current_date())) &
                (F.month("Date") == F.month(F.current_date()))
            )
        )
        .withColumn(
            "IsCurrentYear",
            F.year("Date") == F.year(F.current_date())
        )

        # Selección y orden final de columnas
        .select(
            "Date",
            "Year",
            "Month",
            "Day",
            "DayName",
            "DayShort",
            "MonthName",
            "MonthShort",
            "Quarter",
            "QuarterYear",
            "WeekNumber",
            "IsCurrentMonth",
            "IsCurrentYear"
        )
        .orderBy("Date")
    )

    return df_calendar

# ------------------------------------------------------------
# Crear/reemplazar tabla Delta
# ------------------------------------------------------------
"""(
    df_calendar
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABLE_NAME)
)"""

def make_dim_date_pipe_maker(catalog):
    TABLE_NAME = f"{catalog}.gold_date.dim_date"

    @dp.table(
        name= TABLE_NAME,
        comment="Dimension Date. Adds the date metadata for each date.",
        table_properties={
            "quality": "gold"
        }
    )
    def f():
        return make_calendar_df()

    TABLE_NAME_HOUR = f"{catalog}.gold_date.dim_hour"

    @dp.table(
        name= TABLE_NAME_HOUR,
        comment="Dimension Hour. Adds the daily time metadata.",
        table_properties={
            "quality": "gold"
        }
    )
    def f2():
        return make_seconds_in_day_range()

    return None

