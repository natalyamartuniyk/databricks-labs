from pyspark.sql import DataFrame
from pyspark.sql import functions as F

def date_range(df: DataFrame) -> DataFrame:
    return(
        df.agg(
            F.min("trending_dt").alias("start_date"),
            F.max("trending_dt").alias("end_date"),
        )
        .select(
            F.explode(
                F.sequence("start_date", "end_date", F.expr("INTERVAL 1 DAY"))
            ).alias("full_date")
        )
    )

def add_calendar_attributes(df: DataFrame) -> DataFrame:
    return df.select(
        F.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        F.year("full_date").alias("year"),
        F.quarter("full_date").alias("quarter"),
        F.month("full_date").alias("month"),
        F.date_format("full_date", "MMMM").alias("month_name"),
        F.dayofmonth("full_date").alias("day_of_month"),
        F.dayofweek("full_date").alias("day_of_week"),
        F.date_format("full_date", "EEEE").alias("day_name"),
        F.dayofweek("full_date").isin(1, 7).alias("is_weekend"),
    )

def build_dim_date(df: DataFrame) -> DataFrame:
    return add_calendar_attributes(date_range(df))