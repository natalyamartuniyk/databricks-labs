import datetime

from youtube_gold.scope import filter_to_period, parse_trending_date, scope_silver


def test_parse_trending_date_reads_year_day_month(spark):
    df = spark.createDataFrame([("17.14.11",)], ["trending_date"])

    result = parse_trending_date(df).collect()[0]

    assert result["trending_dt"] == datetime.date(2017, 11, 14)


def test_filter_to_period_keeps_boundaries(spark):
    df = spark.createDataFrame(
        [
            (datetime.date(2017, 11, 13),),
            (datetime.date(2017, 11, 14),),
            (datetime.date(2018, 6, 14),),
            (datetime.date(2018, 6, 15),),
        ],
        ["trending_dt"],
    )

    result = filter_to_period(df, "2017-11-14", "2018-06-14")

    dates = sorted(row["trending_dt"] for row in result.collect())
    assert dates == [datetime.date(2017, 11, 14), datetime.date(2018, 6, 14)]


def test_scope_silver_drops_rows_outside_period(spark):
    df = spark.createDataFrame(
        [
            ("v1", "17.14.11"),
            ("v2", "18.14.06"),
            ("v3", "26.01.01"),
        ],
        ["video_id", "trending_date"],
    )

    result = scope_silver(df, "2017-11-14", "2018-06-14")

    ids = sorted(row["video_id"] for row in result.collect())
    assert ids == ["v1", "v2"]