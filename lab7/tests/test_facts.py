import datetime

from youtube_gold.facts import (
    add_trending_day_num,
    add_views_delta,
    build_fact_source,
    dedup_latest_per_day,
    find_missing_channel_keys,
    find_orphans,
    lookup_channel_key,
)

def test_dedup_keeps_latest_row_per_video_and_day(spark):
    df = spark.createDataFrame(
        [
            ("abc", datetime.date(2017, 11, 14), datetime.datetime(2026, 1, 1, 10, 0), 100),
            ("abc", datetime.date(2017, 11, 14), datetime.datetime(2026, 1, 1, 12, 0), 120),
            ("abc", datetime.date(2017, 11, 15), datetime.datetime(2026, 1, 1, 10, 0), 150),
        ],
        ["video_id", "trending_dt", "_silver_processed_at", "views"],
    )

    result = dedup_latest_per_day(df)

    views_by_day = {row["trending_dt"]: row["views"] for row in result.collect()}

    assert views_by_day == {
        datetime.date(2017, 11, 14): 120,
        datetime.date(2017, 11, 15): 150,
    }


def test_lookup_channel_key_keeps_unknown_channel_as_null(spark):
    df = spark.createDataFrame(
        [("v1", "Channel A"), ("v2", "Unknown Channel")],
        ["video_id", "channel_title"],
    )
    dim_channel = spark.createDataFrame(
        [(1, "Channel A"), (2, "Channel B")],
        ["channel_key", "channel_title"],
    )

    result = lookup_channel_key(df, dim_channel)

    key_by_video = {row["video_id"]: row["channel_key"] for row in result.collect()}
    assert key_by_video == {"v1": 1, "v2": None}


def make_history(spark):
    return spark.createDataFrame(
        [
            ("abc", datetime.date(2017, 11, 16), 175),
            ("abc", datetime.date(2017, 11, 14), 100),
            ("xyz", datetime.date(2017, 11, 15), 999),
            ("abc", datetime.date(2017, 11, 15), 150),
        ],
        ["video_id", "trending_dt", "views"],
    )


def test_views_delta_null_orfirst_day_then_difference(spark):
    result = add_views_delta(make_history(spark))

    delta = {(row["video_id"], row["trending_dt"]): row["views_delta"] for row in result.collect()}
    assert delta == {
        ("abc", datetime.date(2017, 11, 14)): None,
        ("abc", datetime.date(2017, 11, 15)): 50,
        ("abc", datetime.date(2017, 11, 16)): 25,
        ("xyz", datetime.date(2017, 11, 15)): None,
    }


def test_trending_day_num_counts_days_per_video(spark):
    result = add_trending_day_num(make_history(spark))

    day_num = {(row["video_id"], row["trending_dt"]): row["trending_day_num"] for row in result.collect()}
    assert day_num == {
        ("abc", datetime.date(2017, 11, 14)): 1,
        ("abc", datetime.date(2017, 11, 15)): 2,
        ("abc", datetime.date(2017, 11, 16)): 3,
        ("xyz", datetime.date(2017, 11, 15)): 1,
    }


def test_build_fact_source_end_to_end(spark):
    silver = spark.createDataFrame(
        [
            ("abc", datetime.date(2017, 11, 14), datetime.datetime(2026, 1, 1, 10, 0), "Channel A", "10", 100, 5, 1, 2),
            ("abc", datetime.date(2017, 11, 14), datetime.datetime(2026, 1, 1, 12, 0), "Channel A", "10", 120, 6, 1, 3),
            ("abc", datetime.date(2017, 11, 15), datetime.datetime(2026, 1, 1, 10, 0), "Channel A", "10", 150, 8, 2, 4),
        ],
        ["video_id", "trending_dt", "_silver_processed_at", "channel_title", "category_id",
         "views", "likes", "dislikes", "comment_count"],
    )
    dim_channel = spark.createDataFrame([(1, "Channel A")], ["channel_key", "channel_title"])

    result = build_fact_source(silver, dim_channel)

    assert result.columns == [
        "date_key", "video_id", "channel_key", "category_id",
        "views", "likes", "dislikes", "comment_count",
        "views_delta", "trending_day_num",
    ]

    rows = {row["date_key"]: row.asDict() for row in result.collect()}
    assert rows == {
        20171114: {
            "date_key": 20171114, "video_id": "abc", "channel_key": 1, "category_id": 10,
            "views": 120, "likes": 6, "dislikes": 1, "comment_count": 3,
            "views_delta": None, "trending_day_num": 1,
        },
        20171115: {
            "date_key": 20171115, "video_id": "abc", "channel_key": 1, "category_id": 10,
            "views": 150, "likes": 8, "dislikes": 2, "comment_count": 4,
            "views_delta": 30, "trending_day_num": 2,
        },
    }

def test_find_missing_channel_keys_returns_only_null_keys(spark):
    fact = spark.createDataFrame([("v1", 1), ("v2", None)], ["video_id", "channel_key"])

    result = find_missing_channel_keys(fact)

    assert [row["video_id"] for row in result.collect()] == ["v2"]


def test_find_orphans_returns_keys_missing_in_dimension(spark):
    fact = spark.createDataFrame([("v1", 10), ("v2", 99)], ["video_id", "category_id"])
    dim_category = spark.createDataFrame([(10, "Music"), (24, "Entertainment")], ["category_id", "category_name"])

    result = find_orphans(fact, dim_category, "category_id")

    assert [row["video_id"] for row in result.collect()] == ["v2"]