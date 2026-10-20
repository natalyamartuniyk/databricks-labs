import datetime

from youtube_gold.dimensions import add_calendar_attributes, date_range


def test_date_range_fills_days_without_data(spark):
    df = spark.createDataFrame(
        [
            (datetime.date(2017, 11, 14),),
            (datetime.date(2017, 11, 14),),
            (datetime.date(2017, 11, 17),),
        ],
        ["trending_dt"],
    )

    result = date_range(df)

    dates = sorted(row["full_date"] for row in result.collect())
    assert dates == [
        datetime.date(2017, 11, 14),
        datetime.date(2017, 11, 15),
        datetime.date(2017, 11, 16),
        datetime.date(2017, 11, 17),
    ]


def test_calendar_attributes_for_one_day(spark):
    df = spark.createDataFrame([(datetime.date(2017, 11, 14),)], ["full_date"])

    result = add_calendar_attributes(df).collect()[0].asDict()

    assert result == {
        "date_key": 20171114,
        "full_date": datetime.date(2017, 11, 14),
        "year": 2017,
        "quarter": 4,
        "month": 11,
        "month_name": "November",
        "day_of_month": 14,
        "day_of_week": 3,
        "day_name": "Tuesday",
        "is_weekend": False,
    }


def test_is_weekend_only_saturday_and_sunday(spark):
    df = spark.createDataFrame(
        [
            (datetime.date(2017, 11, 17),),
            (datetime.date(2017, 11, 18),),
            (datetime.date(2017, 11, 19),),
            (datetime.date(2017, 11, 20),),
        ],
        ["full_date"],
    )

    result = add_calendar_attributes(df)

    weekend = {row["day_name"]: row["is_weekend"] for row in result.collect()}
    assert weekend == {
        "Friday": False,
        "Saturday": True,
        "Sunday": True,
        "Monday": False,
    }