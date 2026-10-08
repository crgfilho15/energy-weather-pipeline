import io
import logging

import holidays
import pandas as pd

from src import config
from src.storage import get_s3_client

LOCAL_TZ = "America/New_York"
START_UTC = "2002-01-01 00:00"
END_UTC = "2018-12-31 23:00"
KEY = "curated/source=calendar/dataset=dim_datetime/part-0.parquet"
# federal holidays only (observed days included); years padded so edge dates are covered
US_HOLIDAYS = holidays.country_holidays("US", years=range(2001, 2020))


def build_dim_datetime():
    ts_utc = pd.date_range(START_UTC, END_UTC, freq="h", tz="UTC")
    local = ts_utc.tz_convert(LOCAL_TZ)
    local_naive = local.tz_localize(None)
    offset = local_naive - ts_utc.tz_localize(None)

    dim = pd.DataFrame({"ts_utc": ts_utc})
    dim["local_ts"] = local_naive
    dim["local_date"] = local_naive.date
    dim["local_year"] = local_naive.year
    dim["local_month"] = local_naive.month
    dim["local_hour"] = local_naive.hour
    dim["local_weekday"] = local_naive.dayofweek
    dim["is_weekend"] = dim["local_weekday"] >= 5
    dim["is_dst"] = offset == pd.Timedelta(hours=-4)
    dim["holiday_name"] = [US_HOLIDAYS.get(d) for d in dim["local_date"]]
    dim["is_holiday"] = dim["holiday_name"].notna()
    return dim


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    dim = build_dim_datetime()
    buffer = io.BytesIO()
    dim.to_parquet(buffer, index=False)
    s3 = get_s3_client(config.AWS_PROFILE, config.AWS_REGION)
    s3.put_object(Bucket=config.S3_BUCKET, Key=KEY, Body=buffer.getvalue())
    logging.info("dim_datetime written: %d rows, %d holiday hours", len(dim), int(dim["is_holiday"].sum()))


if __name__ == "__main__":
    main()
