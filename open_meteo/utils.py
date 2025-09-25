import requests_cache
from retry_requests import retry
import openmeteo_requests
import pandas as pd

# 내부 공용 함수


def _get_openmeteo_client():
    cache_session = requests_cache.CachedSession('.cache', expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)


def _build_time_index(block):
    start = pd.to_datetime(block.Time(), unit="s", utc=True)
    end = pd.to_datetime(block.TimeEnd(), unit="s", utc=True)
    freq = pd.Timedelta(seconds=block.Interval())
    return pd.date_range(start=start, end=end, freq=freq, inclusive="left")
