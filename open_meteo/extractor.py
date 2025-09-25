from typing import Literal, Sequence
import pandas as pd
from .utils import _get_openmeteo_client, _build_time_index


class OpenMeteoExtractor:
    DAILY_FEATURES = [
        "weather_code",
        "temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
        "apparent_temperature_mean", "apparent_temperature_max", "apparent_temperature_min",
        "sunrise", "sunset", "daylight_duration", "sunshine_duration",
        "precipitation_sum", "rain_sum", "snowfall_sum", "precipitation_hours",
        "wind_speed_10m_max", "wind_gusts_10m_max", "wind_direction_10m_dominant",
        "shortwave_radiation_sum", "et0_fao_evapotranspiration"
    ]
    HOURLY_FEATURES = [
        "temperature_2m", "relative_humidity_2m",
        "dew_point_2m", "apparent_temperature",
        "precipitation", "rain", "snowfall", "snow_depth", "weather_code",
        "pressure_msl", "surface_pressure", "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high",
        "et0_fao_evapotranspiration", "vapour_pressure_deficit",
        "wind_speed_10m", "wind_speed_100m", "wind_direction_10m", "wind_direction_100m", "wind_gusts_10m",
        "soil_temperature_0_to_7cm", "soil_temperature_7_to_28cm", "soil_temperature_28_to_100cm", "soil_temperature_100_to_255cm",
        "soil_moisture_0_to_7cm", "soil_moisture_7_to_28cm", "soil_moisture_28_to_100cm", "soil_moisture_100_to_255cm",
        "shortwave_radiation", "direct_radiation", "diffuse_radiation", "direct_normal_irradiance", "global_tilted_irradiance", "terrestrial_radiation", "shortwave_radiation_instant", "direct_radiation_instant", "diffuse_radiation_instant", "direct_normal_irradiance_instant", "global_tilted_irradiance_instant", "terrestrial_radiation_instant"
    ]
    url = "https://archive-api.open-meteo.com/v1/archive"

    def __init__(self):
        self.client = _get_openmeteo_client()

    def _validate_features(self, mode: Literal["daily", "hourly"], features: Sequence[str]) -> None:
        allowed = set(self.DAILY_FEATURES if mode == "daily" else self.HOURLY_FEATURES)
        unknown = set(features) - allowed
        if unknown:
            raise ValueError(f"Unknown {mode} features: {sorted(unknown)}")
        if not features:
            raise ValueError("features must be a non-empty sequence")

    def _set_params_by(self, mode: Literal["daily", "hourly"], features: Sequence[str], start_date: str, end_date: str, latitude: float, longitude: float, timezone: str):
        self._validate_features(mode, features)
        base = {
            "latitude": latitude,
            "longitude": longitude,
            "start_date": start_date,
            "end_date": end_date,
            "timezone": timezone,
        }
        key = "daily" if mode == "daily" else "hourly"
        base[key] = list(features)
        return base

    def __call__(self, mode: Literal["daily", "hourly"], features: Sequence[str], start_date: str, end_date: str, latitude: float, longitude: float, timezone: str):
        params = self._set_params_by(mode, features, start_date, end_date, latitude, longitude, timezone)
        responses = self.client.weather_api(self.url, params=params)
        response = responses[0]
        if mode == 'daily':
            daily = response.Daily()
            idx = _build_time_index(daily).tz_convert(timezone).tz_localize(None)
            df = pd.DataFrame({"date": idx})
            for i, var in enumerate(features):
                vals = daily.Variables(i)
                if var in ("sunrise", "sunset"):
                    df[var] = (
                        pd.to_datetime(vals.ValuesInt64AsNumpy(), unit="s", utc=True)
                        .tz_convert(timezone)
                        .tz_localize(None)
                    )
                elif var == 'shortwave_radiation_sum':
                    df["shortwave_radiation_sum_kWh_m2"] = vals.ValuesAsNumpy() * 0.27778

                else:
                    df[var] = vals.ValuesAsNumpy()
            return df
        else:
            hourly = response.Hourly()
            idx = _build_time_index(hourly).tz_convert(timezone).tz_localize(None)
            df = pd.DataFrame({"time": idx})

            for i, var in enumerate(features):
                df[var] = hourly.Variables(i).ValuesAsNumpy()
            return df
