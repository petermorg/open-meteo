import openmeteo_requests
from openmeteo_sdk.WeatherApiResponse import WeatherApiResponse, VariablesWithTime
from utils.open_meteo.enums import Location, WeatherVariable


def fetch_forecasts(
        locations: list[Location],
        variables: list[WeatherVariable],
) -> list[WeatherApiResponse]:
    parameters = _construct_parameters(locations, variables)
    forecast_responses = _call_open_meteo_api(parameters)
    return forecast_responses


def _construct_parameters(locations: list[Location], variables: list[WeatherVariable]) -> dict:
    lat_list = [location.value[0] for location in locations]
    long_list = [location.value[1] for location in locations]
    hourly_variable_list = [variable.value for variable in variables]

    parameters = {
        "latitude": lat_list,
        "longitude": long_list,
        "hourly": hourly_variable_list,
    }

    return parameters


def _call_open_meteo_api(parameters):
    om = openmeteo_requests.Client()
    responses = om.weather_api("https://api.open-meteo.com/v1/forecast", params=parameters)
    return responses
