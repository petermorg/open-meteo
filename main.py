import datetime
import logging
import numpy as np
import pyarrow as pa
import src.storage as storage
import src.viz as viz
import utils.open_meteo as om

iceberg_catalog = storage.catalog.IcebergCatalog()
table_name = 'default.data_points'


def main():
    configure_logging()
    load_open_meteo_data()
    viz.plot_mean_temperature_by_location(iceberg_catalog, table_name)


def load_open_meteo_data():
    locations = [l for l in om.enums.Location]  # all locations
    variables = [v for v in om.enums.WeatherVariable]  # all weather variables

    extract_time = datetime.datetime.now()
    forecast_responses = om.api_helpers.fetch_forecasts(locations, variables)

    process_responses(forecast_responses, locations, variables, extract_time)


def process_responses(
        responses: list[om.api_helpers.WeatherApiResponse], 
        locations: list[om.enums.Location], 
        variables: list[om.enums.WeatherVariable],
        extract_time: datetime.datetime,
        ) -> None:
    
    logging.info(f"Processing {len(responses)} weather forecast responses")
    assert len(responses) == len(locations), "The number of responses must match the number of locations"

    errors = []

    for i, response in enumerate(responses):
        try:
            process_response(response, locations[i], variables, extract_time)
        except Exception as e:
            logging.error(f"Error processing response for location {locations[i]}: {e}")
            errors.append(e)

    if errors:
        raise Exception(f"Errors occurred when processing responses: {errors}")


def process_response(
        response: om.api_helpers.WeatherApiResponse, 
        location: om.enums.Location, 
        variables: list[om.enums.WeatherVariable],
        extract_time: datetime.datetime,
        ) -> None:
    
    logging.info(f"Processing response for location {location}")
    hourly_response = response.Hourly()
    assert hourly_response.VariablesLength() == len(variables), "The number of variables in the response must match the number of requested variables"

    full_table = None

    for i, variable in enumerate(variables):
        table = convert_response_to_pyarrow_table(hourly_response, i, variable.name, location.name, extract_time)

        if full_table is None:
            full_table = table
        else:
            full_table = pa.concat_tables([full_table, table])            

    add_table_to_iceberg(full_table)


def convert_response_to_pyarrow_table(
        hourly_response: om.api_helpers.VariablesWithTime, 
        variable_index: int,
        variable_name: str,
        location_name: str,
        extract_time: datetime.datetime,
        ) -> pa.Table:
    
    start_time = hourly_response.Time()
    end_time = hourly_response.TimeEnd()
    interval = hourly_response.Interval()

    timestamps_array = pa.array([
        datetime.datetime.fromtimestamp(ts)
        for ts in np.arange(start_time, end_time, interval)
    ])

    arrays = [
        timestamps_array,
        hourly_response.Variables(variable_index).ValuesAsNumpy(),
        pa.array([variable_name] * len(timestamps_array), pa.string()),
        pa.array([location_name] * len(timestamps_array), pa.string()),
        pa.array([extract_time] * len(timestamps_array), pa.timestamp('ms'))
    ]

    fields = [
        pa.field('forecast_time', pa.timestamp('s'), nullable=False),
        pa.field('value', pa.float32(), nullable=False),
        pa.field('variable_name', pa.string(), nullable=False),
        pa.field('location_name', pa.string(), nullable=False),
        pa.field('extract_time', pa.timestamp('ms'), nullable=False),
    ]

    table = pa.table(arrays, schema=pa.schema(fields))

    return table


def add_table_to_iceberg(table: pa.Table) -> None:
    schema = storage.schemas.schema
    iceberg_table = iceberg_catalog.initialise_table(table_name, schema)

    iceberg_table.append(df=table)


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler()
        ]
    )

if __name__ == "__main__":
    configure_logging()
    main()
