import pyarrow as pa
from pyiceberg.expressions import EqualTo
import src.storage as storage
import src.storage as storage
import utils.open_meteo as om


def plot_mean_temperature_by_location(catalog: storage.catalog.IcebergCatalog, table_name: str) -> None:
    mean_temps = extract_mean_temps_by_location(catalog, table_name)
    plot_mean_temps(mean_temps)


def extract_mean_temps_by_location(catalog: storage.catalog.IcebergCatalog, table_name: str) -> pa.Table:
    data_points = catalog.catalog.load_table(table_name)
    temperature_data_points = data_points.scan(row_filter=EqualTo('variable_name', om.enums.WeatherVariable.TEMPERATURE_2M.name))
    
    arrow_table = temperature_data_points.to_arrow()
    arrow_aggregated_table = pa.TableGroupBy(arrow_table, 'location_name').aggregate([("value", "mean")])
    
    return arrow_aggregated_table


def plot_mean_temps(mean_temps: pa.Table) -> None:
    import plotly.express as px

    mean_temps_df = mean_temps.to_pandas()
    mean_temps_df = mean_temps_df.sort_values(by=['value_mean'])


    fig = px.bar(mean_temps_df, x='location_name', y='value_mean', 
                labels={'location_name': 'City', 'value_mean': 'Average Temperature'},
                title='Average Temperatures by City')

    fig.show()
