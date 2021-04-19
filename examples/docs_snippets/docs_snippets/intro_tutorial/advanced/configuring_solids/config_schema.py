import csv
import requests
from dagster import solid


# start_read_csv_marker
@solid(config_schema={"url": str})
def download_csv(context):
    response = requests.get(context.solid_config["url"])
    lines = response.text.split("\n")
    return [row for row in csv.DictReader(lines)]


# end_read_csv_marker
