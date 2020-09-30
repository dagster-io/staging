import os

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.functions import max as pyspark_max

from dagster import Field, InputDefinition, String, pipeline, solid


def create_spark_session():
    return SparkSession.builder.getOrCreate()


def df_from_csv(path):
    spark_session = create_spark_session()
    return spark_session.read.option("header", True).format("csv").load(path)


def df_to_csv(df, path):
    df.toPandas().to_csv(path)


@solid(
    config_schema={
        "temperature_file": Field(String),
        "dir": Field(String),
        "dummy_val": Field(String),
    }
)
def get_max_temp_per_station(context):
    fpath = os.path.join(context.solid_config["dir"], context.solid_config["temperature_file"])
    tmpf_df = df_from_csv(fpath)
    w = Window.partitionBy("station")
    max_df = (
        tmpf_df.withColumn("maxTmpf", pyspark_max("tmpf").over(w))
        .where(col("tmpf") == col("maxTmpf"))
        .drop("maxTmpf")
    )
    selected_cols_df = max_df.selectExpr(
        ["station as airport_code", "valid as date", "tmpf as temperature_f"]
    )
    outpath = os.path.join(context.solid_config["dir"], "maxtemp.csv")
    df_to_csv(selected_cols_df, outpath)
    return outpath


@solid(
    config_schema={"station_file": Field(String), "dir": Field(String), "dummy_val": Field(String)}
)
def get_consolidated_location(context):
    fpath = os.path.join(context.solid_config["dir"], context.solid_config["station_file"])
    station_df = df_from_csv(fpath)
    consolidated_df = station_df.withColumn(
        "full_address",
        concat(
            lit("Country: "),
            col("country"),
            lit(", State: "),
            col("state"),
            lit(", Zip: "),
            col("zip"),
        ),
    )
    consolidated_df = consolidated_df.select(col("station"), col("full_address"))
    outpath = os.path.join(context.solid_config["dir"], "stationcons.csv")
    df_to_csv(consolidated_df, outpath)
    return outpath


@solid(
    config_schema={"dummy_val": Field(String), "dir": Field(String)},
    input_defs=[
        InputDefinition(name="maxtemp_path", dagster_type=String),
        InputDefinition(name="stationcons_path", dagster_type=String),
    ],
)
def combine_dfs(context, maxtemp_path, stationcons_path):
    maxtemps = df_from_csv(maxtemp_path)
    stationcons = df_from_csv(stationcons_path)
    joined_temps = maxtemps.join(stationcons, col("airport_code") == col("station")).select(
        col("full_address"), col("temperature_f")
    )
    outpath = os.path.join(context.solid_config["dir"], "temp_for_place.csv")
    df_to_csv(joined_temps, outpath)
    return outpath


@solid(
    config_schema={"dummy_val": Field(String), "dir": Field(String)},
    input_defs=[InputDefinition(name="path", dagster_type=String),],
)
def pretty_output(context, path):
    temp_for_place = df_from_csv(path)
    pretty_result = temp_for_place.withColumn(
        "temperature_info",
        concat(col("full_address"), lit(", temp (Fahrenheit): "), col("temperature_f")),
    )
    pretty_result = pretty_result.select(col("temperature_info"))
    outpath = os.path.join(context.solid_config["dir"], "pretty_output.csv")
    df_to_csv(pretty_result, outpath)
    return outpath


@pipeline
def pyspark_assets_pipeline():
    pretty_output(combine_dfs(get_max_temp_per_station(), get_consolidated_location()))
