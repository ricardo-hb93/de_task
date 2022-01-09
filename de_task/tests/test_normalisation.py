from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from src.etl.normalization import normalize_df, normalize_color, normalize_make
from src.etl.preprocessing import SCHEMA
import pandas as pd


def test_normalization():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    input_df = spark.read.json("de_task/tests/resources/input_normalization.json")

    expected_output_df = sql_context.createDataFrame(
        [
            (
                "Cabriolet",
                "Silver",
                "Occasion",
                None,
                None,
                "Porrentruy",
                None,
                "Alfa Romeo",
                "2011",
                "23900",
                "kilometer",
                "8C",
                "8C",
                None,
                None,
                None,
                "4",
                None,
            ),
        ],
        SCHEMA,
    )

    real_output_df = normalize_df(input_df)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(SCHEMA.names).toPandas(),
        real_output_df.sort(SCHEMA.names).toPandas(),
        check_like=True,
    )

    spark_context.stop()


def test_normalize_make():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    input_df = sql_context.createDataFrame(
        ["MERCEDES-BENZ", "BMW", "ALFA ROMEO", "DeLorean", "SEAT"], StringType()
    )

    expected_output_df = sql_context.createDataFrame(
        [
            "Mercedes-Benz",
            "BMW",
            "Alfa Romeo",
            "DeLorean",
            "SEAT",
        ],
        StringType(),
    )

    print(input_df)
    real_output_df = input_df.withColumn("value", normalize_make("value"))

    pd.testing.assert_frame_equal(
        expected_output_df.sort("value").toPandas(),
        real_output_df.sort("value").toPandas(),
        check_like=True,
    )

    spark_context.stop()


def test_normalize_color():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)

    input_df = sql_context.createDataFrame(
        ["schwarz mét.", "orange", "bordeaux", "grün", "schwarz", "rot"], StringType()
    )

    expected_output_df = sql_context.createDataFrame(
        ["Black", "Orange", "Other", "Green", "Black", "Red"], StringType()
    )

    real_output_df = input_df.withColumn("value", normalize_color("value"))

    pd.testing.assert_frame_equal(
        expected_output_df.sort("value").toPandas(),
        real_output_df.sort("value").toPandas(),
        check_like=True,
    )

    spark_context.stop()
