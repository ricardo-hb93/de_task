import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType
from src.etl.normalization import normalize_color, normalize_df, normalize_make
from src.etl.preprocessing_new import SUPPLIER_SCHEMA


def test_normalization():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    input_df = spark.read.json("de_task/tests/resources/input_normalization.json")

    expected_output_df = sql_context.createDataFrame(
        [
            (
                None,
                "2011",
                "2",
                "schwarz",
                None,
                "4",
                "0",
                None,
                "4691",
                "Occasion",
                '"Ab MFK"',
                "Silver",
                "Hinterradantrieb",
                "Automatik-Getriebe",
                "23900",
                "450",
                "Cabriolet",
                "Benzin",
                "Porrentruy",
                "Alfa Romeo",
                "8C",
                "8C",
                None,
                "ALFA ROMEO 8C",
                "b5b48f5d-9170-4388-9160-41d3c756e0e1",
            )
        ],
        SUPPLIER_SCHEMA,
    )

    attribute_list = ["MakeText", "BodyColorText"]

    real_output_df = normalize_df(input_df, attribute_list)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(SUPPLIER_SCHEMA.names).toPandas(),
        real_output_df.sort(SUPPLIER_SCHEMA.names).toPandas(),
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
