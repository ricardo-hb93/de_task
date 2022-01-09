from src.etl.preprocessing import preprocess_input, SCHEMA
from pyspark import SparkContext
from pyspark.sql import SQLContext
import pandas as pd

from pyspark.sql import SparkSession


def test_empty_input():
    spark_context = SparkContext()
    spark = SparkSession.builder.getOrCreate()

    input = spark.read.json(spark_context.parallelize([""]))

    expected_output = spark.read.json(spark_context.parallelize([""]))

    real_output = preprocess_input(input)

    pd.testing.assert_frame_equal(
        expected_output.toPandas(),
        real_output.toPandas(),
        check_like=True,
    )

    spark_context.stop()


def test_rows_from_different_items():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    schema = SCHEMA

    input_df = spark.read.json(
        "de_task/tests/resources/input_rows_from_different_items.json"
    )

    expected_output_df = sql_context.createDataFrame(
        [
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                None,
                None,
                None,
                "SLR",
                "SLR McLaren",
                None,
                None,
                None,
                None,
                None,
            ),
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                None,
                None,
                None,
                "ML 350",
                "ML 350 Inspiration",
                None,
                None,
                None,
                None,
                None,
            ),
        ],
        schema,
    )

    real_output_df = preprocess_input(input_df)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(schema.names).toPandas(),
        real_output_df.sort(schema.names).toPandas(),
        check_like=True,
    )

    spark_context.stop()


def test_rows_from_same_item():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    schema = SCHEMA

    input_df = spark.read.json("de_task/tests/resources/input_rows_from_same_item.json")

    expected_output_df = sql_context.createDataFrame(
        [
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "2007",
                None,
                None,
                "SLR",
                "SLR McLaren",
                None,
                None,
                None,
                None,
                None,
            ),
        ],
        schema,
    )

    real_output_df = preprocess_input(input_df)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(schema.names).toPandas(),
        real_output_df.sort(schema.names).toPandas(),
        check_like=True,
    )

    spark_context.stop()


def test_multiple_rows():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    schema = SCHEMA

    input_df = spark.read.json("de_task/tests/resources/input_rows_multiple.json")

    expected_output_df = sql_context.createDataFrame(
        [
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "LAMBORGHINI",
                None,
                None,
                None,
                "MURCIÉLAGO",
                "Murciélago LP640-4 Cpé",
                None,
                None,
                None,
                None,
                "l_km_consumption",
            ),
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "2003",
                None,
                None,
                "ML 350",
                "ML 350 Inspiration",
                None,
                None,
                None,
                None,
                None,
            ),
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "2007",
                None,
                None,
                "SLR",
                "SLR McLaren",
                None,
                None,
                None,
                "10",
                None,
            ),
        ],
        schema,
    )

    real_output_df = preprocess_input(input_df)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(schema.names).toPandas(),
        real_output_df.sort(schema.names).toPandas(),
        check_like=True,
    )

    spark_context.stop()


def test_different_values_for_same_attribute():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    schema = SCHEMA

    input_df = spark.read.json("de_task/tests/resources/input_rows_from_same_item.json")

    expected_output_df = sql_context.createDataFrame(
        [
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "2007",
                None,
                None,
                "SLR",
                "SLR McLaren",
                None,
                None,
                None,
                None,
                None,
            ),
        ],
        schema,
    )

    real_output_df = preprocess_input(input_df)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(schema.names).toPandas(),
        real_output_df.sort(schema.names).toPandas(),
        check_like=True,
    )

    spark_context.stop()
