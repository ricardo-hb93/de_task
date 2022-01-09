import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from src.etl.preprocessing import SUPPLIER_SCHEMA, preprocess_input


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

    schema = SUPPLIER_SCHEMA

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
                "2",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "SLR",
                "SLR McLaren",
                "McLaren",
                "MERCEDES-BENZ SLR McLaren",
                "0001fda6-192b-46a8-bc08-0e833f904eed",
            ),
            (
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "235",
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "ML 350",
                "ML 350 Inspiration",
                "ML 350 Inspiration",
                "MERCEDES-BENZ ML 350 Inspiration",
                "00107c2d-0071-4475-88f0-810133638b7e",
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

    schema = SUPPLIER_SCHEMA

    input_df = spark.read.json("de_task/tests/resources/input_rows_from_same_item.json")

    expected_output_df = sql_context.createDataFrame(
        [
            (
                None,
                "2007",
                None,
                None,
                None,
                None,
                "2",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "SLR",
                "SLR McLaren",
                "McLaren",
                "MERCEDES-BENZ SLR McLaren",
                "0001fda6-192b-46a8-bc08-0e833f904eed",
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

    schema = SUPPLIER_SCHEMA

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
                "21.3 l/100km",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "LAMBORGHINI",
                "MURCIÉLAGO",
                "Murciélago LP640-4 Cpé",
                "Murciélago LP640-4 Cpé",
                "LAMBORGHINI Murciélago LP640-4 Cpé",
                "4ff3d5d3-4fb0-492a-b536-9d4b2500394c",
            ),
            (
                None,
                "2003",
                None,
                None,
                None,
                None,
                "5",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "235",
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "ML 350",
                "ML 350 Inspiration",
                "ML 350 Inspiration",
                "MERCEDES-BENZ ML 350 Inspiration",
                "00107c2d-0071-4475-88f0-810133638b7e",
            ),
            (
                None,
                "2007",
                None,
                None,
                None,
                "10",
                "2",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "SLR",
                "SLR McLaren",
                "McLaren",
                "MERCEDES-BENZ SLR McLaren",
                "0001fda6-192b-46a8-bc08-0e833f904eed",
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

    schema = SUPPLIER_SCHEMA

    input_df = spark.read.json("de_task/tests/resources/input_rows_from_same_item.json")

    expected_output_df = sql_context.createDataFrame(
        [
            (
                None,
                "2007",
                None,
                None,
                None,
                None,
                "2",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                "MERCEDES-BENZ",
                "SLR",
                "SLR McLaren",
                "McLaren",
                "MERCEDES-BENZ SLR McLaren",
                "0001fda6-192b-46a8-bc08-0e833f904eed",
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
