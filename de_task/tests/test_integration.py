import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType, StructField, StructType
from src.etl.integration import integrate

INTEGRATION_SCHEMA = StructType(
    [
        StructField("color", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("model_variant", StringType(), True),
        StructField("city", StringType(), True),
    ]
)


def test_same_number_of_records():
    spark_context = SparkContext()
    spark = SparkSession.builder.getOrCreate()

    input_df = spark.read.json("de_task/tests/resources/input_integration.json")

    real_output_df = integrate(input_df)

    assert real_output_df.count() == 5

    spark_context.stop()


def test_integration():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    input_df = spark.read.json("de_task/tests/resources/input_integration.json")

    expected_output_df = sql_context.createDataFrame(
        [
            ("Silver", "BMW", "645", "645i", "Zuzwil"),
            ("Silver", "Porsche", "911", "911 GT3", "Zuzwil"),
            ("Silver", "Mercedes-Benz", "E 420", "E 420 Avantgarde", "Zuzwil"),
            ("Silver", "Porsche", "911", "911 Carrera 4 S", "Zuzwil"),
            ("Silver", "Aston Martin", "DB9", "DB9 Volante", "Zuzwil"),
        ],
        INTEGRATION_SCHEMA,
    )

    real_output_df = integrate(input_df)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(INTEGRATION_SCHEMA.names).toPandas(),
        real_output_df.sort(INTEGRATION_SCHEMA.names).toPandas(),
        check_like=True,
    )

    spark_context.stop()
