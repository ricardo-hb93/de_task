import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import StringType, StructField, StructType
from src.etl.extraction import extract_value_and_unit

EXTRACTION_SCHEMA = StructType(
    [
        StructField("ConsumptionRatingText", StringType(), True),
        StructField("FirstRegYear", StringType(), True),
        StructField("Doors", StringType(), True),
        StructField("InteriorColorText", StringType(), True),
        StructField("Co2EmissionText", StringType(), True),
        StructField("FirstRegMonth", StringType(), True),
        StructField("Seats", StringType(), True),
        StructField("ConsumptionTotalText", StringType(), True),
        StructField("Ccm", StringType(), True),
        StructField("ConditionTypeText", StringType(), True),
        StructField("Properties", StringType(), True),
        StructField("BodyColorText", StringType(), True),
        StructField("DriveTypeText", StringType(), True),
        StructField("TransmissionTypeText", StringType(), True),
        StructField("Km", StringType(), True),
        StructField("Hp", StringType(), True),
        StructField("BodyTypeText", StringType(), True),
        StructField("FuelTypeText", StringType(), True),
        StructField("City", StringType(), True),
        StructField("MakeText", StringType(), True),
        StructField("ModelText", StringType(), True),
        StructField("ModelTypeText", StringType(), True),
        StructField("TypeName", StringType(), True),
        StructField("TypeNameFull", StringType(), True),
        StructField("entity_id", StringType(), True),
        StructField("extracted-value-ConsumptionTotalText", StringType(), True),
        StructField("extracted-unit-ConsumptionTotalText", StringType(), True),
    ]
)


def test_extraction():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    spark = SparkSession.builder.getOrCreate()

    input_df = spark.read.json("de_task/tests/resources/input_extraction.json")

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
                None,
                None,
            ),
            (
                "G",
                "2000",
                "2",
                "schwarz",
                "281 g/km",
                "6",
                "4",
                "11.7 l/100km",
                "5439",
                "Occasion",
                '"Ab MFK"',
                "Silver",
                "Hinterradantrieb",
                "Automat",
                "79500",
                "347",
                "Coupé",
                "Benzin",
                "Zuzwil",
                "Mercedes-Benz",
                "CLK 55 AMG",
                "CLK 55 AMG Avantgarde",
                "CLK 55 AMG Avantgarde",
                "MERCEDES-BENZ CLK 55 AMG Avantgarde",
                "74eb2d24-bc23-46c6-916a-050c40f45836",
                "11.7",
                "l/100km",
            ),
        ],
        EXTRACTION_SCHEMA,
    )

    real_output_df = extract_value_and_unit(input_df)

    pd.testing.assert_frame_equal(
        expected_output_df.sort(EXTRACTION_SCHEMA.names).toPandas(),
        real_output_df.sort(EXTRACTION_SCHEMA.names).toPandas(),
        check_like=True,
    )

    spark_context.stop()
