from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import first, when, col, lit

SCHEMA = StructType(
    [
        StructField("carType", StringType(), True),
        StructField("color", StringType(), True),
        StructField("condition", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("drive", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("make", StringType(), True),
        StructField("manufacture_year", StringType(), True),
        StructField("mileage", StringType(), True),
        StructField("mileage_unit", StringType(), True),
        StructField("model", StringType(), True),
        StructField("model_variant", StringType(), True),
        StructField("price_on_request", StringType(), True),
        StructField("type", StringType(), True),
        StructField("zip", StringType(), True),
        StructField("manufacture_month", StringType(), True),
        StructField("fuel_consumption_unit", StringType(), True),
    ]
)


def preprocess_input(input_df: DataFrame) -> DataFrame:
    if len(input_df.head(1)) == 0:
        return input_df

    output_df = (
        input_df.groupBy("ID")
        .agg(
            first(
                when(
                    col("Attribute Names").contains("BodyTypeText"),
                    col("Attribute Values"),
                ),
                ignorenulls=True,
            ).alias("carType"),
            first(
                when(
                    col("Attribute Names").contains("BodyColorText"),
                    col("Attribute Values"),
                ),
                ignorenulls=True,
            ).alias("color"),
            first(
                when(
                    col("Attribute Names").contains("ConditionTypeText"),
                    col("Attribute Values"),
                ),
                ignorenulls=True,
            ).alias("condition"),
            first(
                when(col("Attribute Names").contains("City"), col("Attribute Values")),
                ignorenulls=True,
            ).alias("city"),
            first("MakeText", ignorenulls=True).alias("make"),
            first(
                when(
                    col("Attribute Names").contains("FirstRegYear"),
                    col("Attribute Values"),
                ),
                ignorenulls=True,
            ).alias(  # I'm assuming the FirstRegYear is the manufacture year
                "manufacture_year"
            ),
            first(
                when(col("Attribute Names").contains("Km"), col("Attribute Values")),
                ignorenulls=True,
            ).alias("mileage"),
            first(
                when(col("Attribute Names").contains("Km"), "kilometer"),
                ignorenulls=True,
            ).alias("mileage_unit"),
            first("ModelText", ignorenulls=True).alias("model"),
            first("ModelTypeText", ignorenulls=True).alias("model_variant"),
            first(
                when(
                    col("Attribute Names").contains("FirstRegMonth"),
                    col("Attribute Values"),
                ),
                ignorenulls=True,
            ).alias(  # I'm assuming the FirstRegMonth is the manufacture month
                "manufacture_month"
            ),
            first(
                when(
                    col("Attribute Names").contains("ConsumptionTotalText")
                    & col("Attribute Values").contains("l/100km"),
                    "l_km_consumption",
                ),
                ignorenulls=True,
            ).alias("fuel_consumption_unit"),
        )
        .withColumn("currency", lit(None).cast(StringType()))
        .withColumn("drive", lit(None).cast(StringType()))
        .withColumn("country", lit(None).cast(StringType()))
        .withColumn("price_on_request", lit(None).cast(StringType()))
        .withColumn("type", lit(None).cast(StringType()))
        .withColumn("zip", lit(None).cast(StringType()))
    )

    return output_df.select(SCHEMA.names)
