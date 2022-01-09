from pyspark.sql import DataFrame
from pyspark.sql.functions import col, first, when
from pyspark.sql.types import StringType, StructField, StructType

FINAL_SCHEMA = StructType(
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

SUPPLIER_SCHEMA = StructType(
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
    ]
)


def preprocess_input(input_df: DataFrame) -> DataFrame:
    if len(input_df.head(1)) == 0:
        return input_df

    output_df = input_df.groupBy("ID").agg(
        first(
            when(
                col("Attribute Names").contains("ConsumptionRatingText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("ConsumptionRatingText"),
        first(
            when(
                col("Attribute Names").contains("FirstRegYear"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("FirstRegYear"),
        first(
            when(
                col("Attribute Names").contains("Doors"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("Doors"),
        first(
            when(
                col("Attribute Names").contains("InteriorColorText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("InteriorColorText"),
        first(
            when(
                col("Attribute Names").contains("Co2EmissionText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("Co2EmissionText"),
        first(
            when(
                col("Attribute Names").contains("FirstRegMonth"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("FirstRegMonth"),
        first(
            when(
                col("Attribute Names").contains("Seats"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("Seats"),
        first(
            when(
                col("Attribute Names").contains("ConsumptionTotalText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("ConsumptionTotalText"),
        first(
            when(
                col("Attribute Names").contains("Ccm"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("Ccm"),
        first(
            when(
                col("Attribute Names").contains("ConditionTypeText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("ConditionTypeText"),
        first(
            when(
                col("Attribute Names").contains("Properties"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("Properties"),
        first(
            when(
                col("Attribute Names").contains("BodyColorText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("BodyColorText"),
        first(
            when(
                col("Attribute Names").contains("DriveTypeText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("DriveTypeText"),
        first(
            when(
                col("Attribute Names").contains("TransmissionTypeText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("TransmissionTypeText"),
        first(
            when(
                col("Attribute Names").contains("Km"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("Km"),
        first(
            when(
                col("Attribute Names").contains("Hp"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("Hp"),
        first(
            when(
                col("Attribute Names").contains("BodyTypeText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("BodyTypeText"),
        first(
            when(
                col("Attribute Names").contains("FuelTypeText"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("FuelTypeText"),
        first(
            when(
                col("Attribute Names").contains("City"),
                col("Attribute Values"),
            ),
            ignorenulls=True,
        ).alias("City"),
        first(
            "MakeText",
            ignorenulls=True,
        ).alias("MakeText"),
        first(
            "ModelText",
            ignorenulls=True,
        ).alias("ModelText"),
        first(
            "ModelTypeText",
            ignorenulls=True,
        ).alias("ModelTypeText"),
        first(
            "TypeName",
            ignorenulls=True,
        ).alias("TypeName"),
        first(
            "TypeNameFull",
            ignorenulls=True,
        ).alias("TypeNameFull"),
        first(
            "entity_id",
            ignorenulls=True,
        ).alias("entity_id"),
    )

    return output_df.select(SUPPLIER_SCHEMA.names)
