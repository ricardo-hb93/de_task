from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def integrate(input_df: DataFrame) -> DataFrame:
    if len(input_df.head(1)) == 0:
        return input_df

    return input_df.select(
        col("BodyColorText").alias("color"),
        col("MakeText").alias("make"),
        col("ModelText").alias("model"),
        col("ModelTypeText").alias("model_variant"),
        col("City").alias("city"),
    )
