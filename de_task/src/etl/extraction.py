from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def extract_value_and_unit(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "extracted-value-ConsumptionTotalText",
        extract_value_from_consumptiontotaltext(df["ConsumptionTotalText"]),
    ).withColumn(
        "extracted-unit-ConsumptionTotalText",
        extract_unit_from_consumptiontotaltext(df["ConsumptionTotalText"]),
    )


@udf(returnType=StringType())
def extract_value_from_consumptiontotaltext(text: str) -> str:
    if not text:
        return text
    return text.split()[0]


@udf(returnType=StringType())
def extract_unit_from_consumptiontotaltext(text: str) -> str:
    if not text or text == "null":
        return text
    return text.split()[1]
