import re
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

SPECIAL_CASES = dict({"seat": "SEAT", "delorean": "DeLorean"})

COLORS_DICTIONARY = dict(
    {
        "beige": "Beige",
        "schwarz": "Black",
        "blau": "Blue",
        "braun": "Brown",
        "gold": "Gold",
        "grau": "Gray",
        "grün": "Green",
        "orange": "Orange",
        "violett": "Purple",
        "rot": "Red",
        "silber": "Silver",
        "weiss": "White",
        "gelb": "Yellow",
    }
)


@udf(returnType=StringType())
def normalize_make(text: str) -> str:
    if not text:
        return text
    if len(text) < 4:
        return text.upper()
    if text.lower() in SPECIAL_CASES:
        return SPECIAL_CASES[text.lower()]
    words = re.split(r"(\W)", text)
    return "".join([item.capitalize() for item in words])


# I'm assuming the "metalized" variants are the base color
@udf(returnType=StringType())
def normalize_color(color: str) -> str:
    if not color:
        return color
    if color.split()[0] in COLORS_DICTIONARY:
        return COLORS_DICTIONARY[color.split()[0]]
    return "Other"


def normalize_df(df: DataFrame, attributes: List[str]) -> DataFrame:
    return df.withColumn(attributes[0], normalize_make(df[attributes[0]])).withColumn(
        attributes[1], normalize_color(df[attributes[1]])
    )
