from etl.extraction import extract_value_and_unit
from etl.integration import integrate
from etl.normalization import normalize_df
from etl.preprocessing import preprocess_input
from pyspark import SparkContext
from pyspark.sql import SparkSession

spark_context = SparkContext()
spark = SparkSession.builder.getOrCreate()

input_df = spark.read.json("Data Engineering Tasks/supplier_car.json")

df_preprocessed = preprocess_input(input_df)
df_normalized = normalize_df(df_preprocessed, ["MakeText", "BodyColorText"])
df_extracted = extract_value_and_unit(df_normalized)
df_integrated = integrate(df_extracted)

df_normalized.toPandas().to_csv("de_task/output/normalized_data.csv", index=False)
df_extracted.toPandas().to_csv("de_task/output/extracted_data.csv", index=False)
df_integrated.toPandas().to_csv("de_task/output/integrated_data.csv", index=False)
df_preprocessed.toPandas().to_csv("de_task/output/preprocessed_data.csv", index=False)

spark_context.stop()
