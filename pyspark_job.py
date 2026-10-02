from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_data(df: DataFrame) -> DataFrame:

    return (
        df.filter(F.col("amount") > 0)
        .filter(F.col("name").isNotNull())
        .withColumn("amount_with_tax", F.col("amount") * 1.20)
    )