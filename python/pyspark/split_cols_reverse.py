"""
This shows how to split a string on some delimiter in reverse for a set
number of columns

Examples for three columns
'foo-bar-baz-lol' -> |foo-bar|baz|lol|
'foo-bar-baz' -> |foo|bar|baz|
'foo-bar' -> |null|foo|bar|
'foo' -> |null|null|foo|

If you want to split from the front with a max number of columns, just
use the split function with the limit parameter
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, reverse, concat_ws, col, when, size


DELIMITER = " - "
MAXCOLS = 2

# If you're working in fabric, you don't need to define the spark session, it's just magically there.
spark = SparkSession.builder.getOrCreate()

df = spark.read.options(header=True).csv("filename.csv")
df = df.withColumn("colname_split", reverse(split(col("colname"), DELIMITER)))

for i in range(MAXCOLS):
    if i != MAXCOLS - 1:
        df = df.withColumn(f"colname_{MAXCOLS - i}", col("colname_split").getItem(i))
    else:
        df = df.withColumn(
            "colname_1",
            when(
                size(col("colname_split")) <= MAXCOLS,
                col("colname_split").getItem(i),
            ).otherwise(
                concat_ws(
                    DELIMITER,
                    reverse(
                        split(
                            reverse(
                                split(
                                    concat_ws(DELIMITER, col("colname_split")),
                                    DELIMITER,
                                    MAXCOLS,
                                )
                            ).getItem(0),
                            DELIMITER,
                        )
                    ),
                )
            ),
        )


df = df.drop("colname_split")
df = df.select(*["colname"] + [f"colname_{i + 1}" for i in range(MAXCOLS)])
df.show(truncate=False)
