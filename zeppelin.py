# %spark.pyspark

from pyspark.sql.functions import json_tuple, from_json, get_json_object, col, desc

df = sqlContext.read.parquet("hdfs://spark-zeppelin-m/rsvp")
df.show(2, False)

yes_df = df.filter(col("parsed_value.response") == "yes")

agg_df = yes_df.groupBy("parsed_value.category.shortname", "parsed_value.group.group_country",
                        "parsed_value.response").count().orderBy("shortname", desc("count"))
agg_df.show(2, False)

agg_df.registerTempTable("rsvp_agg")
