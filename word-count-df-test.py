from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark=SparkSession.builder.appName("word-count-df-test").getOrCreate()

df=spark.read.text("book.txt")

df.show()

df1=df.select(func.explode(func.split(df.value,"\\W+")).alias("word"))
df1.show()

df2=df1.filter(df1.word!="")
df2.select(func.lower(df2.word).alias("word")).show()
df3=df2.groupBy("word").count().sort("count")
df3.show(df3.count())