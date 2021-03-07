from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func

spark=SparkSession.builder.appName("friends_by_age_df").getOrCreate()

df=spark.read.option("header","true").option("inferschema","true").csv("fakefriends-header.csv")
df.show()
df.printSchema()

#One way

df.registerTempTable("test_table")
df1=spark.sql("select age,avg(friends) from test_table group by age")

for i in df1.collect():
    print(i)


# 2nd way

df2=df.select("age","friends").groupBy("age").avg("friends").sort("age").show()
df3=df.select("age","friends").groupBy("age").agg(func.round(func.avg("friends"),2).alias("avg_frnds")).sort("age").show(35)
