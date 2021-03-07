from pyspark.sql import SparkSession,Row

spark=SparkSession.builder.appName("dataframe-test2").getOrCreate()

df=spark.read.option("header","true").option("inferschema","true").csv("fakefriends-header.csv")
df.show()

df.printSchema()

df.select("name").show()

df.filter(df.age<21).show()

df.filter("age = 25" ).show()

df.groupBy("age").count().show()

df.select("name",(df.age+10).alias("newage")).filter("newage < 40").show()