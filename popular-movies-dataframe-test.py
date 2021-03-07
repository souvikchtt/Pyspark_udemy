from pyspark.sql import SparkSession,Row
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,LongType,StringType

spark=SparkSession.builder.appName("test").getOrCreate()

schema=StructType([StructField("usedid",IntegerType(),True),\
                   StructField("movieid",IntegerType(),True),\
                   StructField("rating",IntegerType(),True),\
                   StructField("timestamp",LongType(),True)])


df1=spark.read.option("sep","\t").schema(schema).csv("file:///SparkCourse/ml-100k/u.data")
df1.show()

df2=df1.groupBy("movieid").count().orderBy(func.desc("count"))

def test(line):
    fields=line.split("|")
    movieid=int(fields[0])
    moviename=fields[1]
    return (movieid,moviename)

schema1=StructType([StructField("movieid",IntegerType(),True),\
                   StructField("moviename",StringType(),True)])

sc=spark.sparkContext
rdd1=sc.textFile("file:///SparkCourse/ml-100k/u.item")
rdd2=rdd1.map(test)
df3=spark.createDataFrame(rdd2,schema=schema1)
df3.show()
df_join_inner=df2.join(df3,on="movieid",how="inner")
df_join_inner.show()

