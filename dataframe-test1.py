from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("dataframe-test").getOrCreate()

def test(line):
    fields=line.split(",")
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

rdd=spark.sparkContext.textFile("fakefriends.csv")
rdd1=rdd.map(test)

dataframe1=spark.createDataFrame(rdd1)
dataframe1.createOrReplaceTempView("testview")

dataframe2=spark.sql("select * from testview where age<26")
dataframe3=dataframe2.collect()

for i in dataframe3:
    print(i)

dataframe4=dataframe1.groupBy("age").count().orderBy("age").show()
dataframe5=dataframe2.filter(dataframe2.age<25).show()
