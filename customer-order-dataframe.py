from pyspark.sql import SparkSession,Row

spark=SparkSession.builder.appName("customer-order-df").getOrCreate()

def test(line):
    fields=line.split(",")
    return Row(id=fields[0],order=float(fields[2]))

rdd=spark.sparkContext.textFile("customer-orders.csv")
rdd1=rdd.map(test)

df=spark.createDataFrame(rdd1)
df.show()
df.groupBy("id").sum("order").show()

df_tbl=df.createOrReplaceTempView("cust_table")
df_tbl1=spark.sql("select id,sum(order) from cust_table group by id").collect()

for i in df_tbl1:
    print(i)