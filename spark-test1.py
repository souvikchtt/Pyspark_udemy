from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("test1")
sc = SparkContext(conf = conf)

rdd=sc.parallelize([2,4,6])
print(rdd.count())
rdd1=rdd.map(lambda x:x+5)
print(rdd1.collect())
rdd2=rdd.flatMap(lambda x:x+5)
for i in rdd2.collect():
    print(i)