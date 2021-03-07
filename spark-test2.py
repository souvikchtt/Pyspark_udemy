from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

line=sc.textFile("file:///SparkCourse/sample.txt")
rdd1=line.map(lambda x:x.split())
print(rdd1.collect())
rdd2=line.flatMap(lambda x:x.split())
print(rdd2.collect())