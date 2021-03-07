from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("movie-rating-test")
sc = SparkContext(conf=conf)

file=sc.textFile("file:///SparkCourse/ml-100k/u.data")
rating=file.map(lambda x:x.split()[2])
result=rating.countByValue()

print(result)
