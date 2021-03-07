from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("word-count-test")
sc = SparkContext(conf=conf)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

line=sc.textFile("file:///SparkCourse/book.txt")
words=line.flatMap(normalizeWords)
wordcount=words.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)
print(wordcount.collect())

wordcount_sort=wordcount.map(lambda x:(x[1],x[0])).sortByKey(ascending=False).map(lambda x:(x[1],x[0]))
print(wordcount_sort.collect())

 