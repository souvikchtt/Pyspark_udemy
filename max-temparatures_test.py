from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("maxtemp")
sc = SparkContext(conf=conf)

def fetch_line(line):
    cols=line.split(",")
    stationid=cols[0]
    type=cols[2]
    temparature=float(cols[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationid,type,temparature)

line = sc.textFile("file:///SparkCourse/1800.csv")
rdd1=line.map(fetch_line)
print(rdd1.collect())
rdd2=rdd1.filter(lambda x:"TMAX" in x[1])
print(rdd2.collect())
rdd3=rdd2.map(lambda x:(x[0],x[2]))
print(rdd3.collect())
rdd4=rdd3.reduceByKey(lambda x,y:max(x,y))
print(rdd4.collect())
