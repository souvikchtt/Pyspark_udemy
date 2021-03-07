from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("min_temp_test")
sc=SparkContext(conf=conf)

def test(line):
    fields=line.split(",")
    stationid=fields[0]
    temptype=fields[2]
    temaparature=float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationid,temptype,temaparature)

file = sc.textFile("file:///SparkCourse/1800.csv")
rdd1=file.map(test)
rdd2=rdd1.filter(lambda x:'TMAX' in x[1])
rdd3=rdd2.map(lambda x:(x[0],x[2]))
rdd4=rdd3.reduceByKey(lambda x,y:min(x,y))
print(rdd1.collect())
print(rdd2.collect())
print(rdd3.collect())
print(rdd4.collect())