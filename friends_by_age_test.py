from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("frnds_by_age_test")
sc=SparkContext(conf=conf)

def test(line):
    fields=line.split(",")
    age = int(fields[2])
    frnds_num = int(fields[3])
    return(age,frnds_num)

file=sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd1=file.map(test)
rdd2=rdd1.mapValues(lambda x:(x,1))
rdd3=rdd2.reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))
rdd4=rdd3.mapValues(lambda x:(x[0]/x[1]))
print(rdd1.collect())
print(rdd2.collect())
print(rdd3.collect())
result=rdd4.sortByKey(ascending=False).collect()
for i in result:
    print(i)
