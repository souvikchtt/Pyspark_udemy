from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster("local").setAppName("customer-data")
sc=SparkContext(conf=conf)

def test(line):
    fields=line.split(",")
    customerid=fields[0]
    orderprice=round(float(fields[2]),1)
    return (customerid,orderprice)

file=sc.textFile("file:///SparkCourse/customer-orders.csv")
rdd1=file.map(test)
#rdd2=rdd1.filter(lambda x:('44' in x[0]))
rdd2=rdd1.reduceByKey(lambda x,y:x+y)
rdd3=rdd2.map(lambda x:(x[1],x[0]))
rdd4=rdd3.sortByKey(ascending=False)
print(rdd1.collect())
print(rdd2.collect())
print(rdd3.collect())

for i in rdd4.collect():
    print(i)