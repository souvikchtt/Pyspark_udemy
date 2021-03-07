from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField,IntegerType,StringType

spark=SparkSession.builder.appName("most-popular-superhero-df-test").getOrCreate()

schema=StructType([StructField ("id",IntegerType(),True ),StructField("name",StringType(),True)])

movie_names_df=spark.read.option("sep"," ").schema(schema).csv("Marvel-names.txt")
print(type(movie_names_df))
movie_names_df.show()

movie_id_df1=spark.read.text("Marvel-graph.txt")
movie_id_df1.show()

movie_id_df2=movie_id_df1.withColumn("id",f.split(f.col("value")," ")[0])\
    .withColumn("num_appearence",f.size(f.split(f.col("value")," "))-1).select("id","num_appearence")

movie_id_df2.show()

movie_id_df3=movie_id_df2.groupBy("id").agg(f.sum("num_appearence").alias("appearence_count"))

movie_id_df3.show()
print("movie_id_cnt_desc")
movie_id_df4=movie_id_df3.sort(f.col("appearence_count").desc())

print(movie_id_df4)
print(type(movie_id_df4))
#result=movie_names_df.filter(f.col("id") == movie_id_df4[0]).select("name").collect()

#print(result)
join_result=movie_names_df.join(movie_id_df4,on="id", how="inner").sort(f.col("appearence_count").desc()).first()

print("The most popular superhero is "+join_result[1]+" having id: "+str(join_result[0])+" and appear count is "+str(join_result[2]))


