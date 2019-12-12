import sys
import shutil
from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("Spark Count")
spark = SparkContext(conf=conf)

#example names "hdfs:///friends/names_short.csv"
file_names = spark.textFile(sys.argv[1])
#example relations: "hdfs:///friends/relations_short.csv"
file_rel = spark.textFile(sys.argv[2])

rdd_names = file_names.map(lambda line: line.split(","))
rdd_rel = file_rel.flatMap(lambda line: line.split(",")).map(lambda id_: (id_, 1)).reduceByKey(lambda x, y: x + y)
rdd = rdd_names.join(rdd_rel).map(lambda x: x[1])

#example output: "hdfs:///friends/output/spark"
try:
    shutil.rmtree(sys.argv[3], ignore_errors=True)
except:
    pass
rdd.saveAsTextFile(sys.argv[3])
