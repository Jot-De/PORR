from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local").setAppName("Spark Count")
spark = SparkContext(conf=conf)

file_names = spark.textFile("hdfs:///friends/names_short.csv")
file_rel = spark.textFile("hdfs:///friends/relations_short.csv")

rdd_names = file_names.map(lambda line: line.split(","))
rdd_rel = file_rel.flatMap(lambda line: line.split(",")).map(lambda id_: (id_, 1)).reduceByKey(lambda x, y: x + y)
rdd = rdd_names.join(rdd_rel).map(lambda x: x[1])
rdd.saveAsTextFile("hdfs:///friends/output/spark")
