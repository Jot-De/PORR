from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster("local[2]").setAppName("Spark Count")
spark = SparkContext(conf=conf)
file_names = spark.textFile("hdfs:///friends/names.txt")
file_rel = spark.textFile("hdfs///friends/relations.txt")

file_names = file_names.flatMap(lambda line: line.split(","))
file_rel = file_rel.flatMap(lambda line: line.split(","))

rdd_names = file_names.map(lambda id_: (id_.split(",")[0], "name" + "   " + id_.split(",")[1]))
rdd_rel = file_names.map(lambda id_: (id_.split(",")[0], "rel" + "   " + id_.split(",")[1]))
rdd = rdd_names.join(rdd_rel)
context = rdd.reduceByKey(lambda a, b: a + b)
context.saveAstTextFile("hdfs///friends/output/spark.txt")
