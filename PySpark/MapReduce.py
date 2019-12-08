from pyspark import SparkContext, SparkConf
conf = SparkConf().setMaster("local[2]").setAppName("Spark Count")
spark = SparkContext(conf=conf)
file_names = spark.textFile("hdfs://...")
file_rel = spark.textFile("hdfs//...")

file_names = file_names.flatMap(lambda line: line.split(","))
file_rel = file_rel.flatMap(lambda line: line.split(","))

context = file_names.map(lambda id: (id.split(",")[0], "name" + "   " + id.split(",")[1]))

context = file_names.map(lambda id: (id.split(",")[0], "rel" + "   " + id.split(",")[1]))

context=context.reduceByKey(lambda a, b: a + b)