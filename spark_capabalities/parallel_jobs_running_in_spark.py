from pyspark.sql import SparkSession
import threading

spark = SparkSession \
    .builder \
    .appName("Parallel Jobs Execution") \
    .master("local[*]") \
    .getOrCreate()

def run_job(args):
    rdd = spark.sparkContext.parallelize(range(0,10000000000))
    rdd = rdd.map(lambda x: {"id":x * args})
    rdd.toDF().write.csv("data_"+str(args),mode="overwrite")

for i in range(10):
    x = threading.Thread(target=run_job,args=(i,))
    x.start()

