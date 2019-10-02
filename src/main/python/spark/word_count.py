from pyspark.sql import SparkSession
from operator import add
import os

if __name__ == "__main__":
  # // Initialization:
  threads = 3  # program simulates a single executor with 3 cores (one local JVM with 3 threads)
  sparksession = SparkSession.builder \
    .appName("Python WordCount") \
    .master('local[{}]'.format(threads)) \
    .getOrCreate()

  lines = sparksession.sparkContext.textFile('../../../resources/HoD.txt')
  tokens = lines.flatMap(lambda x: x.split()) \
    .map(lambda x: (x.lower(), 1))  # yields a pair of <token, 1> so tokens can be summed up in the next line
  counts = tokens.reduceByKey(add)  # same as more explicit lambda x, y: x + y

  # materializing to local disk:  coalesce is optional, without it many tiny output files are generated
  counts.coalesce(threads).saveAsTextFile('./countsplits')

  sparksession.stop()
