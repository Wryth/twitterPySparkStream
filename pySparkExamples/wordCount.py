from pyspark import SparkContext
from operator import add

#This only counts the lenght of the list.
def listCountDemo():
    sc = SparkContext("local", "count app")
    words = sc.parallelize(
        ["scala",
         "java",
         "hadoop",
         "spark",
         "akka",
         "spark vs hadoop",
         "pyspark",
         "pyspark and spark,"]
    )
    counts = words.count()
    print(type(counts))

    print("There are: ")
    print(counts)
    print("Words in this list.")
    sc.stop()

# This counts each word.
def wordCountDemo():
    sc = SparkContext("local", "count app")
    s = 'Hi hi hi bye bye bye word count'
    seq = s.split()  # ['Hi', 'hi', 'hi', 'bye', 'bye', 'bye', 'word', 'count']
    result = sc.parallelize(seq) \
        .map(lambda x: (x, 1)) \
        .reduceByKey(add) \
        .collect()
    print(result)
    sc.stop()
