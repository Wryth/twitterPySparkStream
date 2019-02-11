from pyspark import *
from pyspark.sql import SQLContext

pdf.head()
# Create a Spark DataFrame from a Pandas DataFrame
# create Spark context and sql context.
sc = SparkContext()
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(pdf)

# use latitude and longitide as x and y features with k-means
from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler(inputCols=["lat", "long"], outputCol="features")
new_df = vecAssembler.transform(df)

# run K-means over longitude and latitude, with k amount of target clusters.
from pyspark.ml.clustering import KMeans

kmeans = KMeans(k=16, seed=1)  # 16 clusters
model = kmeans.fit(new_df.select('features'))

# show tweets with their associated cluster("prediction")
transformed = model.transform(new_df)
transformed.select("tweet_id", "prediction", "lat", "long", "tweet_text").show()

# Shows the cluster centers. ("Centroids")
centers = model.clusterCenters()
print("Cluster Centers: ")
for idx, center in enumerate(centers):
    print(idx, center)

# Combines all tweets under its associated k-means cluster class.
import pyspark.sql.functions as F
from pyspark.sql import functions as F
documents = transformed.groupBy('prediction')\
  .agg(F.collect_list('tweet_text').alias("cluster_text")).orderBy("prediction")

# Combine each list of tweets to a string.
documents = documents.withColumn('cluster_text', F.concat_ws(',', 'cluster_text'))

# Tokenize
from pyspark.ml.feature import Tokenizer, RegexTokenizer
from pyspark.sql.functions import *
from pyspark.sql.types import *
tokenizer = Tokenizer(inputCol="cluster_text", outputCol="words")

countTokens = udf(lambda words: len(words), IntegerType())
tokenized = tokenizer.transform(documents)

# StopWordsRemover
from pyspark.ml.feature import StopWordsRemover
remover = StopWordsRemover(inputCol="words", outputCol="filtered")

# Add stopwords to the existing list.
add_stopwords = ["like", "-", "the", "to", "@", "get", "got", "i´m", "don´t"]
newStopwords = remover.getStopWords() + add_stopwords
remover.setStopWords(newStopwords)
remover.getStopWords()

# transform twitter text by removing stopwords
tokenized = remover.transform(tokenized)

# Explode and aggregate words
tokenized = tokenized.withColumn("word", F.explode('filtered'))

# Add counter column with 1 initiated.
tokenized = tokenized.withColumn("count", F.lit(1))

# Count words under the same prediction(cluster) group.
countedWords = tokenized.groupBy("prediction", "word")\
  .agg(F.count("count").alias("wordCount"))#.orderBy("wordCount", ascending=False).show()

# Create column containing the top 3 words of each cluster.
maxWord = countedWords.groupBy("prediction")\
    .agg(F.max("wordCount").alias("maxCount"))
maxWord = maxWord.withColumnRenamed("prediction", "cluster")

# each cluster with its most used word and the times it is used.
clusterMaxWord = countedWords.crossJoin(maxWord).filter((countedWords.wordCount == maxWord.maxCount))\
    .filter((countedWords.prediction == maxWord.cluster)).select("cluster", "word", "maxCount") \
    .orderBy("cluster", ascending=True)

# Add each cluster Top word to each row.
result_sdf = transformed.crossJoin(clusterMaxWord)\
    .where(transformed.prediction == clusterMaxWord.cluster).drop("cluster")

# Convert the Spark DataFrame back to a Pandas DataFrame.
result_pdf = result_sdf.select("*").toPandas()
result_pdf.head(5)

# sc.stop()
