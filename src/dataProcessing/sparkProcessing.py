from pyspark import *
from pyspark.sql import SQLContext

pdf.head()
# Create a Spark DataFrame from a Pandas DataFrame
# create Spark context and sql context.
sc = SparkContext()
sqlContext = SQLContext(sc)
df = sqlContext.createDataFrame(pdf)

df.select("tweet_id","tweet_date","lat","long","tweet_text").show()

# use latitude and longitide as x and y features with k-means
from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler(inputCols=["lat", "long"], outputCol="features")
new_df = vecAssembler.transform(df)
new_df.select("tweet_id","features","tweet_text").show()

# run K-means over longitude and latitude, with k amount of target clusters.
from pyspark.ml.clustering import KMeans

kmeans = KMeans(k=16, seed=1) # 16 clusters
model = kmeans.fit(new_df.select('features'))

# show tweets with their associated cluster("prediction")
transformed = model.transform(new_df)
transformed.select("tweet_id","prediction","lat","long","tweet_text").show()

# Shows the cluster centers. ("Centroids")
centers = model.clusterCenters()
print("Cluster Centers: ")
for idx,center in enumerate(centers):
    print(idx,center)

transformed.printSchema()

# Combines all tweets under its associated k-means cluster class.
import pyspark.sql.functions as F
documents = transformed.groupBy('prediction')\
  .agg(F.collect_list('tweet_text').alias("cluster_text")).orderBy("prediction")

documents.show()

documents.select("cluster_text").first()

# Combine each list of tweets to a string.
documents = documents.withColumn('cluster_text', F.concat_ws(',', 'cluster_text'))
documents.select("cluster_text").first()

# Convert the Spark DataFrame back to a Pandas DataFrame.
result_pdf = transformed.select("*").toPandas()

result_pdf.head(5)

# Write pandas dataframe to json file
with open('data/processed/clusterData.json', 'w') as f:
    f.write(result_pdf.to_json(orient='records'))

#sc.stop()

