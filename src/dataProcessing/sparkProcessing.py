from pyspark import *
from pyspark.sql import SQLContext
import json

# if true Enable Arrow-based columnar data transfers for speed.
#spark.conf.set("spark.sql.execution.arrow.enabled", "false")

# create Spark context and sql context.
sc = SparkContext()
sqlContext = SQLContext(sc)

import numpy as np
import pandas as pd
from pandas.io.json import json_normalize #package for flattening json in pandas df

# load json object testData,
#with open("data/testData.json", encoding="utf-8") as f:
#    d = json.load(f)

# load this json file if you ran TwitterStream.py first.
with open("data/tweets/json_data.json", encoding="utf-8") as f:
    d = json.load(f)

# Creates a dataframe; each row is a tweet, each column is a tweet attribute + tweet ID
pdf = json_normalize(data=d['tweets'])
pdf.head(5)

#_____________Pandas Text Cleaning__________________
# Convert to lowercase
pdf['tweet_text'] = pdf['tweet_text'].apply(lambda x: " ".join(x.lower() for x in x.split()))
pdf['tweet_text'].head()

rgx = '[.,]'  # remove , or .
pdf['tweet_text'] = pdf['tweet_text'].str.replace(rgx, '')
pdf['tweet_text'].head()

# Create count numbers of hastags used in each tweet.
pdf['hastags'] = pdf['tweet_text'].apply(lambda x: len([x for x in x.split() if x.startswith('#')]))
pdf[['tweet_text','hastags']].head()
pdf['hastags'].max()
pdf["hastags"].sum()

# Filter by keyword, we can have it search many keywords such as [roadblocking, landslide, poweroutage...]
#Which ever we find most suitable.
keyword = "help"
pdf['keyword'] = pdf['tweet_text'].apply(lambda x: len([x for x in x.split() if x.startswith(keyword)]))
pdf[['keyword']].max()

# Split latitude and longitude.
pdf["lat"] = pdf["tweet_location"].apply(lambda x: x[0])
pdf["long"] = pdf["tweet_location"].apply(lambda x: x[1])
pdf

# Remove tweet that does not cointain any of the keywords.
#keywordDf = pdf[(pdf['keyword'] >= 1)]
#keywordDf.head()

# Create a Spark DataFrame from a Pandas DataFrame
df = sqlContext.createDataFrame(pdf)

df

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
with open('data/processed/testDataCluster.json', 'w') as f:
    f.write(result_pdf.to_json(orient='records'))

#sc.stop()

# add "var tweets = " to json file output.
def add_firstline(inputfile, line):
    with open(inputfile, 'r+') as f:
        content = f.read()
        f.seek(0, 0)
        f.write(line.rstrip('\r\n') + '\n' + content)


add_firstline('data/processed/testDataCluster.json', "var tweets = ")

