import numpy as np
import pandas as pd
import json

# if true Enable Arrow-based columnar data transfers for speed.
#spark.conf.set("spark.sql.execution.arrow.enabled", "false")

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
