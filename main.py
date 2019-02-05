from apiConfigs import twitterConfigs
#from pySparkExamples import wordCount
#from pySparkExamples.twitterStreamDemo.laurentSparkTwitterDemo import sparkDemo, tweetRead
import tweepy

#https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/connecting.html
#https://www.linkedin.com/pulse/apache-spark-streaming-twitter-python-laurent-weichberger

consumer_key = twitterConfigs.apiKey
consumer_secret = twitterConfigs.secretKey
access_token = twitterConfigs.token
access_token_secret = twitterConfigs.secretToken

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

public_tweets = api.home_timeline()
for tweet in public_tweets:
    print(tweet.text)

user = api.get_user('twitter')

# Starts script
#if __name__ == "__main__":
#    "derp"
