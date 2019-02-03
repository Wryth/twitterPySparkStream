# From within pyspark or send to spark-submit:

from pyspark.streaming import StreamingContext
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark instance with the above configuration
sc = SparkContext(conf=conf)
ssc = StreamingContext(sc, 5)  # 5 second batch interval

IP = "localhost"  # Replace with your stream IP
Port = 5555  # Replace with your stream port

lines = ssc.socketTextStream(IP, Port)
lines.pprint()  # Print tweets we find to the console
ssc.start()  # Start reading the stream
ssc.awaitTermination()  # Wait for the process to terminate

# TweetRead.py
# This first python script doesn’t use Spark at all:
import os
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
import socket
import json

from apiConfigs import twitterConfigs
consumer_key = twitterConfigs.apiKey
consumer_secret = twitterConfigs.secretKey
access_token = twitterConfigs.token
access_secret = twitterConfigs.secretToken


class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            print(data.split('\n'))
            self.client_socket.send(data)
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['trump'])


if __name__ == "__main__":
    s = socket.socket()  # Create a socket object
    host = "localhost"  # Get local machine name
    port = 5555  # Reserve a port for your service.
    s.bind((host, port))  # Bind to the port

    print("Listening on port: %s" % str(port))

    s.listen(5)  # Now wait for client connection.
    c, addr = s.accept()  # Establish connection with client.

    print("Received request from: " + str(addr))

    sendData(c)

#sc.stop()
