import tweepy
import json
import os
import sys

# ---- Connect to Kafka Server
from pykafka import KafkaClient

# Connect to the kafka server running on Amazon EC2 virtual machine
client = KafkaClient(hosts='18.221.88.167:9092')

# load twitter credentials
with open('twitter_config.json') as config:
    configuration = json.load(config)

consumer_key = configuration.get('api_key')
consumer_secret = configuration.get('api_secret_key')
access_token = configuration.get('access_token')
access_secret = configuration.get('access_token_secret')

# authenticate with twitter api using OAuth1a
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

assert api.verify_credentials(), "Authentication of credentials failed!" 

# setup twitter stream object to capture tweets
class StreamListener(tweepy.StreamListener):
    def on_status(self, status):
        print(status.id_str)
        # if "retweeted_status" attribute exists, flag this tweet as a retweet.
        is_retweet = hasattr(status, "retweeted_status")

        # check if text has been truncated
        if hasattr(status, "extended_tweet"):
            text = status.extended_tweet["full_text"]
        else:
            text = status.text

        # check if this is a quote tweet.
        is_quote = hasattr(status, "quoted_status")
        quoted_text = ""
        if is_quote:
            # check if quoted tweet's text has been truncated before recording it
            if hasattr(status.quoted_status,"extended_tweet"):
                quoted_text = status.quoted_status.extended_tweet["full_text"]
            else:
                quoted_text = status.quoted_status.text

        with open("out.csv", "a", encoding='utf-8') as f:
            f.write("%s,%s,%s,%s,%s,%s\n" % (status.created_at,status.user.screen_name,is_retweet,is_quote,text,quoted_text))

    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")



myStreamListener = StreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener, tweet_mode='extended')

# Track tweets that mention the AstraZeneca vaccine for sentiment analsis
myStream.filter(track=['AstraZeneca vaccine', 'Oxford vaccine', 'AZD1222'], languages=['en'])