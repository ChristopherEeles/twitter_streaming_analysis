import tweepy
import json
import os

# load twitter credentials
with open('twitter_config.json') as config:
    configuration = json.load(config)

consumer_key = configuration.get('api_key')
consumer_secret = configuration.get('api_secret_key')

# authenticate with twitter api using OAuth2
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)

api = tweepy.API(auth)

# connect to filters