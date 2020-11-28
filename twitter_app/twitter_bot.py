import tweepy
import re
import os

# ---- 1. Connect to Kafka Server
from pykafka import KafkaClient

# Connect to the kafka server running on Amazon EC2 virtual machine
client = KafkaClient(hosts='18.221.88.167:9092', zookeeper_hosts='18.221.88.167:2181')

# Configure procuder for my topic
topic = client.topics['azn_vaccine']
producer = topic.get_producer()

# ----- 2. Connect to Twitter

# load twitter credentials
consumer_key = os.environ['api_key']
consumer_secret = os.environ['api_secret_key']
access_token = os.environ['access_token']
access_secret = os.environ['access_token_secret']

# authenticate with twitter api using OAuth1a
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

# confirm authentication worked
assert api.verify_credentials(), "Authentication of credentials failed!" 

# ---- 3. Setup Twitter Stream Producer

# setup twitter stream object to capture tweets
class StreamListener(tweepy.StreamListener):
    # Handle incoming tweets
    def on_status(self, status):
        print(status.id_str)
        
        # Determine if this is a retweet
        is_retweet = hasattr(status, "retweeted_status")

        # Check if the tweet has been truncated
        is_fulltext = hasattr(status, "extended_tweet")

        if is_fulltext:
            text = status.extended_tweet["full_text"]
        else:
            text = status.text

        # check if this is a quote tweet
        is_quote = hasattr(status, "quoted_status")
        quoted_text = ""
        if is_quote:
            # check if quoted tweet's text has been truncated before recording it
            if hasattr(status.quoted_status,"extended_tweet"):
                quoted_text = status.quoted_status.extended_tweet["full_text"]
            else:
                quoted_text = status.quoted_status.text

        # Remove any quote characters to prevent breakings the json file
        text = re.sub("'", "", re.sub('"', '', text))
        quoted_text = re.sub("'", "", re.sub('"', '', quoted_text))

        # Assemble the tweet data into a JSON string
        tweet = f'{{datetime: {status.created_at}, user: {status.user.screen_name}, retweet: {is_retweet}, quote: {is_quote}, text: "{text}", quote_text: "{quoted_text}"}}'
        print(tweet)
        producer.produce(tweet.encode())
    
    # Handle errors
    def on_error(self, status_code):
        print("Encountered streaming error (", status_code, ")")

# ---- 4. Start Twitter Stream Producer

myStreamListener = StreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener, tweet_mode='extended')

# Track tweets that mention the AstraZeneca vaccine for sentiment analsis
myStream.filter(track=['AstraZeneca vaccine', 'Oxford vaccine', 'AZN vaccine', 'AZD1222'], languages=['en'], is_async=True)