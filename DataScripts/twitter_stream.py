#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import logging

logger = logging.getLogger('tweet_data_crawler')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('tweet_data_crawler.log')
fh.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
logger.addHandler(fh)

access_token = "4079143643-cjGRCiPUvjpn1BXQeC1fIPLHzvhe4sDiyyVBTGb"
access_token_secret = "9wy5F7oBveW738MN2dccvkhbOxm7I4ucFRnYjAhTpCamB"
consumer_key = "ewu6uVlmrIsBon3VbmZrTzQQB"
consumer_secret = "GP7K5SIfUrSPQ110fKKVgZX590pjlst0KUIQizhKyyJoIzGldu"


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def on_data(self, data):
        print data
        return True

    def on_error(self, status):
        print status


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    l = StdOutListener()
    logger.info("Init StdOutListener")
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    try:
        stream.sample()
    except Exception as e:
        logger.error("The sampling is failed, try to restart")
