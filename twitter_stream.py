#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#Variables that contains the user credentials to access Twitter API
access_token = "467470574-fwdfwPrR7xY4jZyiBR8wBEoXcKsEPRlml7jov8XH"
access_token_secret = "3XM6g8QJEKobvONLBOW8FFJvYnfrUqDCINeHqIDMk"
consumer_key = "U6sR2gmXylI5RX8C2z7PA"
consumer_secret = "7IeP4vZ5Vd2GfnuaRPLvatNhIwrbEQDI9GB5yrME"


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
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.sample()
