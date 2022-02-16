"""Twe following script is meant for being used for the TWITTER API-V2.
The least tweepy version to use is 4.01"""



import tweepy
from credentials import *
import logging
import pymongo



mongo_client = pymongo.MongoClient("mongodb")
db = mongo_client.twitter
collection = db.tweets








##### AUTHENTICATION #####
client = tweepy.Client(bearer_token=BEARER_TOKEN,consumer_key=API_KEY,consumer_secret=API_KEY_SECRET,
access_token=ACCESS_TOKEN,access_token_secret=ACCESS_TOKEN_SECRET)


if client:
    logging.critical("\nAutentication OK")
else:
    logging.critical('\nVerify your credentials')


#### LOOKUP USERS USING THEIR USERNAME

# for user_fields parameters check here https://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/user

##elon = client.get_user(username='elonmusk',user_fields=['name','id','created_at'])
#print(elon)

#print(f'the user with name {elon.data.name} and ID {elon.data.id} created its twitter account on {elon.data.created_at}')



#### LOOKUP AT USER'S TIMELINE

## elon musk's timeline
## passing elon id into the function below
# for tweets_fields parameters check herehttps://developer.twitter.com/en/docs/twitter-api/data-dictionary/object-model/tweet
#elon_tweets = client.get_users_tweets(id=elon.data.id, tweet_fields=['id','text','created_at'],max_results=5)
#print(elon_tweets.data)
#for tweet in elon_tweets.data:
    #print(f"the user {elon.data.name} at {tweet.created_at} wrote:{tweet.text}\n")



#### SEARCHING FOR TWEETS #####

# Defining a query search string
query = 'climate change lang:en -is:retweet'  


search_tweets = client.search_recent_tweets(query=query,tweet_fields=['id','created_at','text'], max_results=10)
#print(search_tweets)
for tweet in search_tweets.data:
    logging.critical(f'\n\n\nINCOMING TWEET:\n{tweet.text}\n\n\n')
     # create a json record and inserting it in the collections called tweets 
    record = {'text': tweet.text, 'id': tweet.id, 'created_at': tweet.created_at}
    # and inserting it in the collections called tweets 
    logging.critical(f"\n---- INSERTING A NEW tweet DOCUMENT INTO THE 'tweets' ----\n")
    db.tweets.insert_one(document=record)

    if db.tweets.find_one(record):
       logging.critical(f"\n---- tweet DOCUMENT SUCCESSFULLY INSERTED ----\n")

### Getting more than 100 tweets using Paginator 
# Check here https://docs.tweepy.org/en/stable/pagination.html

#paginator = tweepy.Paginator(client.search_recent_tweets,tweet_fields=['id','created_at','text'], query=query).flatten(limit=100)
#print(paginator)
#for tweet in paginator:
    #logging.critical(f'\n\n\nINCOMING TWEET ID {tweet.id}:\n{tweet.text}\n\n\n')
    #file = open('fetched_tweets.txt',mode='a')
    #file.write(tweet.text)
    #file.close()
