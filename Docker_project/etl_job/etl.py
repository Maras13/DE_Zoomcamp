'''
1. EXTRACT the tweets from mongodb
- connect to the database 
- query the data
2. TRANSFORM the data
- clean the text before?
- sentiment analysis
- maybe transform data types?
3. LOAD the data into postgres
- connect to postgres 
- insert into postgres
'''
import time
from pymongo import MongoClient
from sqlalchemy import create_engine
import psycopg2
import pymongo
import logging
import re


### create connections to databases (check your mongosb and postgres in python notebooks (or luftdaten))
# Establish a connection to the MongoDB server
time.sleep(10) 
client = pymongo.MongoClient(host="mongodb", port=27017)

USERNAME_PG = 'postgres'
PASSWORD_PG = 'postgres'
HOST_PG = 'mypostgres' # if in docker it would be the container name
PORT_PG = 5432
DATABASE_NAME_PG = 'twitter'


# Connection string
conn_string_pg = f"postgresql://{USERNAME_PG}:{PASSWORD_PG}@{HOST_PG}:{PORT_PG}/{DATABASE_NAME_PG}" 
pg = create_engine(conn_string_pg)



# Select the database you want to use withing the MongoDB server
db = client.twitter


pg.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    text VARCHAR(500),
    sentiment NUMERIC
);
''')
## Great suggestion: work step by step in each function with a limited amount of data e.g. 5 tweets, hence the .find(limit = 5) to test

def extract():
    ''' Extracts tweets from mongodb'''
    # always take all and deal with duplicates in the transform?
    # .find(limit = 5) and ADVANCED: if you want to filter .find({condition})
    extracted_tweets = list(db.tweets.find())
    return extracted_tweets

def clean(extracted_tweets):
    '''Cleans tweets'''
    extracted_tweets= re.sub('@[A-Za-z0-9]+', '', extracted_tweets)  #removes @mentions
    extracted_tweets = re.sub('#', '', extracted_tweets) #removes hashtag symbol
    extracted_tweets= re.sub('RT\s', '', extracted_tweets) #removes RT to announce retweet
    extracted_tweets = re.sub('https?:\/\/\S+', '', extracted_tweets) #removes most URLs
    
    return extracted_tweets

def transform(exctracted_tweets):
    ''' Transforms data: clean text, gets sentiment analysis from text, formats date '''
    ## sentiment analysis tomorrow, basically you pass text and get a number between 0-1 as the sentiment score
    ## add the sentiment to the tweet and store in a dataframe or a dictionary
    transformed_tweets = []
    for tweet in extracted_tweets:
        sentiment = 1 # later on you will calculate a sentiment
        # datatype of the tweet: dictionary
        tweet['sentiment'] = sentiment # adding a key: value pair with 'sentiment' as the key and the score as the value
        transformed_tweets.append(tweet)
        # transformed_tweets is a list of transformed dictionaries

    return transformed_tweets




def load(transformed_tweets):
    ''' Load final data into postgres'''
    ## example function to load
    for tweet in transformed_tweets:
        insert_query = "INSERT INTO tweets VALUES (%s, %s, %s)"
        pg.execute(insert_query, tweet['text'], tweet['sentiment'])
        logging.critical('---Inserted a new tweet into postgres---')
        logging.critical(tweet)




