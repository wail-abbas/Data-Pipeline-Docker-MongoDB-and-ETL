from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from sqlalchemy import create_engine
import pymongo
import time
import os
import config

DB_USER_NAME = config.DB_USER_NAME
DB_PASSWORD = config.DB_PASSWORD
s  = SentimentIntensityAnalyzer()

time.sleep(20)
client = pymongo.MongoClient('mongodb')
db = client.tweets_db

pg = create_engine(f'postgres://{DB_USER_NAME}:{DB_PASSWORD}@postgresdb:5432/postgres', echo=True)

pg.execute('''
    CREATE TABLE IF NOT EXISTS tweets (
    username VARCHAR(30),
    followers INTEGER,
    text VARCHAR(500),
    sentiment NUMERIC
);
''')

entries = db.collections.tweets.find()
for e in entries:
    print(e)
    username = e['username']
    followers = e['followers_count']
    text = e['text']
    sentiment = s.polarity_scores(e['text'])
    print(sentiment)
    score = sentiment['compound']
    query = "INSERT INTO tweets VALUES (%s, %s, %s, %s);"
    pg.execute(query, (username, followers, text, score))



