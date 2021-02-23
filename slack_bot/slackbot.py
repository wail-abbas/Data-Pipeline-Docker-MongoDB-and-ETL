from sqlalchemy import create_engine
import pymongo
import time
import requests
import os
import config

DB_USER_NAME = config.DB_USER_NAME
DB_PASSWORD = config.DB_PASSWORD
WEBHOOK_URL = config.WEBHOOK_URL

pg = create_engine(f'postgres://{DB_USER_NAME}:{DB_PASSWORD}@postgresdb:5432/postgres', echo=True)

data = """SELECT username, text, followers, sentiment from tweets;"""
result = pg.execute(data).fetchall()

for row in result:
    data = {'text': f"""
@{row[0]}, ({row[2]} followers):
{row[1]}
The sentiment score is: {row[3]}
-----------------------
    """}
    requests.post(url=WEBHOOK_URL, json = data)
    time.sleep(15)
