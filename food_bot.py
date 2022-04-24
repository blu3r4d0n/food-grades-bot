from __future__ import print_function
from time import sleep
import os.path
from secrets import *
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import * 
from datetime import datetime
#logging.basicConfig(filename='log.txt', encoding='utf-8',filemode='a', level=logging.INFO)
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

import pandas as pd

import tweepy
engine = engine=create_engine('sqlite:///food-grades')
grades = []
with engine.connect() as conn:
        for row in conn.execute(text('select * from grades where spreadsheet = 0')):
            grades.append(list(row[1:-2]))    
def main():
    #get data from DB 
    c_restaurants = []
    with engine.connect() as conn:
        for row in conn.execute(text('select * from grades where tweeted = 0 AND Grade=\'C\'')):
            c_restaurants.append(list(row[1:-2]))
    client = tweepy.Client( consumer_key=consumer_key, consumer_secret=consumer_secret,access_token=access_token, access_token_secret=access_token_secret)
    for i in c_restaurants:
        sleep(1)
        inspection_date=datetime.strptime(i[4],"%Y-%m-%d").strftime('%B %d')
        client.create_tweet(text=f"On {inspection_date}, {i[0].title()} in {i[2].title()}, recieved a C rating from DHEC in a {i[5].lower()} inspection. Report: {i[6]}")
        creds = None
    with engine.connect() as conn:
            conn.execute(text('UPDATE grades set tweeted = 1 where tweeted = 0 AND Grade=\'C\''))
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    try:
        service = build('sheets', 'v4', credentials=creds)
        spreadsheet_id='1AHwSYntNsAAyPvMM6PYLVvu7_ZKs11iS7Y8o6cyFW5Y'
        # Call the Sheets API
        #sheet = service.spreadsheets()
        values = grades
        body = {
            'values': values
        }
        result = service.spreadsheets().values().append(insertDataOption='INSERT_ROWS',
            spreadsheetId=spreadsheet_id, range='grades',
            valueInputOption='USER_ENTERED', body=body).execute()

    except HttpError as err:
        print(err)
    with engine.connect() as conn:
        conn.execute(text('UPDATE grades set spreadsheet = 1 where spreadsheet = 0'))



if __name__ == '__main__':
    main()

