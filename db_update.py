from __future__ import print_function
from time import sleep
import os.path
from gcp_secrets import *
import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from sqlalchemy import * 
from datetime import datetime
from pdfminer.high_level import extract_text
import re
import requests 
from io import BytesIO
from string import capwords
#logging.basicConfig(filename='log.txt', encoding='utf-8',filemode='a', level=logging.INFO)
# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

import pandas as pd

import tweepy
engine =create_engine('sqlite:///food-grades')
grades = []
with engine.connect() as conn:
        for row in conn.execute(text('select * from grades where spreadsheet = 0')):
            grades.append(list(row[1:-2]))    
def main():
    #get data from DB 
    c_restaurants = []
    print("beginning")
    with engine.connect() as conn:
        for row in conn.execute(text('select * from grades where tweeted = 0 AND Grade=\'C\'')):
            c_restaurants.append(row)
    c_restaurants=pd.DataFrame(c_restaurants)
    print(c_restaurants)
    client = tweepy.Client( consumer_key=consumer_key, consumer_secret=consumer_secret,access_token=access_token, access_token_secret=access_token_secret)
    for row,idx in c_restaurants.iterrows():
        sleep(1)
        inspection_date=datetime.strptime(idx.Date ,"%Y-%m-%d").strftime('%B %-d')
        resp = requests.get(idx.PDF)
        print(idx.Establishment)
        print(idx.PDF)
        if resp.status_code>=200 and resp.status_code<=299:
                pdf = resp.content
                pdf=BytesIO(pdf)
                txt=extract_text(pdf)
                #pct_loc=re.search('%',txt).span()[0]
                score = int(re.findall(r'Points:  \d+',extract_text(BytesIO(resp.content)))[0].split('  ')[1])
                #score=score[1:]
                print(f"On {inspection_date}, {idx[0].title()} in {idx[2].title()}, received a C rating and a score of {score}% from DHEC in a {idx[5].lower()} inspection. Report: {idx[6]}")
                print(len("On {inspection_date}, {capwords(idx.Establishment)} in {capwords(idx.City)}, received a C rating and a score of {score}% from DHEC in a {idx.Type.lower()} inspection. Report: {idx[6]}"))
                client.create_tweet(text=f"On {inspection_date}, {capwords(idx.Establishment)} in {capwords(idx.City)}, received a C rating and a score of {score}% from @scdhec in a {idx.Type.lower()} inspection. Report: {idx.PDF}")
        else:
            print(resp.status_code)
            print(f"On {inspection_date}, {capwords(idx.Establishment)} in {capwords(idx.City)}, received a C rating and from DHEC in a {idx.Type.lower()} inspection. Report from @scdhec is unavailable.")
            client.create_tweet(text=f"On {inspection_date}, {capwords(idx.Establishment)} in {capwords(idx.City)}, received a C rating and from DHEC in a {idx.Type.lower()} inspection. Report from @scdhec is unavailable.")

        creds = None
        with engine.connect() as conn: 
            conn.execute(text(f'UPDATE grades set tweeted = 1 where tweeted = 0 AND Grade=\'C\' AND ID = \'{idx.ID}\' '))
    #with engine.connect() as conn:
    #        conn.execute(text('UPDATE grades set tweeted = 1 where tweeted = 0 AND Grade=\'C\''))
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
        #spreadsheet_id='1jd9Sig8a6i0TzesN0FAi_KXuffiVTtPqE6f91GtqaO0'
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
