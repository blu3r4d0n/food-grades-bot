from __future__ import print_function
from time import sleep
import os.path
from secrets import *
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


import requests
import pandas as pd
from datetime import datetime,timedelta
import pytz
from bs4 import BeautifulSoup
import tweepy



#api = tweepy.API(auth)    


def main():
    #get data from DHEC
    date=datetime.now(pytz.timezone('America/New_York'))-timedelta(2)
    datefmt=date.strftime('%m-%d-%y').replace('-','%2F')
    resp=requests.get(f"https://apps.dhec.sc.gov/Environment/FoodGrades/rate.aspx?fn=&pg=&ft=&aa=&ci=&cy=&sd={datefmt}&ed={datefmt}").content
    df=pd.read_html(resp)[0]
    soup=BeautifulSoup(resp)
    links=[]
    for tr in soup.findAll("tr"):
        trs = tr.findAll("td")
        for each in trs:
            try:
                link = each.find('a')['href']
                links.append(link)
            except:
                pass
    df["PDF"]=links
    c_restaurants = df[df.Grade=="C"]
    grades=[] #store grades for appending to google sheet
    for i in df.iterrows():
        grades.append(list(i[1].array))
    client = tweepy.Client(bearer_token=bearer_token)
    for i in c_restaurants.iterrows():
        sleep(1)
        restaurant=i[1]
        client.create_tweet(f"On {date.strftime('%B %d')}, {restaurant.Establishment.title()} in {restaurant.City.title()}, recieved a C rating from DHEC in a {restaurant.Type.lower()} inspection. Report: {restaurant.PDF}")
    creds = None
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


if __name__ == '__main__':
    main()

