from sqlalchemy import * 
import requests
from datetime import datetime, timedelta
import pytz
from bs4 import BeautifulSoup
import pandas as pd

tz = pytz.timezone("America/New_York")
today = datetime.now(tz=tz).strftime('%m-%d-%y').replace('-','%2F')
weekago = (datetime.now(tz=tz) - timedelta(7)).strftime('%m-%d-%y').replace('-','%2F')
engine=create_engine('sqlite:///food-grades')
conn = engine.connect()
resp=requests.get(f"https://apps.dhec.sc.gov/Environment/FoodGrades/rate.aspx?fn=&pg=&ft=&aa=&ci=&cy=&sd={weekago}&ed={today}").content

df=pd.read_html(resp)[0]
soup=BeautifulSoup(resp,features='lxml')
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
df["ID"]=df.PDF.str.split('n/').apply(lambda x: x[1].replace('.pdf',''))
df[df.Grade.isna()]='N/A'
df["spreadsheet"] = 0
df["tweeted"]= 0 

for i in df.to_records(index=False):
    conn.execute(text(f"INSERT  OR IGNORE INTO grades(Establishment, Street, City, Grade, Date, Type, PDF, ID, spreadsheet, tweeted) VALUES{i}" ))

conn.close() # close sql connection for safety