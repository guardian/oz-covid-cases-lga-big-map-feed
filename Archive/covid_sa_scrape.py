#%%
import requests
from bs4 import BeautifulSoup as bs
import time
import pandas as pd
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 

def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    s = df_2[key2].tolist()
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
    df_1['matches'] = m
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    
    return df_1


r = requests.get('https://covidlive.com.au/report/cases-by-lga/sa')

soup = bs(r.text, 'html.parser')
tds = soup.find_all(class_='COL1 LGA')

lgas = [x.a for x in tds]

# lgas=[f'https://covidlive.com.au{x}' for x in lgas if "qld" in x]

listo = []

exclude = ['Interstate / Overseas','LGA']

for lga in tds:
    if lga.text not in exclude:
        print(lga.text)
        urlo = 'https://covidlive.com.au' + lga.a['href']
        r2 = requests.get(urlo)

        tablo = pd.read_html(r2.text)[1]
        tablo = tablo[['DATE', 'CASES']]

        # print(tablo['DATE'])

        jan = tablo.loc[tablo['DATE'].str.contains("Jan")].copy()
        jan['DATE'] = pd.to_datetime(jan['DATE'].add(' 2022'), format="%d %b %Y")
        dec = tablo.loc[tablo['DATE'].str.contains("Dec")].copy()
        dec['DATE'] = pd.to_datetime(dec['DATE'].add(' 2021'), format="%d %b %Y")

        tablo = jan.append(dec)
        # tablo['DATE'] = pd.to_datetime(tablo['DATE'].add(' 2021'), format="%d %b %Y")
        tablo['DATE'] = tablo['DATE'].dt.strftime("%Y-%m-%d")
        tablo['LGA'] = lga.text

        tablo = tablo.loc[tablo['DATE'] >= "2021-12-20"]

        p = tablo
        # print(p)

        listo.append(tablo)
        # print(p.columns)
        # print(tablo)
        time.sleep(1)

old = pd.concat(listo)
# print(lgas)



#%%

# old = pd.read_csv('Archive/Covidlive_SA_LGA_Archive.csv')
### SA pop
print("Adding SA populations")

sa_pops = pd.read_excel('32180DS0002_2019-20.xls',
sheet_name='Table 4', skiprows=7)

sa_pops = sa_pops[['Local Government Area', 'no..1']]
sa_pops.columns = ['LGA', 'Population']

sa_pops.dropna(subset=['LGA'], inplace=True)
sa_pops = sa_pops.loc[~sa_pops['LGA'].str.contains("TOTAL")]

latest_m = fuzzy_merge(old, sa_pops, "LGA", "LGA", threshold=90, limit=1)


old = pd.merge(latest_m, sa_pops, left_on='matches', right_on='LGA', how='left')

old = old[['LGA_y', 'CASES', 'DATE']]
old.columns = ['LGA', 'Total cases', 'Date']

p = old

# print(p)
# print(p.loc[p['Population'].isna()])
# print(p.columns)

with open('Archive/Covidlive_SA_LGA_Archive.csv', 'w') as f:
    old.to_csv(f, index=False, header=True)



