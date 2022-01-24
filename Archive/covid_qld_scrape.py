#%%
import requests 
from bs4 import BeautifulSoup as bs
import time
import pandas as pd

# r = requests.get('https://covidlive.com.au/report/cases-by-lga/qld')

# soup = bs(r.text, 'html.parser')
# tds = soup.find_all(class_='COL1 LGA')

# lgas = [x.a for x in tds]

# # lgas=[f'https://covidlive.com.au{x}' for x in lgas if "qld" in x]


# listo = []

# for lga in tds:
#     if lga.text != 'LGA':
#         print(lga.text)
#         urlo = 'https://covidlive.com.au' + lga.a['href']
#         r2 = requests.get(urlo)

#         tablo = pd.read_html(r2.text)[1]
#         tablo = tablo[['DATE', 'CASES']]
#         tablo['DATE'] = pd.to_datetime(tablo['DATE'].add(' 2021'), format="%d %b %Y")
#         tablo['DATE'] = tablo['DATE'].dt.strftime("%Y-%m-%d")
#         tablo['LGA'] = lga.text

#         tablo = tablo.loc[tablo['DATE'] >= "2021-12-20"]

#         p = tablo
#         # print(p)

#         listo.append(tablo)
#         # print(p.columns)
#         # print(tablo)

# old = pd.concat(listo)
# # print(lgas)

# with open('Archive/Covidlive_QLD_LGA_Archive.csv', 'w') as f:
#     old.to_csv(f, index=False, header=True)

old = pd.read_csv('/Users/josh_nicholas/github/oz-small-charts/oz-220105-covid-cases-lgas-maps/Archive/Covidlive_QLD_LGA_Archive.csv')
exclude = ['Not Supplied', 'Interstate/other']
old = old.loc[~old['LGA'].isin(exclude)]

#%%

qld_pops = pd.read_excel('32180DS0002_2019-20.xls',
sheet_name='Table 3', skiprows=7)

qld_off = qld_pops[['Local Government Area']]
qld_off.columns = ['LGA']
qld_off.dropna(subset=['LGA'], inplace=True)

qld_off = qld_off.loc[~qld_off['LGA'].str.contains("TOTAL")]

# off = qld_off['Local Government Area'].unique().tolist()

# print(qld_off)

# p = qld_off

from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 
def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    """
    :param df_1: the left table to join
    :param df_2: the right table to join
    :param key1: key column of the left table
    :param key2: key column of the right table
    :param threshold: how close the matches should be to return a match, based on Levenshtein distance
    :param limit: the amount of matches that will get returned, these are sorted high to low
    :return: dataframe with boths keys and matches
    """
    s = df_2[key2].tolist()
    
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
    df_1['matches'] = m
    
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    
    return df_1

latest_m = fuzzy_merge(old, qld_off, "LGA", "LGA", threshold=90, limit=1)



combo = pd.merge(latest_m, qld_off, left_on='matches', right_on='LGA', how='left')


# %%

fin = combo.copy()

fin = fin[['DATE', 'CASES', 'matches']]

fin.columns = ['Date', 'Total cases', 'LGA']

fin = fin.loc[(fin['Date'] >= "2021-12-20") & (fin['Date']< "2021-12-24")]

with open('/Users/josh_nicholas/github/oz-small-charts/oz-220105-covid-cases-lgas-maps/qld_lga_data/covidlive_20_24_dec.csv', 'w') as f:
    fin.to_csv(f, index=False, header=True)

testo = fin.copy()

# testo = testo.loc[testo['LGA_y'].isna()]

p = testo
print(p)
print(p.columns)
# print(p['LGA_x'].unique().tolist())
# print(p['CASES'].unique().tolist())

# # %%
