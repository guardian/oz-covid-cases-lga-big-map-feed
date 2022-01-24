#%%
import pandas as pd 
import os 
import datetime 
import pytz
# from modules.syncData import syncData
pd.set_option("display.max_rows", 100)
from fuzzywuzzy import fuzz 
from fuzzywuzzy import process 
def fuzzy_merge(df_1, df_2, key1, key2, threshold=90, limit=1):
    s = df_2[key2].tolist()
    m = df_1[key1].apply(lambda x: process.extract(x, s, limit=limit))    
    df_1['matches'] = m
    m2 = df_1['matches'].apply(lambda x: ', '.join([i[0] for i in x if i[1] >= threshold]))
    df_1['matches'] = m2
    
    return df_1

utc_now = pytz.utc.localize(datetime.datetime.utcnow())
bris_today = utc_now.astimezone(pytz.timezone("Australia/Brisbane"))
seven_ago_date = bris_today - datetime.timedelta(days=7)

bris_today = bris_today.strftime('%Y-%m-%d')
seven_ago_date = seven_ago_date.strftime('%Y-%m-%d')

#%%

print("Starting QLD")

### GET QLD SCRAPE DATA
listo = []

new = pd.read_csv('https://raw.githubusercontent.com/joshnicholas/oz-covid-data/main/output/qld_lgas.csv')

old = pd.read_csv('qld_lga_data/covidlive_20_24_dec.csv')

qld = new.append(old)

qld = qld.sort_values(by='Date', ascending=True)

qld['Date'] = pd.to_datetime(qld['Date'])

qld.drop_duplicates(subset=['LGA', 'Date'], inplace=True)

# qld = qld.loc[qld['Date'] == qld['Date'].max()]

qld['LGA'] = qld['LGA'].str.replace("Town", '')
qld['LGA'] = qld['LGA'].str.replace("Regional", '')
qld['LGA'] = qld['LGA'].str.replace("Shire", '')
qld['LGA'] = qld['LGA'].str.replace("City", '')
qld['LGA'] = qld['LGA'].str.replace("Aboriginal", '')

qld = qld.loc[~qld['LGA'].isin(['Interstate/Other', 'Not Supplied'])]


#%%

### QLD pop
print("Adding QLD populations")

qld_pops = pd.read_excel('32180DS0002_2019-20.xls',
sheet_name='Table 3', skiprows=7)

qld_pops = qld_pops[['Local Government Area', 'no..1']]
qld_pops.columns = ['LGA', 'Population']

qld_pops.dropna(subset=['LGA'], inplace=True)
qld_pops = qld_pops.loc[~qld_pops['LGA'].str.contains("TOTAL")]

latest_m = fuzzy_merge(qld, qld_pops, "LGA", "LGA", threshold=90, limit=1)


qld = pd.merge(latest_m, qld_pops, left_on='matches', right_on='LGA', how='left')

qld = qld[['LGA_y', 'Total cases', 'Date', 'Population']]
qld.columns = ['LGA', 'Total cases', 'Date', 'Population']

p = qld

print(p)
print(p.columns)