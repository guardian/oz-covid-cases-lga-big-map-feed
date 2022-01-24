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

earliest_date = "2021-12-19"

testo = ''
# testo = '-testo'

#%%

print("Starting SA")

### GET QLD SCRAPE DATA
listo = []

sa_init = pd.read_csv('https://www.thenewseachday.com.au/static2/sa.csv', delimiter='\t')
# 'date', 'lga', 'new_cases', 'cum_cases'
sa_init.columns = ['Date', 'Local Government Area', 'New Cases', 'Total cases']

sa_init = sa_init.loc[sa_init['Date'] > "2021-12-20"]

sa_init.drop_duplicates(subset=['Local Government Area', 'Date'], inplace=True)

sa_init = sa_init.sort_values(by='Date', ascending=True)

sa_init['Date'] = pd.to_datetime(sa_init['Date'], format="%Y-%m-%d")

exclude = ['INTERSTATE / OVERSEAS',]
sa_init = sa_init.loc[~sa_init['Local Government Area'].isin(exclude)]

sa_init['Local Government Area'] = sa_init['Local Government Area'].str.title()

sa_init.loc[sa_init['Local Government Area'] == 'Berri Barmera', 'Local Government Area'] = 'Berri and Barmera (DC)'

#%%

### SA pop
print("Adding SA populations")

sa_pops = pd.read_excel('32180DS0002_2019-20.xls',
sheet_name='Table 4', skiprows=7)

sa_pops = sa_pops[['Local Government Area', 'no..1']]
sa_pops.columns = ['LGA', 'Population']

sa_pops.dropna(subset=['LGA'], inplace=True)
sa_pops = sa_pops.loc[~sa_pops['LGA'].str.contains("TOTAL")]

print("SA Fuzzied")

latest_m = fuzzy_merge(sa_init, sa_pops, "Local Government Area", "LGA", threshold=90, limit=1)

sa_init = pd.merge(latest_m, sa_pops, left_on='matches', right_on='LGA', how='left')

sa_init.dropna(subset=['Population'], inplace=True)
# sa_init = pd.merge(sa_init, sa_pops, on='LGA', how='left')


sa_init = sa_init[['Date',  'Total cases', 'LGA', 'Population']]


#%%
### PROCESS SA DATA

print("Processing SA")

def process_agg(frame, state):

    earliest_date = frame['Date'].min()
    earliest_date = earliest_date.strftime('%Y-%m-%d')

    # print(earliest_date)

    latest_date = frame['Date'].max()
    latest_date = latest_date.strftime('%Y-%m-%d')

    copy = frame.copy()
    zlisto = []

    for lga in copy['LGA'].unique().tolist():
        try:

            inter2 = copy.loc[copy['LGA'] == lga].copy()
            inter2.set_index("Date", inplace=True)

            daily=pd.date_range(earliest_date, latest_date, freq='D')
            inter2 = inter2.reindex(daily, method='ffill')

            inter2 = inter2.reset_index()
            inter2.rename(columns={'index':"Date"}, inplace=True)

            inter2['New cases'] = inter2['Total cases'].diff()

            inter2['7 day sum'] = inter2['New cases'].rolling(window=7).sum()

            latest_cases = inter2.iloc[-1]['Total cases']
            # print(latest_cases)
            # print("Here")
            # second_latest_cases = inter2.iloc[-2]['Total cases']
            # if latest_cases == second_latest_cases:
            #     inter2 = inter2.iloc[:-1].copy()

            seven_ago = inter2.loc[inter2['Date'] == seven_ago_date].copy()
            # print(seven_ago)

            latest = inter2.loc[inter2['Date'] == inter2['Date'].max()].copy()
            # print("Here")
            latest_cases = latest['New cases'].values[0]
            latest_total = latest['Total cases'].values[0]
            latest_sum = latest['7 day sum'].values[0]
            latest_date = latest['Date'].values[0]

            population = latest['Population'].values[0]

            Per_1k = round((latest_cases / population)*1000,2)
            Weekly_1k = round((latest_sum / population)*1000,2)
            Total_1k = round((latest_total / population)*1000,2)

            seven_ago_total_cases = seven_ago['Total cases']
            seven_ago_sum = seven_ago['7 day sum'].values[0]

            to_use =    [{"LGA":lga,
                        "Date":latest_date,
                        "New_cases":latest_cases,
                        "New_per_1k": Per_1k,
                        "Weekly_per_1k":Weekly_1k,
                        "Total_per_1k":Total_1k,
                        "Population": population,
                        "Total_cases":latest_total,
                        "Weekly_cases":latest_sum,
                        "Previous_week":seven_ago_sum,
                        "Weekly_change":latest_sum-seven_ago_sum}]

            fin = pd.DataFrame.from_records(to_use)

            zlisto.append(fin)
        except Exception as e:
            print(f"Problem with {lga}: {e}")
            # break


    fin = pd.concat(zlisto)
    fin['State'] = state

    return fin

sa = process_agg(sa_init, "SA")

print(sa)