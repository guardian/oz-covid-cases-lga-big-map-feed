#%%
import pandas as pd
import os
import datetime
import pytz
# from modules.syncData import syncData
pd.set_option("display.max_rows", 100)

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
# import fuzzywuzzy

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

earliest_date = "2021-12-20"

testo = ''
# testo = '-testo'

#%%

# GET NSW DATA
print("Starting NSW")

# nsw_init = pd.read_csv("https://data.nsw.gov.au/data/dataset/aefcde60-3b0c-4bc0-9af1-6fe652944ec2/resource/21304414-1ff1-4243-a5d2-f52778048b29/download/confirmed_cases_table1_location.csv")
nsw_init_2 = pd.read_csv('https://data.nsw.gov.au/data/dataset/aefcde60-3b0c-4bc0-9af1-6fe652944ec2/resource/5d63b527-e2b8-4c42-ad6f-677f14433520/download/confirmed_cases_table1_location_agg.csv')

nsw_max_date = nsw_init_2['notification_date'].max()
# nsw_max_date = datetime.datetime.strptime(nsw_max_date, "%d/%m/%y")
# nsw_max_date = nsw_max_date.strftime("%Y-%m-%d")
# %%

# PROCESS DATA

print("Processing NSW")


def nsw_second_process_cumulat(frame, state, max_date):
    init = frame.copy()
    init = init.sort_values(by=['Date'], ascending=True)
    # init['Total cases'] = 1
    init = init.groupby(by=['Date', 'LGA', 'Population'])['Total cases'].sum().reset_index()

    zlisto = []
    for lga in init['LGA'].unique().tolist():
        inter3 = init.loc[init['LGA'] == lga].copy()

        inter3['Total cases'] = inter3['Total cases'].cumsum()
        inter3 = inter3.loc[inter3['Date'] >= earliest_date]

        inter3['Date'] = pd.to_datetime(inter3['Date'])

        inter3.set_index("Date", inplace=True)

        daily=pd.date_range(earliest_date, max_date, freq='D')
        inter3 = inter3.reindex(daily, method='ffill')

        inter3 = inter3.reset_index()
        inter3.rename(columns={'index':"Date"}, inplace=True)

        inter3['New cases'] = inter3['Total cases'].diff()

        inter3['7 day sum'] = inter3['New cases'].rolling(window=7).sum()

        # print(inter3.tail(20))
        # latest_cases = inter3.iloc[-1]['Total cases']
        # # print(latest_cases)
        # second_latest_cases = inter3.iloc[-2]['Total cases']
        # # print(second_latest_cases)
        # if latest_cases == second_latest_cases:
        #     inter3 = inter3.iloc[:-1].copy()


        seven_ago = inter3.loc[inter3['Date'] == seven_ago_date].copy()
        # print(seven_ago)

        latest = inter3.loc[inter3['Date'] == inter3['Date'].max()].copy()

        # print("\n\n")
        #
        # print(f"Latest{state}",latest)
        #
        # print("\n\n")

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

        fin['State'] = state

        zlisto.append(fin)

    last = pd.concat(zlisto)

    return last

# nsw = nsw_init.copy()
# nsw = nsw[['notification_date','lga_name19']]
# nsw.columns = ['Date', 'LGA']

# old_nsw = pd.read_csv('nsw_old_data/confirmed_cases_table1_location.csv')
# old_nsw = old_nsw[['notification_date','lga_name19']]
# # old_nsw.columns = ['Date', 'LGA']
# old_nsw['confirmed_cases_count'] = 1

# old_nsw = old_nsw.groupby(by=['notification_date','lga_name19'])['confirmed_cases_count'].sum().reset_index()
# newest_date = nsw_init_2['notification_date'].min()
# old_nsw = old_nsw.loc[old_nsw['notification_date'] < newest_date]

# with open('nsw_old_data/prev_nsw_data.csv', 'w') as f:
#     old_nsw.to_csv(f, index=False, header=True)

old_nsw = pd.read_csv('nsw_old_data/prev_nsw_data.csv')



nsw = nsw_init_2.copy()
# 'notification_date', 'postcode', 'lhd_2010_code', 'lhd_2010_name',
#        'lga_code19', 'lga_name19', 'confirmed_by_pcr',
#        'confirmed_cases_count'

### THIS IS THE NEW DATSET
nsw = nsw[['notification_date', 'lga_name19',  'confirmed_cases_count']]

nsw = nsw.append(old_nsw)
print(nsw)
print(nsw.columns)
nsw.columns = ['Date', 'LGA', 'Total cases']

## Grab the population data

print("Adding NSW pops")

nsw_pops = pd.read_excel('32180DS0002_2019-20.xls',
sheet_name="Table 1", skiprows=7)

nsw_pops = nsw_pops[['Local Government Area', 'no..1']]
nsw_pops.columns = ['LGA', 'Population']

nsw_pops.dropna(subset=['LGA'], inplace=True)
nsw_pops = nsw_pops.loc[~nsw_pops['LGA'].str.contains("TOTAL")]

nsw = pd.merge(nsw, nsw_pops, left_on="LGA",right_on='LGA', how='left')

nsw = nsw.loc[~nsw['LGA'].isna()]

nsw = nsw_second_process_cumulat(nsw, "NSW", nsw_max_date)

print(nsw)

# print(nsw_init_2)
# print(nsw_init_2.columns)