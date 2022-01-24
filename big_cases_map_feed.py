#%%
import pandas as pd
import os
import datetime
import pytz
from modules.syncData import syncData
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

print("Starting QLD")

### GET QLD SCRAPE DATA
listo = []

new = pd.read_csv('https://raw.githubusercontent.com/joshnicholas/oz-covid-data/main/output/qld_lgas.csv')

old = pd.read_csv('qld_lga_data/covidlive_20_24_dec.csv')

qld = new.append(old)

qld.drop_duplicates(subset=['LGA', 'Date'], inplace=True)

qld = qld.sort_values(by='Date', ascending=True)

qld['Date'] = pd.to_datetime(qld['Date'], format="%Y-%m-%d")
qld['Date'] = qld['Date'] - pd.DateOffset(days=1)
# print(qld)
earliest_date = qld['Date'].min()
earliest_date = earliest_date.strftime('%Y-%m-%d')

#%%

### QLD pop
print("Adding QLD populations")

qld['LGA'] = qld['LGA'].str.replace("Town", '')
qld['LGA'] = qld['LGA'].str.replace("Regional", '')
qld['LGA'] = qld['LGA'].str.replace("Shire", '')
qld['LGA'] = qld['LGA'].str.replace("City", '')
qld['LGA'] = qld['LGA'].str.replace("Aboriginal", '')

qld = qld.loc[~qld['LGA'].isin(['Interstate/Other', 'Not Supplied'])]

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

# qld_max_date = 
# print(qld)

#%%
### PROCESS QLD DATA

print("Processing QLD")

zlisto = []

for lga in qld['LGA'].unique().tolist():
    try:
        # print(lga)
        inter2 = qld.loc[qld['LGA'] == lga].copy()
        inter2.set_index("Date", inplace=True)

        daily=pd.date_range(earliest_date, bris_today, freq='D')
        inter2 = inter2.reindex(daily, method='ffill')

        inter2 = inter2.reset_index()
        inter2.rename(columns={'index':"Date"}, inplace=True)

        # print(inter2)

        inter2['New cases'] = inter2['Total cases'].diff()

        inter2['7 day sum'] = inter2['New cases'].rolling(window=7).sum()

        latest_cases = inter2.iloc[-1]['Total cases']
        second_latest_cases = inter2.iloc[-2]['Total cases']
        third_latest_cases = inter2.iloc[-3]['Total cases']

        if third_latest_cases == latest_cases:
            inter2 = inter2.iloc[:-2].copy()
        elif latest_cases == second_latest_cases:
            inter2 = inter2.iloc[:-1].copy()
        

        seven_ago = inter2.loc[inter2['Date'] == seven_ago_date].copy()
        # print(seven_ago)

        latest = inter2.loc[inter2['Date'] == inter2['Date'].max()].copy()

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

        # print(fin)

        # if lga != "Brisbane City":
        #     print(to_use)
        zlisto.append(fin)
    except Exception as e:
        print(f"Problem with {lga}: {e}")


qld = pd.concat(zlisto)
qld['State'] = "QLD"

# print(qld)

# print(qld)
# print(qld.columns)
# 'LGA', 'Date', 'New_cases', 'Total_cases',
# 'Weekly_cases','Previous_week',
# 'Weekly_change', 'State'

#%%

# GET NSW DATA
print("Starting NSW")

nsw_init = pd.read_csv("https://data.nsw.gov.au/data/dataset/aefcde60-3b0c-4bc0-9af1-6fe652944ec2/resource/21304414-1ff1-4243-a5d2-f52778048b29/download/confirmed_cases_table1_location.csv")

nsw_max_date = nsw_init['notification_date'].max()
# nsw_max_date = datetime.datetime.strptime(nsw_max_date, "%d/%m/%y")
# nsw_max_date = nsw_max_date.strftime("%Y-%m-%d")
# %%

# PROCESS DATA

print("Processing NSW")


def process_cumulat(frame, state, max_date):
    init = frame.copy()
    init = init.sort_values(by=['Date'], ascending=True)
    init['Total cases'] = 1
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

nsw = nsw_init.copy()
nsw = nsw[['notification_date','lga_name19']]
nsw.columns = ['Date', 'LGA']

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

nsw = process_cumulat(nsw, "NSW", nsw_max_date)

# print(nsw)
# print(nsw.columns)

# %%

## GET VIC DATA
print("Starting VIC")

vic_init = pd.read_csv('https://www.dhhs.vic.gov.au/ncov-covid-cases-by-lga-source-csv')
# vic_init = pd.read_csv('https://www.coronavirus.vic.gov.au/sites/default/files/2022-01/NCOV_COVID_Cases_by_LGA_Source_20220109.csv')
vic_max_date = vic_init['diagnosis_date'].max()
# vic_max_date = vic_max_date.strftime("%Y-%m-%d")
# %%

print(vic_init)
print(vic_init.columns)

### VIC PROCESSING

print("Vic processing")

## Grab the population data
vic_pops = pd.read_excel('32180DS0002_2019-20.xls',
sheet_name="Table 2", skiprows=7)

# print(vic_pops)

vic_pops = vic_pops[['Local Government Area', 'no..1']]
vic_pops.columns = ['LGA', 'Population']

vic_pops.dropna(subset=['LGA'], inplace=True)
vic_pops = vic_pops.loc[~vic_pops['LGA'].str.contains("TOTAL")]

vic = vic_init.copy()
vic = vic[['diagnosis_date', 'Localgovernmentarea']]
vic.columns = ['Date', 'LGA']

vic.loc[vic['LGA'] == "Kingston (C)", 'LGA'] = 'Kingston (C) (Vic.)'
vic.loc[vic['LGA'] == "Latrobe (C)", 'LGA'] = 'Latrobe (C) (Vic.)'

vic = vic.loc[~vic['LGA'].isin(['Overseas','Interstate', 'Unknown'])]

vic = pd.merge(vic, vic_pops, left_on="LGA",right_on='LGA', how='left')

vic = process_cumulat(vic, "VIC", vic_max_date)

# print(vic)

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

# # %%

print("Uploading")

tog = [qld, nsw, vic, sa]

combo = pd.concat(tog)
# combo = qld.copy()
combo['Date'] = pd.to_datetime(combo['Date'])
combo['Date']  = combo['Date'].dt.strftime("%Y-%m-%d")
# combo.set_index("LGA", inplace=True)

combo.fillna('', inplace=True)

p = combo

print(p)
print(p.columns)
# print(combo.to_dict('records'))

syncData(combo.to_dict(orient='records'),'2022/01/oz-corona-map', f"mapdata{testo}")
