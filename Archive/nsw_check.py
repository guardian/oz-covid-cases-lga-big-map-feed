import pandas as pd 

nsw_init = pd.read_csv("https://data.nsw.gov.au/data/dataset/aefcde60-3b0c-4bc0-9af1-6fe652944ec2/resource/21304414-1ff1-4243-a5d2-f52778048b29/download/confirmed_cases_table1_location.csv")
nsw = nsw_init.copy()
nsw = nsw[['notification_date','lga_name19']]

nsw.dropna(subset=['lga_name19'], inplace=True)

print(nsw.loc[nsw['lga_name19'].str.contains("Unincorporated")])

# print(nsw)