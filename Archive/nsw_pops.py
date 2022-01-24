import pandas as pd 


sheet = "Table 2"

## Grab the population data
vic_pops = pd.read_excel('32180DS0002_2019-20.xls',
sheet_name="Table 2", skiprows=7)

print(vic_pops)

vic_pops = vic_pops[['Local Government Area', 'no..1']]
vic_pops.columns = ['LGA', 'Population']

vic_pops.dropna(subset=['LGA'], inplace=True)
vic_pops = vic_pops.loc[~vic_pops['LGA'].str.contains("TOTAL")]

print(vic_pops)