import requests
import pandas as pd

owid_raw_url = "https://raw.githubusercontent.com/owid/covid-19-data/refs/heads/master/public/data/latest/owid-covid-latest.json"

raw_data = requests.get(owid_raw_url).json()

all_countries_df = pd.DataFrame()

for country_short_code in raw_data.keys():
    single_country_df = pd.json_normalize(raw_data[country_short_code])
    single_country_df['iso_country_code'] = country_short_code
    ## Combine/Concat
    all_countries_df = pd.concat([all_countries_df, single_country_df], ignore_index=True)

cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

cities_data = requests.get(cities_url).json()

cities_df = pd.DataFrame(cities_data)

# Merge two df 
merged_df = pd.merge(all_countries_df, cities_df, left_on="iso_country_code",  right_on="country_id", how="inner") 
print(merged_df.head())
