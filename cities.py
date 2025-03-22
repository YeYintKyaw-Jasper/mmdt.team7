import requests
import pandas as pd

cities_url = "https://raw.githubusercontent.com/dr5hn/countries-states-cities-database/refs/heads/master/json/cities.json"

cities_data = requests.get(cities_url).json()

cities_df = pd.DataFrame(cities_data)

print(cities_df.head())
print(cities_df.columns)