import pandas as pd
import requests


city_name = "Yangon"
api_key = "6705a56b54f2a2949f710d6658e41f6b"

weather_url = "https://api.openweathermap.org/data/2.5/weather?q={city_name}&appid={api_key}"

weather_data = requests.get(weather_url).json()

weather_data_df = pd.json_normalize(weather_data)

print(weather_data_df.head())